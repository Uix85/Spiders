# -*- coding: utf-8 -*-
"""bilibili-api 搜索桥接：build_keywords 用它解决搜索接口软风控（v_voucher）。

理由：自研 spider 裸 spi→buvid 缺激活指纹 + bili_ticket，B站搜索接口稳定 v_voucher（实测）；
bilibili-api 的 Api(wbi=True) 自动处理 buvid 激活 / bili_ticket / WBI 重试，搜索接口实测通过。

并发：bilibili-api 是 async 库，单事件循环内 asyncio.gather 并发多词搜索（共享全局 buvid/ticket 无竞态），
实测 8 词并发 0.7s 全部成功、无 v_voucher。

用法：
    from bk_search import search_videos_batch, search_videos_by_bili_api
    vids_all = search_videos_batch(["DeepSeek", "GPT"], 0, 0, pages=2)   # 并发批量 → {"词": [{bvid,title}]}
    vids     = search_videos_by_bili_api("DeepSeek", 0, 0, pages=2)      # 单个词（回验用）
"""
import asyncio
import re
import sys
import os
import time
import threading

_RETRY_MAX = 10**9         # 重试无上限（用户 2026-09-01 指示：一直重试直到成功，永不放弃）
_RETRY_DELAYS = [3, 5, 10]   # 短退避序列（秒），循环使用（配合 412 冷却闸 60s 自调节奏）

class _SoftRisk(Exception):
    """内部信号：HTTP 200 但 data.result 空（软风控特征，AGENTS.md gotcha），触发外层退避重试。"""

class _CoolTimeout(Exception):
    """内部信号：全局冷却闸等待超过 _COOL_MAX_WAIT 秒（持续 412 = IP 疲劳/被拉黑）→ 应熔断退出，
    不能无限排队等冷却（否则 22 线程全睡在闸口、tqdm 停在 0%、熔断永不触发——实测死机态）。"""

# 全局 412 冷却闸：实测 412 是"频率闸"不是拉黑——单搜等 40s 后成功；
# 但批量并发若各自退避，20 词同时等完再同时发请求 = 还是风暴全灭。
# 改成：任一请求遇 412 → 全体共享冷却 _COOL_SECS，闸内所有请求排队等待，顺序放行。
_COOL_SECS = 10   # 412 后全局停多久（10s 足够恢复频率闸；60s 会导致多词轮候活死锁）（实测 30-40s 可恢复，60s 留余量）
_COOL_MAX_WAIT = 10**9     # 冷却闸无限等待（用户 2026-09-01 指示：永不熔断，一直等到解封再继续）
_cool_until = 0.0          # 冷却截止时间戳（跨线程，锁保护）
_cool_lock = threading.Lock()

# ---- 全局速率闸：所有词/页共享一个节拍器，总请求率锁死（防暴冲限流）----
# 背景：B站搜索接口对游客 IP 限额极紧（实测 20req/min 仍会短时限流），而并发词各自打页
# （页间 asyncio.sleep(0.3)）会形成“6词×5页≈30请求/2s”的暴冲，触发 data.result 空/412。
# 办法：单事件循环内共享“上次请求时间戳”，每请求前等够 _RATE_GAP_SEC，任何并发度下总请求率 ≈ 1/gap。
# 对照实验可用环境变量 TK_RATE_GAP 覆盖（用户 2026-09-01 指示：不减每词页数，只控制节奏）。
_RATE_GAP_SEC = float(os.environ.get("TK_RATE_GAP", "1.5"))  # 全局请求间隔：0.3s（原值）。实测干净IP下8并发+0.3s零失败（2026-09-01）
_rate_last = 0.0        # 上次实际请求时间戳（单事件循环内共享）


async def _rate_wait():
    """全局速率闸（async）：等够距上次请求 _RATE_GAP_SEC。
    asyncio.sleep 不阻塞事件循环：其他词在等待期间可推进非请求逻辑，但真正发请求前都会在此串行化。
    """
    global _rate_last
    while True:
        now = time.time()
        wait = _rate_last + _RATE_GAP_SEC - now
        if wait <= 0:
            break
        await asyncio.sleep(wait)
    _rate_last = time.time()


def _wait_cool():
    """冷却闸（sync，线程场景用——spider.py 多线程 httpx）。"""
    t0 = time.time()
    while True:
        with _cool_lock:
            if time.time() >= _cool_until:
                return
        if time.time() - t0 > _COOL_MAX_WAIT:
            raise _CoolTimeout(f"冷却闸等待 > {_COOL_MAX_WAIT}s（持续 412，疑似 IP 被风控）")
        time.sleep(1)


async def _wait_cool_async():
    """冷却闸（async，asyncio 事件循环内用——bk_search 搜索）。await 版不阻塞事件循环。
    原 sync 版在事件循环内被并发词反复 time.sleep 阻塞 → 活死锁（2026-09-01 修复）。"""
    import asyncio as _aio
    t0 = time.time()
    while True:
        with _cool_lock:
            if time.time() >= _cool_until:
                return
        if time.time() - t0 > _COOL_MAX_WAIT:
            raise _CoolTimeout(f"冷却闸等待 > {_COOL_MAX_WAIT}s（持续 412，疑似 IP 被风控）")
        await _aio.sleep(1)


def _hit_412(e: Exception) -> bool:
    """412 判定：NetworkException 消息含"状态码：412"（bilibili-api 对非 JSON 响应抛此异常）。"""
    return "412" in str(e)[:200]


def _set_cool():
    """触发全局冷却（延长到 max(当前冷却, now+60s)）。"""
    global _cool_until
    with _cool_lock:
        _cool_until = max(_cool_until, time.time() + _COOL_SECS)

_HERE = os.path.dirname(os.path.abspath(__file__))
# 爬虫直连游客：忽略系统代理环境变量（用户 shell 常 export all_proxy=socks5://...，
# bilibili-api 内部 client 读它 → 缺 socksio 崩 ImportError）。删除所有代理变量。
for _k in ["http_proxy", "https_proxy", "all_proxy", "no_proxy",
           "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"]:
    os.environ.pop(_k, None)

sys.path.insert(0, os.path.join(_HERE, "bilibili-api"))

_api_ok = False
try:
    import bilibili_api.search as _s
    from bilibili_api.utils.network import Api  # noqa
    _api_ok = True
except Exception as _e:
    _SEARCH_IMPORT_ERR = str(_e)


def _extract(vids_raw: list) -> list:
    """原始结果 → [{bvid, title}]（去 tag 标签、去重）。"""
    out, seen = [], set()
    for v in vids_raw:
        b = v.get("bvid")
        if b and b not in seen:
            seen.add(b)
            t = re.sub(r"<[^>]+>", "", v.get("title", "") or "")
            out.append({"bvid": b, "title": t})
    return out


async def _search_pages_async(keyword: str, begin: int, end: int, pages: int) -> list:
    """单词多页搜索（async）→ [{bvid, title}]。"""
    api = _s.API["search"]["web_search_by_type"]
    base = {"keyword": keyword, "search_type": "video", "page_size": 20}
    if begin:
        base["time_start"] = time.strftime("%Y-%m-%d", time.localtime(begin))
    if end:
        base["time_end"] = time.strftime("%Y-%m-%d", time.localtime(end))
    all_raw = []
    for p in range(1, pages + 1):
        params = dict(base, page=p)
        # 重试：412/网络错/风控码/data.result 空 → 退避重试（bilibili-api 每次 _request 重新激活指纹，等于换钥重试）
        raw = None
        for attempt in range(1, _RETRY_MAX + 1):
            try:
                await _wait_cool_async()   # 全局 412 冷却闸（async，不阻塞事件循环）
                await _rate_wait()   # 全局速率闸：总请求率锁死 ≈ 1/_RATE_GAP_SEC（防并发暴冲限流）
                res = await Api(**api, wbi=True, credential=_LOGIN_CRED).update_params(**params)._request(raw=True)
                if not isinstance(res, dict) or res.get("code") != 0:
                    raise _SoftRisk(f"code={res.get('code') if isinstance(res, dict) else res} msg={res.get('message') if isinstance(res, dict) else ''}")
                raw = (res.get("data") or {}).get("result") or []
                if not raw:
                    # 软风控：HTTP 200 但 data.result 空（AGENTS.md gotcha）。不能当"真无结果"静默放弃
                    # （批量并发时整批词会被误判无结果，round 8 实测 12 词全空但单搜全有结果）
                    raise _SoftRisk(f"data.result 空(已得 {len(all_raw)} 条) → 疑似软风控")
                _FAILS.pop(keyword, None)   # 成功：清残留失败记录
                break   # 成功出重试循环
            except _SoftRisk as e:
                _FAILS[keyword] = str(e) + f" (第{attempt}次)"
                if attempt < _RETRY_MAX:
                    await asyncio.sleep(_RETRY_DELAYS[(attempt - 1) % len(_RETRY_DELAYS)])   # 10s/30s/60s 退避
            except _CoolTimeout:
                raise   # 持续风控信号：直接向上传播触发熔断（不能当普通异常重试，那只会继续白等）
            except Exception as e:
                _FAILS[keyword] = f"{type(e).__name__}: {str(e)[:80]}"
                if _hit_412(e):
                    _set_cool()   # 412 → 全局冷却闸（并发词共享，顺序放行）
                if attempt < _RETRY_MAX:
                    await asyncio.sleep(_RETRY_DELAYS[(attempt - 1) % len(_RETRY_DELAYS)])   # 10s/30s/60s 退避
        if raw is None:   # 重试耗尽仍失败 → 放弃本页（记过 _FAILS 了）
            break
        all_raw.extend(raw)
        await asyncio.sleep(0.3)   # 页间轻微间隔，防同词连打
    return _extract(all_raw)


_FAILS: dict = {}   # 全局失败原因记录（并发冲突/风控时可见）


def get_last_fails() -> dict:
    """最近一次批量搜索的失败词及原因（诊断用）。"""
    return dict(_FAILS)


async def _batch_async(wordlist: list, begin: int, end: int, pages: int,
                       max_concurrency: int = 8) -> dict:
    """并发批量搜索（async）。单个 asyncio 事件循环共享 buvid/ticket（无竞态），
    限制并发数 max_concurrency（防瞬时请求风暴 + 保 IP 较稳）。"""
    sem = asyncio.Semaphore(max_concurrency)
    TASK_TIMEOUT = max(30, pages * 6)   # 每词超时：5页正常5s，30s=6倍余量；毒词（如 世界模型/Pytorch 实测 116-177s）超时返回空不拖死整轮

    async def one(kw):
        async with sem:
            try:
                return kw, await asyncio.wait_for(_search_pages_async(kw, begin, end, pages), timeout=TASK_TIMEOUT)
            except (asyncio.TimeoutError, Exception):
                _FAILS[kw] = f"单词处理超时({TASK_TIMEOUT}s)"
                return kw, []

    return dict(await asyncio.gather(*(one(kw) for kw in wordlist)))


def _apply_login():
    """配置了 BILI_COOKIE 时注入 Credential；BILI_PROXY 可选。"""
    try:
        import config as tkc
        if not tkc.USE_LOGIN:
            return
        from bilibili_api import Credential, request_settings
        import re as _re
        ck = {}
        for part in tkc.LOGIN_COOKIE.split(';'):
            if '=' in part:
                k, v = part.strip().split('=', 1)
                ck[k.strip()] = v
        cred = Credential(sessdata=ck.get('SESSDATA',''), bili_jct=ck.get('bili_jct',''),
                          buvid3=ck.get('buvid3',''), buvid4=ck.get('buvid4',''))
        _apply_login.cred = cred
        if tkc.LOGIN_PROXY:
            request_settings.set_proxy(tkc.LOGIN_PROXY)
        global _LOGIN_CRED
        _LOGIN_CRED = cred
    except Exception:
        pass

_LOGIN_CRED = None
_apply_login()

def search_videos_batch(wordlist: list, begin: int, end: int, pages: int = 2,
                        max_concurrency: int = 8) -> dict:
    """并发批量搜索（同步桥）→ {词: [{bvid,title}]}（bvid 已去重、title 已去标签）。"""
    if not _api_ok:
        raise RuntimeError(f"bilibili-api 导入失败: {_SEARCH_IMPORT_ERR}")
    _FAILS.clear()
    return asyncio.run(_batch_async(list(wordlist), begin, end, pages, max_concurrency))


def search_videos_by_bili_api(keyword: str, begin: int, end: int, pages: int = 2,
                              page_size: int = 20) -> list:
    """单个词搜索（同步桥，回验场景用）→ [{bvid, title}]。"""
    if not _api_ok:
        raise RuntimeError(f"bilibili-api 导入失败: {_SEARCH_IMPORT_ERR}")
    return asyncio.run(_search_pages_async(keyword, begin, end, pages))


def get_video_tags(bvid: str) -> list:
    """按 bvid 取视频 tags（走 bilibili-api 激活指纹，抗并发——与搜索同款机制）。
    返回 tag_name 列表，失败返回空列表。"""
    if not _api_ok:
        raise RuntimeError(f"bilibili-api 导入失败: {_SEARCH_IMPORT_ERR}")
    from bilibili_api import video as _vid

    async def _go():
        try:
            vid = _vid.Video(bvid=bvid)
            tags = await vid.get_tags()
            return [t.get("tag_name", "") for t in (tags or []) if t.get("tag_name")]
        except Exception:
            return []
    return asyncio.run(_go())
