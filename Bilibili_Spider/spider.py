#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""B站评论爬虫（两阶段）。

阶段A：逐词搜索（综合排序，分时间窗块拼接）→ 词×位次视频表矩阵，落 CSV（幂等，已构建即跳过）
阶段B：读矩阵，列优先生成全局唯一 video_id 队列 → 逐视频爬一级最热前 N 条 ± 二级全量 → 每视频一个
       {bvid}.jsonl（首行=视频信息），CSV 完成标记回写 → 断点续爬 + 总量预算 + counter 对账

并发与风控（参考 tag_keywords/bk_search 实测方案）：
    阶段A 搜索改走 tag_keywords 内置的 bilibili-api（buvid 激活指纹 ExClimbWuzhi + bili_ticket + WBI
    自动重试）——自研裸 spi 指纹无激活，搜索接口稳定 v_voucher 软风控（实测）；并发 SEARCH_CONCURRENCY。
    阶段B 评论保留 httpx 直连（评论接口无 v_voucher 问题），线程池 STAGE_B_WORKERS 并行；
    全局速率闸保证总 QPS 恒定（线程只并行、不抬高请求率）+ 全局 412 冷却闸 + 风控后轮换游客指纹。

用法：
    uv run python spider.py            # 全流程（A→B）
    uv run python spider.py --stage a  # 仅阶段A
    uv run python spider.py --stage b  # 仅阶段B（断点补齐）
    uv run python spider.py --check    # 冒烟测试：buvid + WBI 签名 + 单次搜索
"""
import argparse
import asyncio
import csv
import hashlib
import json
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from hashlib import md5
from urllib.parse import urlencode

import httpx

import config

# ---- bilibili-api 通道（复用 tag_keywords 已实测的 bk_search：冷却闸 + 激活指纹）----
_TK_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tag_keywords")
if os.path.isdir(_TK_DIR):
    sys.path.insert(0, _TK_DIR)
try:
    import bk_search as _bks  # noqa: F401  导入副作用：清代理环境变量 + 接入 bilibili-api 路径
    import bilibili_api.search as _bili_search
    from bilibili_api.utils.network import Api
    from bk_search import _CoolTimeout, _SoftRisk, _hit_412, _set_cool, _wait_cool
    _BKS_OK = True
except Exception as _e:  # pragma: no cover
    _BKS_ERR = str(_e)
    _BKS_OK = False

API = "https://api.bilibili.com"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
# 浏览器仿真头：只加真实 Chrome 会带的核心头（Origin/Accept-Language），降低指纹特征
HEADERS = {
    "User-Agent": UA,
    "Referer": "https://www.bilibili.com/",
    "Origin": "https://www.bilibili.com",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}
MIXIN_TABLE = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49,
    33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40,
    61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11,
    36, 20, 34, 44, 52]


def wbi_sign(params: dict, img_key: str, sub_key: str) -> dict:
    """WBI 签名（bilibili-API-collect：wts + 置换表 salt + MD5）。"""
    params["wts"] = int(time.time())
    params = dict(sorted(params.items()))
    params = {k: "".join(c for c in str(v) if c not in "!'()*") for k, v in params.items()}
    mixin = img_key + sub_key
    salt = "".join(mixin[i] for i in MIXIN_TABLE)[:32]
    params["w_rid"] = md5((urlencode(params) + salt).encode()).hexdigest()
    return params


def clean_html(s: str) -> str:
    """去搜索接口标题/简介里的 <em> 高亮标签。"""
    return re.sub(r"<[^>]+>", "", s) if s else ""


def ts(day: datetime, end_of_day: bool = False) -> int:
    """datetime → 秒级时间戳（0点或23:59:59）。"""
    if end_of_day:
        day = day.replace(hour=23, minute=59, second=59)
    return int(day.timestamp())


# ---------- 整体缓存（API 响应级，磁盘 JSONL + 内存 dict；仿 tag_keywords bvid_tags_cache） ----------
# 只缓存成功非空响应（搜索空页 / 详情为空 / 接口失败均不写缓存，防 412/软风控空结果污染）。
_cache_data = None
_cache_write_lock = threading.Lock()


def _cache_load() -> dict:
    d = {}
    if config.CACHE_ENABLED and os.path.exists(config.CACHE_FILE):
        with open(config.CACHE_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                d[rec["k"]] = rec["v"]
    return d


def _cache_peek(key: str):
    """只读命中（不加载则 None 不缓存）。"""
    global _cache_data
    if not config.CACHE_ENABLED:
        return None
    if _cache_data is None:
        _cache_data = _cache_load()
    return _cache_data.get(key)


def _cache_store(key: str, val):
    if not config.CACHE_ENABLED or val is None:
        return
    with _cache_write_lock:
        global _cache_data
        if _cache_data is None:
            _cache_data = _cache_load()
        _cache_data[key] = val
        os.makedirs(os.path.dirname(config.CACHE_FILE), exist_ok=True)
        with open(config.CACHE_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps({"k": key, "v": val}, ensure_ascii=False) + "\n")


def _cache_get(key: str, loader):
    """命中返回；未命中调 loader()，非空结果写缓存。loader 失败应返回空值/抛异常（不缓存）。"""
    v = _cache_peek(key)
    if v is not None:
        return v
    v = loader()
    if v:
        _cache_store(key, v)
    return v


class BiliSpider:
    def __init__(self, cookie: str = None, retry_delays: list = None, request_interval: float = None):
        # 可注入覆盖（如 tag_keywords 独立 config）；None → 用根 config 默认（评论爬虫不受影响）
        self.cookie = cookie if cookie is not None else config.COOKIE
        self.retry_delays = retry_delays if retry_delays is not None else config.RETRY_DELAYS
        self.request_interval = request_interval if request_interval is not None else config.REQUEST_INTERVAL
        self.data_dir = os.path.join(os.path.dirname(__file__), "data", config.PROJECT)
        self.csv_path = os.path.join(self.data_dir, "video_table.csv")
        self.counter_path = os.path.join(self.data_dir, "counter.json")
        os.makedirs(self.data_dir, exist_ok=True)
        self.img_key = self.sub_key = ""
        self.client = httpx.Client(headers=HEADERS, timeout=30,
                                   trust_env=False)   # 忽略系统代理环境变量（all_proxy 含 socks 时会因缺 socksio 崩）；直连游客模式
        self.salt = self._load_salt()
        # 多线程共享状态
        self._lock = threading.Lock()      # 保护 counter / 表回写 / 速率闸 / 指纹轮换
        self._rate_next = 0.0              # 全局速率闸下次可请求时间戳
        self._last_rotate = 0.0            # 上次指纹轮换时间（节流防惊群）
        self._abort = threading.Event()    # 持续风控熔断信号（_CoolTimeout 时置位）

    def _http_get(self, url: str, params: dict = None, headers: dict = None, timeout: float = 30.0) -> httpx.Response:
        """GET 直连（B站 API 均为 https，httpx 原生处理 TLS/chunked/gzip）。返回 httpx.Response。"""
        return self.client.get(url, params=params, headers=headers)

    # ---------- 多线程全局限闸 ----------
    def _gate(self):
        """每次请求前的全局闸（跨线程共享）：412 冷却闸 + 速率闸。
        速率闸保证全局 QPS 恒定 ≈ 1/interval（±30% 抖动防固定节奏特征），
        线程数只增加并行度（重试/冷却等待重叠），不抬高请求率——游客模式宁慢勿崩。"""
        _wait_cool()   # 412 全局冷却（bk_search 共享闸；持续风控抛 _CoolTimeout 熔断）
        iv = self.request_interval * random.uniform(0.7, 1.3)
        with self._lock:
            now = time.time()
            t = max(self._rate_next, now)
            self._rate_next = t + iv
        if t > now:
            time.sleep(t - now)

    def _rotate_identity(self):
        """风控后轮换游客指纹 + WBI 密钥（10s 节流：多线程同时失败只换一次，防惊群）。"""
        with self._lock:
            if time.time() - self._last_rotate < 10:
                return
            self._last_rotate = time.time()
            if not config.COOKIE:   # 游客模式：软风控=当前 buvid 被标记，换新指纹（登录态不动 Cookie）
                self.cookie = self._get_buvid()
            self._refresh_wbi()

    def _load_salt(self) -> str:
        """读取匿名化盐；默认在程序目录保存一份本机盐，不写入配置源码。"""
        if getattr(config, "ANON_SALT", ""):
            return config.ANON_SALT
        salt_path = os.path.join(os.path.dirname(__file__), ".anon_salt")
        try:
            with open(salt_path, encoding="utf-8") as f:
                return f.read().strip()
        except FileNotFoundError:
            salt = os.urandom(16).hex()
            try:
                with open(salt_path, "x", encoding="utf-8") as f:
                    f.write(salt)
            except FileExistsError:
                with open(salt_path, encoding="utf-8") as f:
                    salt = f.read().strip()
            config.ANON_SALT = salt
            return salt

    def _anon_id(self, mid) -> str:
        """评论者假名：SHA-256(盐+mid) 截断 64bit。同盐下同一用户跨视频/跨批次一致。"""
        return hashlib.sha256((self.salt + str(mid)).encode()).hexdigest()[:16]

    # ---------- 平台基础：buvid 游客指纹 + WBI 密钥 ----------
    def _get_buvid(self) -> str:
        r = self._http_get(f"{API}/x/frontend/finger/spi")
        d = r.json()["data"]
        return f"buvid3={d['b_3']}; buvid4={d['b_4']}"

    def _refresh_wbi(self):
        r = self._http_get(f"{API}/x/web-interface/nav", headers={"Cookie": self.cookie})
        img = r.json()["data"]["wbi_img"]["img_url"].rsplit("/", 1)[1].split(".")[0]
        sub = r.json()["data"]["wbi_img"]["sub_url"].rsplit("/", 1)[1].split(".")[0]
        self.img_key, self.sub_key = img, sub

    def _verify_login(self, hard: bool = True) -> bool:
        """校验 config.COOKIE 登录态：调 nav 看 isLogin。
        hard=True：Cookie 非空但未登录 → raise（杜绝降级游客白跑）；无效时明确报错退出。
        hard=False：仅打印状态（--check 用）。"""
        if not config.COOKIE:
            if hard:
                raise RuntimeError("BILI_COOKIE 为空：阶段B正式采样必须设置有效登录 Cookie")
            print("[check] 游客模式（未设置 BILI_COOKIE）")
            return False
        nav = self._http_get(f"{API}/x/web-interface/nav", headers={"Cookie": self.cookie}).json()
        if nav.get("data", {}).get("isLogin"):
            if not hard:
                print(f"[check] 登录态 OK: {nav['data'].get('uname', '')}")
            return True
        if hard:
            raise RuntimeError(f"config.COOKIE 无效或已过期（nav code={nav.get('code')}）："
                               f"请重新登录 B 站并在终端设置 BILI_COOKIE 后重跑")
        print("[check] 未登录（Cookie 无效或已填）")
        return False

    def _api(self, uri: str, params: dict, fail_soft: bool = False) -> dict | None:
        """带 WBI 签名 + 软风控/-412/-352 指数退避重试的 GET。

        fail_soft=True：重试耗尽返回 None（调用方自行处理跳过，不崩）——参考 MediaCrawler 的 “跳过继续”；
        fail_soft=False：抛 RuntimeError（评论爬虫现状，失败即停）。
        退避带随机抖动（-40%~+40%）防固定节奏特征匹配（借鉴 MediaCrawler random.uniform 抖动）。
        """
        if not self.cookie:
            self.cookie = self._get_buvid()
        if not self.img_key:
            self._refresh_wbi()
        for attempt in range(len(self.retry_delays)):
            self._gate()   # 全局冷却闸 + 速率闸（多线程共享，替代裸 sleep）
            signed = wbi_sign(dict(params), self.img_key, self.sub_key)
            r = self._http_get(f"{API}{uri}", params=signed, headers={"Cookie": self.cookie})
            try:
                data = r.json()
                d = data.get("data")
                # 软风控：code=0 但该搜的没搜到 → 视为失败重试，不能当空结果返回。
                # B站软风控形态会变化：实测搜索接口 code=0 但 data 只含 v_voucher 无 result 键（也曾见 result=null），
                # 统一判：搜索类响应 data 是 dict 但没有有效 result 列表 => 风控。
                # 注意只对搜索接口生效：评论接口（/reply/wbi/main）的 data 有 replies 无 result，不能误伤。
                resp_ok = (data.get("code") == 0 and d is not None)
                if resp_ok and "/search/" in uri and isinstance(d, dict):
                    resp_ok = "result" in d and isinstance(d["result"], list)
                if resp_ok:
                    return d
                if data.get("code") == 0 and "/search/" in uri:
                    msg = "软风控: 搜索 code=0 但 result 缺失/非列表"
                else:
                    msg = data.get("message", "?")
                # 业务性不可重试错误（UP主已关闭评论区等）：一次判定直接跳过，不浪费重试退避
                if "/reply/" in uri and ("关闭" in msg or "评论区" in msg or data.get("code") in (-403, -404)):
                    if not fail_soft:
                        print(f"[api] {uri} 业务错误({msg})直接跳过（UP主未开评论区）", file=sys.stderr)
                    return None
                if data.get("code") in (-412, -352):   # 风控码 → 全局冷却闸（多线程共享）
                    _set_cool()
            except Exception as e:
                msg = f"{e.__class__.__name__}: {r.text[:100]}"
                if _hit_412(e):
                    _set_cool()
            delay = self.retry_delays[attempt] * random.uniform(0.6, 1.4)   # 抖动防特征
            print(f"[api] {uri} 失败(尝试{attempt+1}): {msg}，退避 {delay:.1f}s 后重试", file=sys.stderr)
            time.sleep(delay)
            self._rotate_identity()  # 游客模式：换新 buvid 指纹 + WBI 密钥（带节流）
        if fail_soft:
            print(f"[api] {uri} 重试耗尽（fail_soft 跳过）", file=sys.stderr)
            return None
        raise RuntimeError(f"{uri} 重试耗尽")

    # ---------- 接口 ----------
    def search_page(self, keyword: str, page: int, begin: int, end: int) -> list:
        d = self._api("/x/web-interface/wbi/search/type", {
            "search_type": "video", "keyword": keyword, "page": page, "page_size": 20,
            "order": "", "pubtime_begin_s": begin, "pubtime_end_s": end,
        }, fail_soft=True)   # 搜索失败返回空（不崩），build_keywords 跳过该词，后续轮次自然补挖
        return d.get("result") or [] if d else []

    def _fetch_primary(self, aid: int, limit: int) -> tuple[list, int, int]:
        """一级评论（最热 mode=0），返回 (列表, 实取数, all_count总评论数)。分页节奏由 _api 内全局闸控制。
        响应级缓存：key=reply:{aid}|{mode}|{next}，正式跑命中零请求（评论是 150 万主体，必须缓存）。"""
        rows, got, next_p, pg, all_count = [], 0, 0, 0, 0
        while got < limit:
            pg += 1
            ck = f"reply:{aid}|0|{next_p}"   # mode=0（最热）
            d = _cache_get(ck, lambda: self._api(
                "/x/v2/reply/wbi/main",
                {"oid": aid, "type": 1, "mode": 0, "ps": 20, "next": next_p}))
            if not d:   # UP主关闭评论区/业务错误 → 视为无评论，直接结束该视频
                break
            cursor = d.get("cursor") or {}
            # 置顶评论（top_replies）独立于分页列表返回，只在第1页出现；并入主楼（去重），否则每视频漏 1+N 条
            if pg == 1:
                have_r = {c["rpid"] for c in rows}
                for c in d.get("top_replies") or []:
                    if c["rpid"] not in have_r:
                        rows.append(c)
                        have_r.add(c["rpid"])
                        got += 1
            for c in d.get("replies") or []:
                if c["rpid"] in {r["rpid"] for r in rows}:
                    continue
                rows.append(c)
                got += 1
                if got >= limit:
                    break
            all_count = cursor.get("all_count") or all_count
            if cursor.get("is_end") or not cursor.get("next"):
                break
            next_p = cursor["next"]
        return rows, got, all_count or 0

    def _fetch_secondary(self, aid: int, root: int) -> list:
        """二级评论全量（按最热，直到分页结束）。响应级缓存：key=replies:{aid}|{root}|{pn}。"""
        rows, pn = [], 1
        while True:
            ck = f"replies:{aid}|{root}|{pn}"
            d = _cache_get(ck, lambda pn=pn: self._api(
                "/x/v2/reply/reply",
                {"oid": aid, "type": 1, "root": root, "ps": 20, "pn": pn}))
            rows.extend(d.get("replies") or [])
            total = (d.get("page") or {}).get("count", 0)
            if pn * 20 >= total:
                break
            pn += 1
        return rows

    # ---------- 阶段 A：视频表矩阵（bilibili-api 并发搜索） ----------
    def stage_a(self):
        if self._table_done():
            print("[A] 视频表已构建，跳过（--stage b 进入断点补齐）")
            return
        if not _BKS_OK:
            raise RuntimeError(f"bilibili-api 通道不可用（需 tag_keywords/ 子目录）: {_BKS_ERR}")
        start = datetime.strptime(config.START_DAY, "%Y-%m-%d")
        end = (datetime.strptime(config.END_DAY, "%Y-%m-%d") if config.END_DAY else datetime.now())
        # 整窗综合排序取前 N 条（不分时间块）。理由（用户 2026-09-04 纠正）：
        # 分块硬凑早窗口会把新词（Sora/可灵等）在 2023-2024 的无关视频当成 AI 内容采入；
        # B站 pubtime 过滤 + 综合排序本身在整窗内返回相关度最高的前 N，新词自然只出现在有内容的近期。
        # 候选池放大：污染过滤后需补位，搜 1.5× 页数留足余量（仍综合排序）
        pages = max(1, -(-config.N_PER_KEYWORD * 3 // 20 // 2))   # ceil(N*1.5/20)
        print(f"[A] {len(config.KEYWORDS)} 词 × 整窗 {config.START_DAY}~{config.END_DAY or '今天'}"
              f"，每词取前 {config.N_PER_KEYWORD} 条（污染词过滤+前移补位；bilibili-api 激活指纹，并发 {config.SEARCH_CONCURRENCY}）")
        results = {kw: [] for kw in config.KEYWORDS}
        todo = [kw for kw in config.KEYWORDS]
        if todo:
            sem = asyncio.Semaphore(config.SEARCH_CONCURRENCY)
            got = asyncio.run(self._search_block_batch(todo, ts(start), ts(end, end_of_day=True), pages, sem))
            # 污染判定：标题含 POLLUTION_WORDS 任一 → 剔除（用后续候选前移补位）
            pol = getattr(config, "POLLUTION_WORDS", [])
            skipped = {kw: 0 for kw in config.KEYWORDS}
            for kw, items in got.items():
                seen = set()
                for it in items:
                    if len(results[kw]) >= config.N_PER_KEYWORD:
                        break
                    bv = it.get("bvid")
                    if not bv or bv in seen:
                        continue
                    title = clean_html(it.get("title", ""))
                    if any(p.lower() in title.lower() for p in pol):
                        skipped[kw] += 1
                        continue   # 污染：剔除，等价后续前移补位
                    seen.add(bv)
                    results[kw].append({"bvid": bv, "aid": it.get("aid"),
                                        "title": title,
                                        "tags": it.get("tag", ""), "region": it.get("typeid"),
                                        "review": it.get("review", 0), "done": False})
            self._write_stage_a(results)
            skip_tot = sum(skipped.values())
            print(f"[A] 完成（剔除污染 {skip_tot} 条）：" + " ".join(f"{kw}={len(results[kw])}" for kw in todo[:8]) + "…")
        # 完成标记：全部时间窗块已拼接（词表可能个别词不满）。词表变更时由 apply_final_keywords.py 删除强制重建。
        with open(os.path.join(self.data_dir, ".stage_a_complete"), "w") as f:
            f.write(datetime.now().isoformat(timespec="seconds"))
        print(f"[A] 视频表完成：{self.csv_path}")

    def _write_stage_a(self, results: dict):
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for kw in config.KEYWORDS:
                w.writerow([kw] + [json.dumps(c, ensure_ascii=False) for c in results[kw]])

    async def _search_block_batch(self, kws: list, begin: int, end: int, pages: int, sem: asyncio.Semaphore) -> dict:
        """一个时间窗块内全词并发搜索（单事件循环共享激活指纹，无竞态）→ {词: [原始item]}。"""
        return dict(await asyncio.gather(*(self._search_one(kw, begin, end, pages, sem) for kw in kws)))

    async def _fetch_search_page(self, kw: str, begin: int, end: int, page: int,
                                base: dict, api) -> list | None:
        """单页搜索（带退避重试），返回原始 result 列表；重试耗尽/失败返回 None。"""
        params = dict(base, page=page)
        raw = None
        for attempt in range(len(config.RETRY_DELAYS)):
            try:
                _wait_cool()   # 412 全局冷却闸：闸内不产生请求
                res = await Api(**api, wbi=True).update_params(**params)._request(raw=True)
                if not isinstance(res, dict) or res.get("code") != 0:
                    raise _SoftRisk(f"code={res.get('code') if isinstance(res, dict) else res}")
                d = res.get("data") or {}
                if "result" not in d:   # HTTP 200 但 result 缺失 = 软风控，不能当空结果
                    raise _SoftRisk("data 缺 result（软风控）")
                raw = d["result"] or []
                break
            except _CoolTimeout:
                raise   # 持续风控：熔断向上传播
            except Exception as e:
                if _hit_412(e):
                    _set_cool()
                if attempt < len(config.RETRY_DELAYS) - 1:
                    await asyncio.sleep(config.RETRY_DELAYS[attempt] * random.uniform(0.6, 1.4))
        return raw

    async def _search_one(self, kw: str, begin: int, end: int, pages: int, sem: asyncio.Semaphore) -> tuple:
        """单词×单块多页搜索（bilibili-api：buvid 激活 + bili_ticket + WBI 自动处理）。
        页级缓存：key=search:{kw}|{begin}|{end}|{page}，重跑阶段A命中即秒回（412 熔断后不重复烧搜索量）。
        重试：code≠0 / data 缺 result（v_voucher 软风控形态）→ 退避重试；412 → 全局冷却闸。
        与 bk_search 的差异：result==[] 视为真实空结果（小词×早期时间窗正常为空），不重试。"""
        api = _bili_search.API["search"]["web_search_by_type"]
        base = {"keyword": kw, "search_type": "video", "page_size": 20,
                "pubtime_begin_s": int(begin), "pubtime_end_s": int(end)}
        items = []
        async with sem:
            for page in range(1, pages + 1):
                ck = f"search:{kw}|{int(begin)}|{int(end)}|{page}"
                raw = _cache_peek(ck)   # 命中缓存：零请求
                if raw is None:
                    raw = await self._fetch_search_page(kw, begin, end, page, base, api)
                    if raw:   # 只缓存非空成功页
                        _cache_store(ck, raw)
                if raw is None:
                    print(f"[A] {kw} 块{begin} 第{page}页重试耗尽，跳过本页", file=sys.stderr)
                    break
                items.extend(raw)
                if len(raw) < 20:   # 该块翻到底
                    break
                await asyncio.sleep(0.3)   # 页间轻微间隔，防同词连打
        return kw, items

    def _read_table(self) -> tuple[list, list]:
        """读矩阵 → (行=[kw, [cell...]], kw列表)。"""
        with open(self.csv_path, encoding="utf-8") as f:
            rows = list(csv.reader(f))
        return rows, [r[0] for r in rows]

    def _table_done(self) -> bool:
        if not os.path.exists(self.csv_path):
            return False
        rows, _ = self._read_table()
        return len(rows) == len(config.KEYWORDS) and os.path.exists(
            os.path.join(self.data_dir, ".stage_a_complete"))   # 需完成标记：全时间窗块拼接完才算完整

    # ---------- 阶段 B：按唯一视频爬评论（线程池 + 全局闸） ----------
    def stage_b(self):
        if not self._table_done():
            print("[B] 视频表缺失或不完整，先跑阶段A")
            return
        rows, kws = self._read_table()
        cells = {(r[0], i): json.loads(c) for r in rows for i, c in enumerate(r[1:], 1) if c}
        # 列优先：位次1全词 → 位次2全词 → …；归属词 = 首次命中格所在词
        maxpos = max((i for _, i in cells), default=0)
        owner, locs, queue = {}, {}, []
        for pos in range(1, maxpos + 1):
            for kw in kws:
                cell = cells.get((kw, pos))
                if not cell:
                    continue
                b = cell["bvid"]
                locs.setdefault(b, []).append((kw, pos))
                if b not in owner:
                    owner[b] = kw
                    queue.append(b)
        done_all = {c["bvid"] for c in cells.values() if c["done"]}
        pending = [b for b in queue if b not in done_all]
        counter = [self._recount()]   # 共享计数（list 绕开 nonlocal，锁保护）
        self._verify_login()   # 启动强制校验登录态：无效 Cookie 立即报错，不降级游客
        print(f"[B] 队列 {len(queue)} 个唯一视频（已完成 {len(queue) - len(pending)}，待爬 {len(pending)}），当前计数 {counter[0]}"
              + f"；线程 {config.STAGE_B_WORKERS}，全局速率 {1/self.request_interval:.1f} req/s"
              + (f"，从队列第 {queue.index(pending[0]) + 1} 个继续" if pending and len(queue) > len(pending) else ""))
        if not self.cookie:
            self.cookie = self._get_buvid()   # 主线程预取游客指纹，避免多线程首请求竞态
        self._refresh_wbi()
        total = len(pending)
        with ThreadPoolExecutor(max_workers=max(1, config.STAGE_B_WORKERS)) as pool:
            futs = [pool.submit(self._process_video, qi, total, b, rows, kws, cells, locs, owner, counter)
                    for qi, b in enumerate(pending, 1)]
            for f in futs:
                f.result()
        print(f"[B] 阶段结束，当前累计 {counter[0]} 条（限制 {config.TOTAL_LIMIT}）")

    def _process_video(self, qi: int, total: int, b: str, rows, kws, cells, locs, owner, counter):
        """线程 worker：单个视频的评论爬取与落盘（每 bvid 只进一个线程，文件互不冲突）。"""
        if self._abort.is_set():
            return
        with self._lock:
            if counter[0] >= config.TOTAL_LIMIT:
                return
            # ponytail: 预算先到先得，超额 ≤ workers×单视频量；需要硬截断再加预留扣减
        path = os.path.join(self.data_dir, f"{b}.jsonl")
        aid, kw = cells[locs[b][0]]["aid"], owner[b]
        pos0 = locs[b][0][1]
        if os.path.exists(path):
            have = self._jsonl_comment_count(path)
            if have > 0:
                # 结果级复用：词表轮换/热身轮已爬过的视频直接沿用（不重爬、不删数据），
                # 只把首行归属词更新为当前 owner，并置 done → 最终词表重跑时该视频零请求。
                with open(path, encoding="utf-8") as f:
                    lines = f.readlines()
                info = json.loads(lines[0])
                if info.get("source_keyword") != kw:
                    info["source_keyword"] = kw
                    lines[0] = json.dumps(info, ensure_ascii=False) + "\n"
                    with open(path, "w", encoding="utf-8") as f:
                        f.writelines(lines)
                with self._lock:
                    for kw2, pos in locs[b]:
                        cells[(kw2, pos)]["done"] = True
                    self._write_table(rows, kws, cells)
                print(f"[B] ({qi}/{total}) {b} 复用已爬数据（{have} 条评论，归属词→{kw}），跳过", flush=True)
                return
            # 半截文件（仅首行/0 评论）：先删、计数重置（have 应=0，为防异常仍扣减）
            os.remove(path)
            with self._lock:
                counter[0] -= have
            print(f"[B] 清理半截文件 {b}（{-have} 条已从计数扣除）")
        print(f"[B] ({qi}/{total}) {b} [{kw}·位次{pos0}] 爬取中…", flush=True)
        try:
            self._gate()
            detail = self._video_detail(b)  # 搜索接口 desc 恒为空，简介走详情接口
        except Exception as e:
            print(f"[B] {b} 详情接口失败（降级为空简介）：{e}", file=sys.stderr)
            detail = {}
        try:
            comments, meta = self._crawl_video(aid, b, kw)
        except _CoolTimeout:
            self._abort.set()   # 持续风控熔断：通知其余 worker 停止接新活
            print(f"\n[B] {b} 持续风控（冷却闸熔断），停止阶段B；重跑 --stage b 补齐", file=sys.stderr)
            return
        except Exception as e:
            print(f"\n[B] ({qi}/{total}) {b} 爬取失败（格子留 False，重跑补齐）：{e}", file=sys.stderr)
            return
        with open(path, "w", encoding="utf-8") as f:
            cell = cells[locs[b][0]]
            stat = detail.get("stat") or {}
            info = {"video_id": b, "aid": aid, "title": cell["title"],
                    "desc": detail.get("desc", ""), "tags": cell.get("tags", ""),
                    "region": cell.get("region"), "author": detail.get("owner", {}).get("name", ""),
                    "pubdate": detail.get("pubdate", 0), "duration": detail.get("duration", 0),
                    "play": stat.get("view", 0), "danmaku": stat.get("danmaku", 0),
                    "v_like": stat.get("like", 0), "coin": stat.get("coin", 0),
                    "favorite": stat.get("favorite", 0), "share": stat.get("share", 0),
                    "follower": self._follower(detail.get("owner", {}).get("mid")),
                    "reply_count": cell["review"],
                    "source_keyword": kw, "platform": "bilibili"}
            f.write(json.dumps(info, ensure_ascii=False) + "\n")
            for cm in comments:
                f.write(json.dumps(cm, ensure_ascii=False) + "\n")
        with self._lock:   # 计数 + 完成标记回写串行化（表为整文件重写，必须独占）
            counter[0] += len(comments)
            self._write_counter(counter[0])
            for kw2, pos in locs[b]:
                cells[(kw2, pos)]["done"] = True
            self._write_table(rows, kws, cells)
        if meta["truncated"] > 2:
            # 差额来源区分：一级真满 N_COMMENTS 上限 → 截断；否则多为已删除/折叠评论（页面计数不减）
            if meta["primary"] >= config.N_COMMENTS:
                trunc = f"，一级达 {config.N_COMMENTS} 上限截断约 {meta['truncated']} 条"
            else:
                trunc = f"，与页面计数差 {meta['truncated']} 条（多为已删除/折叠评论）"
        else:
            trunc = ""
        print(f"[B] ({qi}/{total}) {b} done: {meta['total']} 条 = 一级 {meta['primary']} + 二级 {meta['secondary']}{trunc}"
              f"（页面总评论 {meta['all_count'] or '?'}）（累计 {counter[0]}/{config.TOTAL_LIMIT}）[{kw}·位次{pos0}]")

    def _video_detail(self, bvid: str) -> dict:
        """视频详情（无签名接口）：desc/tname/owner/pubdate。搜索接口拿不到这些。带 bvid 级缓存。"""
        def loader():
            r = self.client.get(f"{API}/x/web-interface/view", params={"bvid": bvid},
                                headers={"Cookie": self.cookie})
            d = r.json().get("data") or {}
            if not d:
                raise RuntimeError(f"view 接口无数据: {r.text[:100]}")
            return d
        return _cache_get(f"detail:{bvid}", loader)

    def _follower(self, mid: int) -> int:
        """UP主粉丝数（只存数字不存 mid，避免身份 ID 入库）。带 mid 级缓存（0 也缓存，区分失败）。"""
        ck = f"follower:{mid}"
        v = _cache_peek(ck)
        if v is None:
            try:
                self._gate()
                r = self.client.get(f"{API}/x/relation/stat", params={"vmid": mid},
                                    headers={"Cookie": self.cookie})
                v = r.json().get("data", {}).get("follower")
            except Exception:
                v = None
            if v is not None:
                _cache_store(ck, v)
        return v if isinstance(v, int) else 0

    def _crawl_video(self, aid: int, bvid: str, kw: str) -> tuple[list, dict]:
        """一个视频的全部评论行，返回 (评论行, 统计meta)。"""
        primary, got, all_count = self._fetch_primary(aid, config.N_COMMENTS)
        rows, seen = [], set()

        def emit(c: dict, parent_id):
            rpid = str(c["rpid"])
            if rpid in seen:
                return
            seen.add(rpid)
            loc = (c.get("reply_control") or {}).get("location") or ""
            rows.append({
                "comment_id": rpid,
                "user_id": self._anon_id(c.get("member", {}).get("mid", "")),  # 假名化，非明文 UID
                "content": (c.get("content") or {}).get("message", ""),
                "parent_id": parent_id,
                "source_keyword": kw,
                "timestamp": c.get("ctime", 0),
                "platform": "bilibili",
                "province_ip": loc.replace("IP属地：", "") if loc else "",
                "like": c.get("like", 0),      # 评论点赞数
                "rcount": c.get("rcount", 0),  # 该评论的回复数（自评论数量）
                "up_like": int(bool((c.get("up_action") or {}).get("like"))),   # UP主是否点赞
                "up_reply": int(bool((c.get("up_action") or {}).get("reply"))), # UP主是否回复
                "fold": c.get("fold") or 0,     # 折叠数（>0 = 被折叠低质）
                "invisible": int(bool(c.get("invisible"))),  # 平台隐藏标记
                "video_id": bvid,
            })

        for c in primary:
            emit(c, None)
        roots = [c for c in primary if config.FETCH_SUB and (c.get("rcount") or 0) > 0]
        for c in roots:   # 逐 root 爬二级；分页节奏由 _api 全局闸控制（多线程下不打印进度，避免交叉刷屏）
            for sc in self._fetch_secondary(aid, c["rpid"]):
                emit(sc, str(c["rpid"]))
        n_prim = sum(1 for r in rows if not r["parent_id"])
        meta = {"total": len(rows), "primary": n_prim, "secondary": len(rows) - n_prim,
                "all_count": all_count, "truncated": max(0, all_count - len(rows)) if all_count else 0}
        return rows, meta

    def _recount(self) -> int:
        total = 0
        for fn in os.listdir(self.data_dir):
            if fn.endswith(".jsonl"):
                total += self._jsonl_comment_count(os.path.join(self.data_dir, fn))
        self._write_counter(total)
        return total

    @staticmethod
    def _jsonl_comment_count(path: str) -> int:
        with open(path, encoding="utf-8") as f:
            return sum(1 for _ in f) - 1  # 首行是视频信息

    def _write_counter(self, n: int):
        with open(self.counter_path, "w", encoding="utf-8") as f:
            f.write(json.dumps({"count": n}))

    def _write_table(self, rows, kws, cells):
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for r in rows:
                w.writerow([r[0]] + [json.dumps(cells[(r[0], i)], ensure_ascii=False)
                                     for i in range(1, len(r))])

    # ---------- 冒烟测试 ----------
    def check(self):
        if not self.cookie:
            self.cookie = self._get_buvid()
            print("[check] 游客模式（未设置 BILI_COOKIE）")
        nav = self.client.get(f"{API}/x/web-interface/nav", headers={"Cookie": self.cookie}).json()
        if nav.get("data", {}).get("isLogin"):
            print(f"[check] 登录态 OK: {nav['data'].get('uname', '')}")
        else:
            print("[check] 未登录（Cookie 无效或未填，仅游客可见度）")
        self._refresh_wbi()
        items = self.search_page("ChatGPT", 1, ts(datetime.now() - timedelta(days=7)), ts(datetime.now()))
        print(f"[check] WBI 签名 + 搜索 OK: 近7天 hit {len(items)} 条")
        if items:
            print("[check] 样例:", items[0]["bvid"], clean_html(items[0]["title"])[:40])


def main():
    ap = argparse.ArgumentParser(description="B站评论爬虫（两阶段）")
    ap.add_argument("--stage", choices=["a", "b"], help="只跑阶段A或B；缺省全跑")
    ap.add_argument("--check", action="store_true", help="冒烟测试")
    args = ap.parse_args()
    sp = BiliSpider()
    if args.check:
        sp.check()
    elif args.stage == "a":
        sp.stage_a()
    elif args.stage == "b":
        sp.stage_b()
    else:
        sp.stage_a()
        sp.stage_b()


if __name__ == "__main__":
    main()
