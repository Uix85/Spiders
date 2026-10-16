# -*- coding: utf-8 -*-
"""
微博关键词时间窗口爬虫（s.weibo.com / weibo.com）。
登录态从同目录的 cookie.txt 读取。
"""
import argparse
import html as html_lib
import json
import logging
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from itertools import product
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Tuple

import config
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger("weibo")
HERE = os.path.dirname(os.path.abspath(__file__))

SEARCH_URL = "https://s.weibo.com/weibo"
STATUS_API = "https://weibo.com/ajax/statuses/show"
LONGTEXT_API = "https://weibo.com/ajax/statuses/longtext"
MOBILE_STATUS_API = "https://m.weibo.cn/statuses/show"
MOBILE_LONGTEXT_API = "https://m.weibo.cn/statuses/extend"
EXCEL_MAX_ROWS = 1048576
PC_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)
# 搜索页里每张卡片：<div class="card-wrap" action-type="feed_list_item" mid="5118141862773896">
MID_RE = re.compile(r'<div class="card-wrap"[^>]*?\bmid="(\d+)"')
PAGE_LIST_RE = re.compile(r'<ul\b[^>]*class=["\'][^"\']*\b(?:s-scroll|page-list)\b[^"\']*["\'][^>]*>(.*?)</ul>', re.S)
PAGE_ITEM_RE = re.compile(r'<li\b')
CST = timezone(timedelta(hours=8))
WINDOW_LEN = {"hour": timedelta(hours=1), "day": timedelta(days=1)}
WINDOW_UNITS = ("hour", "day", "week", "month", "half-year", "year")
WINDOW_UNIT_LABELS = {
    "hour": "按小时", "day": "按天", "week": "按周",
    "month": "按月", "half-year": "按半年", "year": "按年",
}
WEIBO_TYPE_PARAMS = {
    0: {"typeall": "1"}, 1: {"scope": "ori"}, 2: {"xsort": "hot"},
    3: {"atten": "1"}, 4: {"vip": "1"}, 5: {"category": "4"}, 6: {"viewpoint": "1"},
}
CONTAIN_TYPE_PARAMS = {
    0: {"suball": "1"}, 1: {"haspic": "1"}, 2: {"hasvideo": "1"},
    3: {"hasmusic": "1"}, 4: {"haslink": "1"},
}
PROVINCE_CODES = {
    "北京": 11, "天津": 12, "河北": 13, "山西": 14, "内蒙古": 15,
    "辽宁": 21, "吉林": 22, "黑龙江": 23, "上海": 31, "江苏": 32,
    "浙江": 33, "安徽": 34, "福建": 35, "江西": 36, "山东": 37,
    "河南": 41, "湖北": 42, "湖南": 43, "广东": 44, "广西": 45,
    "海南": 46, "重庆": 50, "四川": 51, "贵州": 52, "云南": 53,
    "西藏": 54, "陕西": 61, "甘肃": 62, "青海": 63, "宁夏": 64,
    "新疆": 65, "台湾": 71, "香港": 81, "澳门": 82,
}


def normalize_window_unit(unit: str) -> str:
    """标准化窗口粒度；正整数表示固定天数。"""
    value = str(unit).strip().lower()
    aliases = {label[1:]: name for name, label in WINDOW_UNIT_LABELS.items()}
    label = value[1:] if value.startswith("按") else value
    value = aliases.get(label, value)
    if value == "halfyear":
        value = "half-year"
    if value in WINDOW_UNITS:
        return value
    if value.isdecimal() and int(value) > 0:
        return str(int(value))
    raise ValueError("时间窗口应为 hour/day/week/month/half-year/year 或正整数天数")


def window_unit_label(unit: str) -> str:
    unit = normalize_window_unit(unit)
    return WINDOW_UNIT_LABELS.get(unit, f"每 {unit} 天")


def validate_regions(regions: List[str]) -> List[str]:
    normalized = list(dict.fromkeys(str(region).strip() for region in regions if str(region).strip()))
    if not normalized or "全部" in normalized:
        return ["全部"]
    unknown = [region for region in normalized if region not in PROVINCE_CODES]
    if unknown:
        raise ValueError("地区只支持省或直辖市名称：" + "、".join(unknown))
    return normalized


class CookieInvalidError(Exception):
    """cookie 无效：搜索页被重定向到登录页"""


class DataFetchError(Exception):
    """接口返回异常"""


class RequestBlockedError(DataFetchError):
    """有限冷却重试后，服务端仍拒绝或限制请求。"""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """不自动跟随跳转：302 要能被识别出来（说明 cookie 没登录）"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def retry_after_seconds(value: Optional[str]) -> Optional[float]:
    """解析 Retry-After 的秒数或 HTTP 日期。"""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


def strip_html(text: str) -> str:
    """微博正文是 HTML，转成纯文本"""
    text = re.sub(r"<br\s*/?>", "\n", text or "")
    text = re.sub(r"<.*?>", "", text)
    return html_lib.unescape(text).replace("\u200b", "").strip()


def read_lines(filename: str) -> List[str]:
    """读取项目内文本文件，忽略空行和 # 注释；也支持绝对路径。"""
    path = os.path.join(HERE, filename)
    if not os.path.isfile(path):
        return []
    with open(path, encoding="utf-8-sig") as source:
        return [line.strip() for line in source if line.strip() and not line.lstrip().startswith("#")]


def gen_windows(start: date, end: date, unit: str) -> List[Tuple[str, str]]:
    """生成左闭右开的窗口；保留原有 key 格式，兼容已有 .done 文件。"""
    unit = normalize_window_unit(unit)
    windows = []
    stop = end + timedelta(days=1)
    if unit == "hour":
        current = datetime.combine(start, datetime.min.time())
        while current.date() < stop:
            right = current + timedelta(hours=1)
            key = current.strftime("%Y-%m-%d-%H")
            windows.append((key, f"{key}:{right:%Y-%m-%d-%H}"))
            current = right
        return windows

    day = start
    while day < stop:
        if unit == "day" or unit.isdecimal():
            span = 1 if unit == "day" else min(int(unit), (stop - day).days)
            right = min(day + timedelta(days=span), stop)
        elif unit == "week":
            right = day + timedelta(days=min(7 - day.weekday(), (stop - day).days))
        else:
            months = {"month": 1, "half-year": 6, "year": 12}[unit]
            next_month = (day.month - 1) // months * months + months
            year_offset, month_index = divmod(next_month, 12)
            right = stop if day.year + year_offset > date.max.year else min(
                date(day.year + year_offset, month_index + 1, 1), stop
            )
        last = right - timedelta(days=1)
        key = f"{day:%Y-%m-%d}" if unit == "day" else f"{day:%Y-%m-%d}__{last:%Y-%m-%d}"
        windows.append((key, f"{day:%Y-%m-%d}-0:{right:%Y-%m-%d}-0"))
        day = right
    return windows


def text_matches(text: str, keyword: str) -> bool:
    """过滤微博宽松搜索结果；忽略大小写，空格分隔的词都必须出现。"""
    low = (text or "").lower()
    return all(word.lower() in low for word in keyword.split())


def window_bounds(key: str, unit: str) -> Tuple[datetime, datetime]:
    unit = normalize_window_unit(unit)
    if unit in WINDOW_LEN:
        fmt = "%Y-%m-%d-%H" if unit == "hour" else "%Y-%m-%d"
        start = datetime.strptime(key, fmt).replace(tzinfo=CST)
        return start, start + WINDOW_LEN[unit]
    left, right = key.split("__", 1)
    start = datetime.strptime(left, "%Y-%m-%d").replace(tzinfo=CST)
    end = datetime.strptime(right, "%Y-%m-%d").replace(tzinfo=CST) + timedelta(days=1)
    return start, end


def parse_time(created_at: str) -> Optional[datetime]:
    """微博的 created_at 是 RFC2822（"Wed Jan 01 22:59:33 +0800 2025"），转成北京时间。"""
    try:
        return parsedate_to_datetime(created_at).astimezone(CST)
    except (TypeError, ValueError):
        return None


def to_record(status: Dict, text: str, keyword: str, window: str, dt: datetime) -> Dict:
    """抽取需求字段（外加 keyword/window 便于溯源）。"""
    user = status.get("user") or {}
    return {
        "keyword": keyword,
        "window": window,
        "publish_year": dt.year,
        "user_id": str(user.get("id") or ""),
        "ip_location": status.get("region_name") or "",
        "post_geo": status.get("geo") or None,
        "weibo_id": str(status.get("idstr") or status.get("id") or ""),
        "publish_time": dt.strftime("%Y-%m-%d %H:%M"),
        "content": text,
        "like_count": status.get("attitudes_count"),
        "comment_count": status.get("comments_count"),
    }


def build_search_url(keyword: str, timescope: Optional[str] = None, page: int = 1,
                     weibo_type: int = 1, contain_type: int = 0,
                     region: Optional[str] = None) -> str:
    """timescope 形如 2025-01-01-0:2025-01-02-0（精确到小时），是绕开翻页上限的关键"""
    if weibo_type not in WEIBO_TYPE_PARAMS or contain_type not in CONTAIN_TYPE_PARAMS:
        raise ValueError("微博类型或内容筛选值无效")
    params = {
        "q": keyword,
        **WEIBO_TYPE_PARAMS[weibo_type],
        **CONTAIN_TYPE_PARAMS[contain_type],
        "Refer": "g",
        "page": str(page),
    }
    if timescope:
        params["timescope"] = f"custom:{timescope}"
    if region:
        if region not in PROVINCE_CODES:
            raise ValueError(f"不支持的地区: {region}")
        params["region"] = f"custom:{PROVINCE_CODES[region]}:1000"
    return f"{SEARCH_URL}?{urllib.parse.urlencode(params)}"


def extract_mids(page_html: str) -> List[str]:
    """抠出搜索页里所有微博 mid"""
    return MID_RE.findall(page_html or "")


def extract_page_count(page_html: str) -> int:
    """兼容新版“共 N 页”和旧版分页列表。"""
    total = re.search(r'共\s*(\d+)\s*页', page_html or "")
    if total:
        return int(total.group(1))
    match = PAGE_LIST_RE.search(page_html or "")
    if not match:
        return 0
    pages = re.findall(r'page=(\d+)', match.group(1))
    return max(map(int, pages)) if pages else len(PAGE_ITEM_RE.findall(match.group(1)))


def parse_scope_boundary(value: str) -> datetime:
    day, hour = value.rsplit("-", 1)
    return datetime.strptime(f"{day}-{int(hour):02d}", "%Y-%m-%d-%H").replace(tzinfo=CST)


def split_timescope(timescope: str) -> List[str]:
    """较宽时间范围先拆到自然日，单日范围再拆成小时。"""
    left, right = timescope.split(":", 1)
    start, end = parse_scope_boundary(left), parse_scope_boundary(right)
    if end - start <= timedelta(hours=1):
        return []
    children = []
    if end - start <= timedelta(days=1):
        step = timedelta(hours=1)
    else:
        step = timedelta(days=1)
    current = start
    while current < end:
        next_boundary = min(current + step, end)
        children.append(f"{format_scope_boundary(current)}:{format_scope_boundary(next_boundary)}")
        current = next_boundary
    return children


def format_scope_boundary(value: datetime) -> str:
    return f"{value:%Y-%m-%d}-{value.hour}"


class WeiboPcClient:
    def __init__(self, cookie_str: str, timeout: int = 30, retries: int = 3,
                 request_interval: Tuple[float, float] = (2.0, 4.0), cooldown_seconds: float = 30.0):
        self.timeout = timeout
        self.retries = retries
        self.request_interval = (max(0.0, request_interval[0]), max(0.0, request_interval[1]))
        self.last_request_at = 0.0
        self.cooldown_seconds = max(0.0, cooldown_seconds)
        self.cooldown_until = 0.0
        self.cooldown_reason = ""
        self.headers = {
            "User-Agent": PC_UA,
            "Cookie": cookie_str,
            "Referer": "https://s.weibo.com/",
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
        self.opener = urllib.request.build_opener(NoRedirect)

    def wait_for_request_slot(self) -> None:
        """按随机间隔节流；服务端要求冷却时，先等冷却结束。"""
        wait = self.cooldown_until - time.monotonic()
        if wait > 0:
            logger.warning("[get] %s，自动冷却 %.1f 秒", self.cooldown_reason, wait)
            time.sleep(wait)
        interval = random.uniform(*self.request_interval)
        wait = interval - (time.monotonic() - self.last_request_at)
        if wait > 0:
            time.sleep(wait)
        self.last_request_at = time.monotonic()

    def start_cooldown(self, seconds: float, reason: str) -> None:
        self.cooldown_until = max(self.cooldown_until, time.monotonic() + seconds)
        self.cooldown_reason = reason

    def get(self, url: str, headers: Optional[Dict[str, str]] = None,
            timeout: Optional[int] = None) -> Tuple[int, str]:
        """GET 请求带有限重试；登录跳转立即停止，访问限制先冷却再重试。"""
        last_err: object = "unknown"
        for attempt in range(1, self.retries + 1):
            self.wait_for_request_slot()
            try:
                req = urllib.request.Request(url, headers=headers or self.headers)
                with self.opener.open(req, timeout=timeout or self.timeout) as resp:
                    return resp.getcode(), resp.read().decode("utf-8", "replace")
            except urllib.error.HTTPError as ex:
                if ex.code in (301, 302, 303, 307):
                    raise CookieInvalidError(f"被重定向到 {ex.headers.get('location', '登录页')}")
                if ex.code in (403, 418, 429):
                    retry_after = retry_after_seconds(ex.headers.get("Retry-After"))
                    cooldown = retry_after if retry_after is not None else min(
                        self.cooldown_seconds * (2 ** (attempt - 1)), 300.0
                    )
                    last_err = f"HTTP {ex.code}"
                    self.start_cooldown(cooldown, last_err)
                    if attempt == self.retries:
                        raise RequestBlockedError(
                            f"{last_err} 经 {self.retries} 次请求和冷却后仍未恢复，已停止本次采集。"
                        )
                    continue
                if 400 <= ex.code < 500 and ex.code != 408:
                    raise DataFetchError(f"HTTP {ex.code}: {url}")
                last_err = f"HTTP {ex.code}"
            except Exception as ex:  # 网络错误、超时、证书等
                last_err = ex
            self.start_cooldown(min(3.0 * (2 ** (attempt - 1)), 60.0), str(last_err))
            logger.warning(f"[get] 第 {attempt}/{self.retries} 次失败({last_err}): {url[:90]}")
        raise DataFetchError(f"请求失败 {url}: {last_err}")

    def search_page(self, keyword: str, timescope: Optional[str] = None, page: int = 1,
                    weibo_type: int = 1, contain_type: int = 0,
                    region: Optional[str] = None) -> Tuple[List[str], int]:
        _, body = self.get(build_search_url(
            keyword, timescope, page, weibo_type, contain_type, region
        ))
        # 空结果页也会展示推荐微博，不能把这些卡片当作检索结果。
        if re.search(r'class=["\'][^"\']*\bcard-no-result\b', body):
            return [], 0
        mids = extract_mids(body)
        if not mids:
            raise DataFetchError("搜索页缺少结果卡片和空结果标记，可能需要验证或页面结构已变化")
        return mids, extract_page_count(body)

    def get_json(self, url: str, mid: str, timeout=None, required=False) -> Dict:
        """统一详情接口请求；可选补查遇到非 JSON 响应时返回空对象。"""
        _, body = self.get(url, timeout=timeout, headers={
            **self.headers, "Referer": f"https://weibo.com/{mid}",
            "X-Requested-With": "XMLHttpRequest",
        })
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            if required:
                raise DataFetchError(f"{mid} 详情不是 JSON: {body[:80]}")
            return {}
        if isinstance(data, dict):
            return data
        if required:
            raise DataFetchError(f"{mid} 详情格式异常: {str(data)[:80]}")
        return {}

    def get_status(self, mid: str) -> Dict:
        status = self.get_json(f"{STATUS_API}?id={mid}", mid, required=True)
        if not status.get("created_at") or not status.get("user") or not (status.get("idstr") or status.get("id")):
            raise DataFetchError(f"{mid} 详情缺少微博 ID、用户或时间，不能当作有效帖子保存")
        return status

    def get_ip_location(self, mid: str, timeout: int) -> str:
        data = self.get_json(
            f"{STATUS_API}?id={urllib.parse.quote(mid)}&locale=zh-CN", mid, timeout=timeout, required=True
        )
        if not (data.get("idstr") or data.get("id")):
            raise DataFetchError(f"{mid} 属地补查返回无效详情，不能视为属地为空")
        location = str(data.get("region_name") or "")
        return location

    def get_long_text(self, mid: str) -> str:
        try:
            data = self.get_json(f"{LONGTEXT_API}?id={mid}", mid, required=True)
            payload = data.get("data")
            content = payload.get("longTextContent") if isinstance(payload, dict) else ""
            text = strip_html(content or data.get("longTextContent") or "")
            if text:
                return text
        except RequestBlockedError:
            raise
        except DataFetchError:
            pass

        try:
            data = self.get_json(f"{MOBILE_STATUS_API}?id={mid}", mid, required=True)
            status = data.get("data") if isinstance(data.get("data"), dict) else data
            if "isLongText" in status and not status["isLongText"]:
                text = (status.get("text_raw") or "").strip() or strip_html(status.get("text") or "")
                if text:
                    return text
        except RequestBlockedError:
            raise
        except DataFetchError:
            pass

        try:
            data = self.get_json(f"{MOBILE_LONGTEXT_API}?id={mid}", mid, required=True)
        except RequestBlockedError:
            raise
        except DataFetchError as error:
            raise DataFetchError(f"{mid} 长文接口均未返回完整正文，将保留窗口以便重试") from error
        payload = data.get("data")
        content = payload.get("longTextContent") if isinstance(payload, dict) else ""
        text = strip_html(content or data.get("longTextContent") or "")
        if not text:
            raise DataFetchError(f"{mid} 长文接口均未返回完整正文，将保留窗口以便重试")
        return text

    def status_text(self, status: Dict) -> str:
        raw = status.get("text_raw")
        text = raw.strip() if raw else strip_html(status.get("text") or "")
        if status.get("isLongText"):
            mid = str(status.get("idstr") or status.get("id") or "")
            text = self.get_long_text(mid)
        return text

    def get_note(self, mid: str, start: Optional[datetime] = None,
                 end: Optional[datetime] = None) -> Tuple[Dict, str]:
        """返回 (详情, 完整正文)。长文补全只多花一次请求"""
        status = self.get_status(mid)
        if start is not None:
            dt = parse_time(status.get("created_at"))
            if dt is None:
                raise DataFetchError(f"{mid} 发布时间无法解析")
            if not start <= dt < end:
                return status, ""  # 先排除窗口外的帖子，避免无用的长文补查。
        text = self.status_text(status)
        original = status.get("retweeted_status")
        if isinstance(original, dict):
            original_text = self.status_text(original)
            if not original_text:
                raise DataFetchError(f"{mid} 转发原文不可获取，正文尚不完整")
            text += "\n\n[转发原文]\n" + original_text
        if not text:
            raise DataFetchError(f"{mid} 正文为空")
        return status, text


def configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure") and (stream.encoding or "").lower() != "utf-8":
            stream.reconfigure(encoding="utf-8", errors="replace")


def read_cookie() -> str:
    lines = read_lines("cookie.txt")
    if not lines:
        sys.exit("请将电脑版 weibo.com 的 Cookie 放到本文件夹 cookie.txt 的第一行。")
    return lines[0]


class WindowCrawler:
    def __init__(self, client: WeiboPcClient, out_path: str):
        self.client = client
        self.out_path = out_path
        self.done_path = out_path + ".done"
        self.regions = validate_regions(config.WEIBO_REGION)
        self.limit_remaining = config.WEIBO_LIMIT_RESULT or None
        self.seen = {}  # ID -> 已保存的发布时间；复查时可辨认窗口内的旧记录。
        self.done = set()
        self.last_window_truncated = False
        self.window_errors = self.problem_windows = 0
        repair_jsonl_tail(self.out_path)
        repair_jsonl_tail(self.out_path + ".status.jsonl")
        self.load_state()
        if config.WEIBO_LIMIT_RESULT:
            self.limit_remaining = max(0, config.WEIBO_LIMIT_RESULT - len(self.seen))

    @property
    def last_window_limited(self) -> bool:
        return self.limit_remaining is not None and self.limit_remaining <= 0

    def load_state(self) -> None:
        if os.path.exists(self.out_path):
            with open(self.out_path, encoding="utf-8") as source:
                for line in source:
                    try:
                        record = json.loads(line)
                        self.seen[str(record["weibo_id"])] = record.get("publish_time")
                    except (json.JSONDecodeError, KeyError):
                        continue
        if os.path.exists(self.done_path):
            with open(self.done_path, encoding="utf-8") as source:
                self.done = {line.strip() for line in source if line.strip()}
        status_path = self.out_path + ".status.jsonl"
        if os.path.exists(status_path):
            with open(status_path, encoding="utf-8") as source:
                for line in source:
                    record = json.loads(line)
                    key = f"{record['keyword']}\t{record['window']}"
                    if record["status"] == "completed":
                        self.done.add(key)
                    else:
                        self.done.discard(key)
        logger.info("[续爬] 当前输出文件已有 %s 条微博", len(self.seen))

    def mark_done(self, keyword: str, window: str) -> None:
        key = f"{keyword}\t{window}"
        self.done.add(key)
        with open(self.done_path, "a", encoding="utf-8") as target:
            target.write(key + "\n")

    def record_window(self, keyword: str, window: str, status: str) -> None:
        if status != "completed":
            self.done.discard(f"{keyword}\t{window}")
        with open(self.out_path + ".status.jsonl", "a", encoding="utf-8") as target:
            record = {"keyword": keyword, "window": window, "status": status,
                      "errors": self.window_errors, "updated_at": datetime.now(CST).isoformat()}
            target.write(json.dumps(record, ensure_ascii=False) + "\n")

    def crawl_window(self, keyword: str, window: str, timescope: str, unit: str) -> int:
        self.last_window_truncated = False
        self.window_errors = self.window_in_range = 0
        self.window_out_of_range = self.window_unexpected_range = 0
        self.window_no_keyword = self.window_no_location = 0
        total = 0
        start, end = window_bounds(window, unit)
        regions = [None] if self.regions == ["全部"] else self.regions
        for region in regions:
            if self.last_window_limited:
                break
            total += self._crawl_scope(keyword, window, timescope, region, start, end)
        if self.window_unexpected_range and not self.window_in_range:
            logger.warning("%s %s: %s 条落在窗口外，请检查时间筛选", keyword, window, self.window_unexpected_range)
            self.window_errors += 1
        if self.window_no_keyword:
            logger.warning("%s %s: %s 条因正文不含关键词被丢弃", keyword, window, self.window_no_keyword)
        if self.window_no_location:
            logger.warning("%s %s: %s 条没有属地", keyword, window, self.window_no_location)
        return total

    def _crawl_scope(self, keyword, window, timescope, region, start, end):
        mids, page_count = self.client.search_page(
            keyword, timescope, 1, config.WEIBO_TYPE, config.WEIBO_CONTAIN_TYPE, region
        )
        children = split_timescope(timescope) if page_count >= config.WEIBO_FURTHER_THRESHOLD else []
        if not children:
            return self._crawl_leaf(keyword, window, timescope, region, start, end, mids, page_count)
        logger.warning("%s %s: 结果接近分页上限（%s 页），拆成 %s 段", keyword, window, page_count, len(children))
        total = 0
        for child in children:
            if self.last_window_limited:
                break
            total += self._crawl_scope(keyword, window, child, region, start, end)
        return total

    def _crawl_leaf(self, keyword, window, timescope, region, start, end, first_mids, page_count):
        got, stale_pages = 0, 0
        handled = set()
        max_pages = min(config.MAX_PAGES, page_count) if page_count else config.MAX_PAGES
        for page in range(1, max_pages + 1):
            if self.last_window_limited:
                break
            mids = first_mids
            if page > 1:
                mids, _ = self.client.search_page(
                    keyword, timescope, page, config.WEIBO_TYPE, config.WEIBO_CONTAIN_TYPE, region
                )
            if not mids:
                break
            page_has_seen = any(mid in self.seen for mid in mids)
            left, right = start.strftime("%Y-%m-%d %H:%M"), end.strftime("%Y-%m-%d %H:%M")
            self.window_in_range += sum(
                left <= self.seen[mid] < right for mid in mids
                if mid in self.seen and self.seen[mid]
            )
            fresh = [mid for mid in mids if mid not in handled and mid not in self.seen]
            page_got, page_in_window = 0, 0
            for mid in fresh:
                handled.add(mid)
                try:
                    status, text = self.client.get_note(mid, start, end)
                except (CookieInvalidError, RequestBlockedError):
                    raise
                except DataFetchError as error:
                    self.window_errors += 1
                    logger.warning("%s %s: mid %s 详情失败，跳过: %s", keyword, window, mid, error)
                    continue
                dt = parse_time(status.get("created_at"))
                if dt is None:
                    self.window_errors += 1
                    continue
                if not start <= dt < end:
                    self.window_out_of_range += 1
                    self.window_unexpected_range += dt != end  # 接口会返回恰好落在右端点的帖子。
                    continue
                page_in_window += 1
                self.window_in_range += 1
                if not config.KEEP_ALL and not text_matches(text, keyword):
                    self.window_no_keyword += 1
                    continue
                if config.WEIBO_FETCH_IP and not status.get("region_name"):
                    try:
                        status["region_name"] = self.client.get_ip_location(mid, config.IP_REQUEST_TIMEOUT)
                    except (CookieInvalidError, RequestBlockedError):
                        raise
                    except DataFetchError as error:
                        self.window_errors += 1
                        logger.warning("[ip] %s 属地补查失败，保留窗口以便重试: %s", mid, error)
                        continue
                record = to_record(status, text, keyword, window, dt)
                if not record["ip_location"]:
                    self.window_no_location += 1
                if record["weibo_id"] in self.seen:
                    continue
                self.fp.write(json.dumps(record, ensure_ascii=False) + "\n")
                self.seen[record["weibo_id"]] = record["publish_time"]
                got += 1
                page_got += 1
                if self.limit_remaining is not None:
                    self.limit_remaining -= 1
                    if self.last_window_limited:
                        break
            self.fp.flush()
            logger.info("%s %s 第 %s 页: 卡片 %s 条，新增 %s，累计 %s", keyword, window, page, len(mids), page_got, got)
            if self.last_window_limited:
                break
            # 已保存的微博仍说明这一页有内容，续跑时应继续翻到后面的新记录。
            stale_pages = stale_pages + 1 if page_in_window == 0 and not page_has_seen else 0
            if stale_pages >= 2:
                logger.info("%s %s: 连续无窗口内新增，停止翻页", keyword, window)
                break
        else:
            if page_count and page_count < 50 and page_count <= config.MAX_PAGES:
                return got
            self.last_window_truncated = True
            logger.warning("%s %s: 已翻满 %s 页上限，仍可能有未显示结果", keyword, window, config.MAX_PAGES)
        return got

    def run(self, keywords: List[str], windows: List[Tuple[str, str]], unit: str) -> int:
        os.makedirs(os.path.dirname(os.path.abspath(self.out_path)), exist_ok=True)
        total, fails = 0, 0
        with open(self.out_path, "a", encoding="utf-8") as self.fp:
            with logging_redirect_tqdm(), tqdm(
                total=len(keywords) * len(windows), desc="采集进度", unit="窗口", dynamic_ncols=True
            ) as progress:
                for keyword, (window, timescope) in product(keywords, windows):
                    if self.last_window_limited:
                        break
                    progress.set_postfix_str(f"{keyword} {window}", refresh=False)
                    try:
                        now = datetime.now(CST)
                        start, end = window_bounds(window, unit)
                        if start > now:
                            continue
                        completed = f"{keyword}\t{window}" in self.done
                        if completed and (end > now or end <= now - timedelta(hours=config.RECHECK_HOURS)):
                            continue
                        got = self.crawl_window(keyword, window, timescope, unit)
                        fails = 0
                        total += got
                        status = "failed" if self.window_errors else "truncated" if self.last_window_truncated else (
                            "limited" if self.last_window_limited else "completed" if end <= now else "open"
                        )
                        self.record_window(keyword, window, status)
                        self.problem_windows += status in ("failed", "truncated")
                        if status == "completed":
                            self.mark_done(keyword, window)
                        if self.last_window_limited:
                            logger.warning("达到数量上限 %s，停止后续窗口", config.WEIBO_LIMIT_RESULT)
                            break
                    except (CookieInvalidError, RequestBlockedError):
                        self.record_window(keyword, window, "blocked")
                        raise
                    except KeyboardInterrupt:
                        self.record_window(keyword, window, "paused")
                        raise
                    except DataFetchError as error:
                        self.window_errors += 1
                        self.problem_windows += 1
                        self.record_window(keyword, window, "failed")
                        fails += 1
                        logger.error("%s %s 失败(%s): %s", keyword, window, fails, error)
                        if fails >= 5:
                            raise
                    finally:
                        progress.update()
        if self.problem_windows:
            logger.warning("有 %s 个失败或截断窗口未标为完成，重跑会继续检查；详见 .status.jsonl", self.problem_windows)
        return total


def repair_jsonl_tail(path: str) -> None:
    """补全有效末行的换行符，丢弃被强制中断留下的不完整末行。"""
    try:
        size = os.path.getsize(path)
    except OSError:
        return
    if not size:
        return
    with open(path, "r+b") as output:
        output.seek(-1, os.SEEK_END)
        if output.read(1) == b"\n":
            return
        position, tail, boundary = size, b"", 0
        while position > 0:
            block_size = min(65536, position)
            position -= block_size
            output.seek(position)
            block = output.read(block_size)
            newline = block.rfind(b"\n")
            tail = block[newline + 1:] + tail
            if newline >= 0:
                boundary = position + newline + 1
                break
        try:
            json.loads(tail.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            output.truncate(boundary)
        else:
            output.seek(0, os.SEEK_END)
            output.write(b"\n")



def export_xlsx(jsonl_path: str, output_dir: Optional[str] = None) -> Tuple[str, int]:
    """流式导出结果；ID 按文本保存，原始 JSONL 继续作为续跑文件。"""
    from openpyxl import Workbook
    from openpyxl.cell import WriteOnlyCell
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

    fields = ("publish_year", "user_id", "ip_location", "post_geo", "weibo_id", "publish_time",
              "content", "like_count", "comment_count")
    headers = ("发布年份", "用户ID", "帖子 IP 属地", "帖子附带的地理数据", "微博ID", "发布时间",
               "完整微博正文", "点赞数", "评论数")
    target_dir = os.path.abspath(output_dir or HERE)
    os.makedirs(target_dir, exist_ok=True)
    target_name = os.path.splitext(os.path.basename(jsonl_path))[0] + ".xlsx"
    target = os.path.join(target_dir, target_name)
    book = Workbook(write_only=True)
    count = 0
    sheets = []
    sheets_by_keyword = {}
    used_titles = set()

    def new_sheet(keyword: str, part: int = 1):
        raw_title = keyword if part == 1 else f"{keyword}_{part}"
        base_title = re.sub(r'[\\/*?:\[\]]', "_", raw_title).strip("'") or "未分类"
        base_title = base_title[:31]
        title = base_title
        suffix_number = 2
        while title.lower() in used_titles:
            suffix = f"_{suffix_number}"
            title = base_title[:31 - len(suffix)] + suffix
            suffix_number += 1
        used_titles.add(title.lower())
        sheet = book.create_sheet(title)
        sheet.freeze_panes = "A2"
        for col, width in zip("ABCDEFGHI", (12, 22, 20, 55, 24, 22, 90, 12, 12)):
            sheet.column_dimensions[col].width = width
        cells = [WriteOnlyCell(sheet, value=header) for header in headers]
        for cell in cells:
            cell.font = Font(bold=True)
            cell.fill = PatternFill("solid", fgColor="E7E6E6")
        sheet.append(cells)
        sheets.append([sheet, 0])
        return sheet

    with open(jsonl_path, encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            record = json.loads(line)
            keyword = str(record.get("keyword") or "未分类")
            state = sheets_by_keyword.get(keyword)
            if state is None or state["rows"] == EXCEL_MAX_ROWS - 1:
                part = 1 if state is None else state["part"] + 1
                sheet = new_sheet(keyword, part)
                state = {"sheet": sheet, "rows": 0, "part": part, "sheet_index": len(sheets) - 1}
                sheets_by_keyword[keyword] = state
            sheet = state["sheet"]
            cells = []
            for field in fields:
                value = record.get(field)
                if field in ("user_id", "weibo_id"):
                    value = str(value or "")
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
                if isinstance(value, str):
                    value = ILLEGAL_CHARACTERS_RE.sub("", value)
                    value = re.sub(r'[\ud800-\udfff]', "", value)
                    if len(value.encode("utf-16-le")) // 2 > 32767:
                        for worksheet in book.worksheets:
                            worksheet.close()
                        raise ValueError(f"第 {line_number} 条 {field} 超出 Excel 单元格长度；完整内容保留在 JSONL")
                cell = WriteOnlyCell(sheet, value=value)
                if isinstance(value, str):
                    cell.data_type = "s"  # 正文中的 = 等符号属于文本，不能转换成公式。
                if field in ("user_id", "weibo_id"):
                    cell.number_format = "@"
                if field == "publish_time" and value:
                    cell.value = datetime.strptime(value, "%Y-%m-%d %H:%M")
                    cell.number_format = "yyyy-mm-dd hh:mm"
                cell.alignment = Alignment(vertical="top", wrap_text=field in ("post_geo", "content"))
                cells.append(cell)
            sheet.append(cells)
            state["rows"] += 1
            sheets[state["sheet_index"]][1] = state["rows"]
            count += 1
    if not sheets:
        new_sheet("无数据")
    for sheet, rows in sheets:
        sheet.auto_filter.ref = f"A1:I{rows + 1}"
    book.save(target + ".tmp")
    os.replace(target + ".tmp", target)
    return target, count


def selftest() -> None:
    """不联网的窗口、解析和关键词匹配自检。"""
    days = gen_windows(date(2022, 11, 1), date(2022, 11, 2), "day")
    assert len(days) == 2 and days[0] == ("2022-11-01", "2022-11-01-0:2022-11-02-0")
    hours = gen_windows(date(2025, 1, 1), date(2025, 1, 1), "hour")
    assert len(hours) == 24
    for hour, (key, scope) in enumerate(hours):
        start = datetime(2025, 1, 1, hour, tzinfo=CST)
        end = start + timedelta(hours=1)
        assert key == start.strftime("%Y-%m-%d-%H")
        assert scope == f"{start:%Y-%m-%d-%H}:{end:%Y-%m-%d-%H}"
        assert window_bounds(key, "hour") == (start, end)
    assert window_bounds("2025-01-01", "day") == (
        datetime(2025, 1, 1, tzinfo=CST), datetime(2025, 1, 2, tzinfo=CST)
    )
    url = build_search_url("人工智能", "2025-01-01-0:2025-01-02-0", page=3)
    assert "q=%E4%BA%BA%E5%B7%A5%E6%99%BA%E8%83%BD" in url and "page=3" in url
    assert "timescope=custom%3A2025-01-01-0%3A2025-01-02-0" in url
    assert "timescope" not in build_search_url("a")
    assert extract_mids('<div class="card-wrap" mid="11"><div class="card-wrap" mid="12">') == ["11", "12"]
    assert extract_mids("<html>登录</html>") == []
    status = {
        "id": 11, "idstr": "11", "created_at": "Wed Jan 01 22:59:33 +0800 2025",
        "region_name": "发布于 广东", "attitudes_count": 104, "comments_count": 0, "user": {"id": 77},
    }
    dt = parse_time(status["created_at"])
    assert to_record(status, "正文", "人工智能", "2025-01-01", dt) == {
        "keyword": "人工智能", "window": "2025-01-01", "publish_year": 2025, "user_id": "77",
        "ip_location": "发布于 广东", "post_geo": None,
        "weibo_id": "11", "publish_time": "2025-01-01 22:59",
        "content": "正文", "like_count": 104, "comment_count": 0,
    }
    for text, keyword, expected in [
        ("创意岗位被ai替代真的很可惜", "AI替代", True), ("是AI替代不了的。", "AI替代", True),
        ("麦ai ai~~~荷e e~塘", "AI替代", False), ("很焦虑，怕被替代", "AI失业", False),
        ("AI 让人失业，很焦虑", "AI 失业", True), ("AI 相关讨论", "AI 失业", False),
    ]:
        assert text_matches(text, keyword) == expected
    print("selftest ok")


def prepare_run() -> Tuple[List[str], List[Tuple[str, str]], str, str]:
    today = datetime.now(CST).strftime("%Y-%m-%d")
    start = datetime.strptime(config.START_DATE or today, "%Y-%m-%d").date()
    end = datetime.strptime(config.END_DATE or today, "%Y-%m-%d").date()
    if start > end:
        raise ValueError("开始日期不能晚于结束日期")
    unit = normalize_window_unit(config.TIME_UNIT)
    keywords = read_lines(config.KEYWORDS_FILE)
    if not keywords:
        raise ValueError(f"没有有效关键词，请检查 KEYWORDS_FILE：{config.KEYWORDS_FILE}")
    if config.WEIBO_TYPE not in WEIBO_TYPE_PARAMS or config.WEIBO_CONTAIN_TYPE not in CONTAIN_TYPE_PARAMS:
        raise ValueError("WEIBO_TYPE 应为 0 到 6，WEIBO_CONTAIN_TYPE 应为 0 到 4")
    if not 1 <= config.WEIBO_FURTHER_THRESHOLD <= 50:
        raise ValueError("WEIBO_FURTHER_THRESHOLD 必须在 1 到 50 之间")
    if config.MAX_PAGES < 1 or config.WEIBO_LIMIT_RESULT < 0:
        raise ValueError("MAX_PAGES 必须大于 0，WEIBO_LIMIT_RESULT 不能小于 0")
    if config.RECHECK_HOURS < 0 or config.IP_REQUEST_TIMEOUT < 1 or config.COOLDOWN_SECONDS < 0:
        raise ValueError("重查和冷却时间不能为负，IP_REQUEST_TIMEOUT 必须大于 0")
    interval = config.REQUEST_INTERVAL
    if len(interval) != 2 or not 0 <= interval[0] <= interval[1]:
        raise ValueError("REQUEST_INTERVAL 应为满足 0 <= min <= max 的二元组")
    validate_regions(config.WEIBO_REGION)
    windows = gen_windows(start, end, unit)
    output = os.path.join(HERE, config.OUTPUT_FILE or f"data/weibo_{start}_{end}.jsonl")
    logger.info("%s ~ %s，%s 个关键词 × %s 个%s窗口 -> %s",
                start, end, len(keywords), len(windows), window_unit_label(unit), output)
    return keywords, windows, unit, output


def main() -> None:
    parser = argparse.ArgumentParser(description="微博关键词时间窗口爬虫")
    parser.add_argument("--selftest", action="store_true", help="运行本地逻辑自检，不联网")
    if parser.parse_args().selftest:
        selftest()
        return
    keywords, windows, unit, output = prepare_run()
    client = WeiboPcClient(read_cookie(), request_interval=config.REQUEST_INTERVAL,
                           cooldown_seconds=config.COOLDOWN_SECONDS)
    crawler = WindowCrawler(client, output)
    try:
        count = crawler.run(keywords, windows, unit)
    finally:
        if config.EXPORT_EXCEL and os.path.exists(output):
            excel, rows = export_xlsx(output)
            logger.info("已导出 %s 条微博、九列数据 -> %s", rows, excel)
        elif not config.EXPORT_EXCEL:
            logger.info("Excel 导出已关闭，仅保存 JSONL")
    if crawler.problem_windows:
        raise DataFetchError("采集含失败或截断窗口，结果已保存，但尚未全部完成；可重跑继续检查")
    logger.info("本次采集结束，新增 %s 条 -> %s", count, output)


if __name__ == "__main__":
    configure_stdio()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        print("\n已中断，重新执行同一命令可续爬")
        sys.exit(130)
    except (CookieInvalidError, DataFetchError) as error:
        logging.error("%s", error)
        sys.exit(1)
