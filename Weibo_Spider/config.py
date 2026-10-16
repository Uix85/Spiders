# 微博爬虫运行设置。修改这里后运行：uv run python spider.py

# 关键词文件：每行一个关键词，空行和 # 注释行会忽略。
KEYWORDS_FILE = "关键词.txt"
# 日期格式 YYYY-MM-DD，结束日期包含当天；留空时使用今天。
START_DATE = "2022-11-01"
END_DATE = "2026-07-10"
# 时间窗口：hour/day/week/month/half-year/year，或正整数天数。
TIME_UNIT = "hour"
# True 保留正文不含关键词的帖子；False 按关键词过滤。
KEEP_ALL = True

# 微博类型：0 全部，1 原创，2 热门，3 关注人，4 认证用户，5 媒体，6 观点。
WEIBO_TYPE = 0
# 内容筛选：0 不筛选，1 图片，2 视频，3 音乐，4 短链接。
WEIBO_CONTAIN_TYPE = 0
# 发布地区：填写“全部”，或填写省/直辖市名称列表。
WEIBO_REGION = ["全部"]

# 最多写入的微博数量；0 表示不限制。
WEIBO_LIMIT_RESULT = 0
# 输出 JSONL 路径；留空时自动生成 data/weibo_<起始日期>_<结束日期>.jsonl。
OUTPUT_FILE = ""
# 是否额外生成 Excel；文件保存到项目根目录，按关键词分别放入 Sheet。
EXPORT_EXCEL = True

# 每次 HTTP 请求之间的随机等待秒数范围。
REQUEST_INTERVAL = (0.5, 1.2)
# 遇到 403/418/429 且服务端没有 Retry-After 时的初始冷却秒数。
COOLDOWN_SECONDS = 30
# 重跑时重新检查最近几小时内已完成的窗口；0 表示不重查。
RECHECK_HOURS = 6
# 单个时间窗口最多翻页数；小一些会更快，但可能漏数据。
MAX_PAGES = 50
# 搜索结果达到此页数时自动细分时间范围，范围 1 到 50。
WEIBO_FURTHER_THRESHOLD = 46
# 仅在主详情缺少属地时发起补查请求。
WEIBO_FETCH_IP = True
IP_REQUEST_TIMEOUT = 5
