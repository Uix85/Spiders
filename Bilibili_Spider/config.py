# -*- coding: utf-8 -*-
import os
"""B站评论爬虫配置；运行前检查关键词、时间范围、预算和并发参数。"""

# --- 整体缓存（API 响应级，跨 run 复用；stage_a 搜索页 / 视频详情 / UP主粉丝数）---
# 文件直接放根目录，JSONL 逐行追加（只缓存成功非空响应）。
CACHE_ENABLED = True
CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "api_cache.jsonl")

# --- 词表：顺序 = 优先级（精确词在前，宽词兜底已置于末尾） ---
KEYWORDS = [
    "DeepSeek",
    "ChatGPT",
    "GPT",
    "Claude",
    "Google Gemini",
    "Kimi",
    "豆包AI",
    "文心一言",
    "通义千问",
    "智谱",
    "讯飞星火",
    "腾讯元宝",
    "腾讯混元",
    "GLM",
    "MiniMax",
    "海螺AI",
    "Midjourney",
    "Sora",
    "Stable Diffusion",
    "ComfyUI",
    "LoRA",
    "可灵",
    "即梦",
    "AI绘画",
    "AI视频",
    "AI配音",
    "AI音乐",
    "数字人",
    "Agent",
    "智能体",
    "Coze",
    "扣子",
    "Manus",
    "MCP",
    "RAG",
    "Prompt",
    "提示词",
    "Cursor",
    "Copilot",
    "AI编程",
    "vibe coding",
    "大模型微调",
    "AI办公",
    "AI搜索",
    "AI换脸",
    "AI诈骗",
    "AI幻觉",
    "算法歧视",
    "AI伦理",
    "人机协作",
    "AIGC",
    "AI写作",
    "AI论文",
    "AI机器人",
    "大模型",
    "大语言模型",
    "LLM",
    "多模态",
    "生成式AI",
    "AGI",
    "机器学习",
    "深度学习",
    "神经网络",
    "Transformer",
    "OpenCV",
    "目标检测",
    "图像分割",
    "语义分割",
    "具身智能",
    "claude code",
    "codex",
    "langchain",
    "langgraph",
    "nlp",
    "openai",
    "pytorch",
    "yolo",
    "人工智能",
    "具身智能机器人",
    "微调",
    "智能驾驶",
    "自然语言处理",
    "计算机视觉",
    "语音识别",
    "部署",
]

# --- 采样参数 ---
START_DAY = "2023-01-01"   # 时间窗起点（含）
END_DAY = "2026-08-01"               # 时间窗终点（空 = 今天）
N_PER_KEYWORD = 200        # 阶段A：每词综合排序取 n 条视频（20条/页 → 10 页）
N_COMMENTS = 250           # 阶段B：每视频一级评论（最热 mode=0）取前 N 条
                         # 2026-09-01 用户拍板：前 250 条一级 + 无限二级（FETCH_SUB=True 已是全量）
                         # 产量是否够 150 万待干净实测确认（堆叠历史均值 762 条/视频，250 截断后预计仍 ≥450）
FETCH_SUB = True           # 是否爬二级评论（rcount>0 的一级评论全量拉取）
TOTAL_LIMIT = 1000000    # 当前来源代码的评论总量上限
SPAN_DAYS = 365            # 搜索接口时间窗单次最大跨度（按段拼接，防接口超限）

# --- 运行 ---
COOKIE = os.environ.get("BILI_COOKIE", "").strip()
PROJECT = "纯净版视频表构建"     # 数据落盘目录：data/{PROJECT}/（评论爬虫）。2026-09-01 用户定名
ANON_SALT = os.environ.get("ANON_SALT", "").strip()  # 可选；默认保存在本地 .anon_salt
REQUEST_INTERVAL = 1.5     # 请求间隔（秒）。2026-09-02 晚：8/6 线程+1.0s 长跑触发 412（13+次）卡死；回 1.5s 疏散
                           # 3s=20 次/分钟；游客模式阈值更紧，宁慢勿崩
RETRY_DELAYS = [10, 30, 60]  # -412/-352/软风控 指数退避（秒），最多 3 次。
                            # 加长：搜索接口限流时短重试只是加深限流（游客 IP 恢复窗口 60s+），长退避给 IP 呼吸

# --- 并发（多线程版，参考 tag_keywords 实测值；REQUEST_INTERVAL 变为全局速率闸：总 QPS 恒定 ≈ 1/interval，±30% 抖动）---
SEARCH_CONCURRENCY = 1   # 阶段A bilibili-api 搜索并发词数（tag_keywords 实测 10 无风控、20 连续 10min 软风控）
STAGE_B_WORKERS = 4       # 阶段B 评论线程数（2026-09-02：8/6 并发长时间跑触发 412 卡死；恢复 4 线程稳定节奏）
