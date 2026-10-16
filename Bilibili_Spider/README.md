# B站评论爬虫（独立版）

从 AI 素养项目的两阶段评论采样程序整理而来。程序不依赖原项目路径；所需搜索库已随包提供，其余 Python 依赖由 uv 安装。默认采样参数及关键词沿用来源配置；登录 Cookie、匿名化盐、缓存和采样数据不随程序包提供。

## 环境

需要 Python 3.11 及 [uv](https://docs.astral.sh/uv/)。首次运行会由 uv 创建环境并安装依赖。

```bash
uv run python spider.py --check
```

`--check` 会访问 B 站接口进行连通检查。

## 运行

```bash
uv run python spider.py             # 阶段 A 建视频表，再运行阶段 B 抓评论
uv run python spider.py --stage a   # 仅建视频表
uv run python spider.py --stage b   # 从已有视频表续爬评论
```

阶段 A 搜索并生成 `data/{PROJECT}/video_table.csv`；阶段 B 需要有效的登录 Cookie，并会将评论按视频写入同目录的 JSONL 文件。中断后重跑可按进度续爬。API 响应缓存保存在 `api_cache.jsonl`。

当前配置沿用来源代码：评论上限为 1,000,000，结果目录名为 `纯净版视频表构建`。如果用于 AI 素养项目的正式采样，请先按项目当前方案核对这两个值。

## 登录 Cookie

不要把 Cookie 写入代码或配置文件。运行阶段 B 前，在当前终端设置 `BILI_COOKIE`：

```bash
export BILI_COOKIE='从浏览器复制的 B 站 Cookie'
uv run python spider.py --stage b
```

匿名化盐首次运行时生成在本地 `.anon_salt`；需要在另一份副本中保持相同匿名 ID 时，可安全保管并复用该文件。`.anon_salt`、Cookie 环境变量、缓存和采样数据均不属于程序包。

## 输出位置

采样结果写入 `data/{PROJECT}/`，搜索缓存写入 `api_cache.jsonl`。修改关键词、日期范围、每词视频数和评论上限，请编辑 `config.py`。请勿将 Cookie、`.anon_salt`、缓存或采样结果一并分享。
