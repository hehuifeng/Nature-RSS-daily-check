# RSS Daily Pipeline

一个自动化的科研论文订阅与翻译工具 🚀  
每天定时检查目标期刊 RSS，抓取最新文章的核心信息（标题、摘要、DOI 等），调用大模型 API 翻译成中文，并输出为 Markdown 报告，方便在公众号或其他平台分享。

---

## 🌟 背景

科研工作者每天都面临着海量的新论文。为了节省筛选与翻译的时间，本项目实现了一个自动化 Pipeline，帮助快速追踪目标期刊的前沿进展。  
我个人在搭建时选择了大模型 API 翻译（目前使用 **qwen-flash** 模型，速度快、价格低，非常适合批量学术翻译），从而实现高效又低成本的自动摘要生成。

---

## ✨ 功能特色

- **自动抓取**：支持 RSS / Atom / RDF 格式，定期检查新论文。  
- **核心信息提取**：期刊、标题、类型、发布日期、DOI、文章链接、摘要。  
- **智能翻译**：中英文双语摘要（支持 OpenAI/通义千问等兼容接口，目前默认 **qwen-flash**）。  
- **去重机制**：SQLite 数据库存储，保证每天不会重复推送旧文章。  
- **Markdown 输出**：每个期刊单独生成一份日报，文件名带有期刊名与时间戳，避免覆盖。  
- **轻量易用**：配置文件集中管理，无需复杂命令行参数。  

---

## 🛠️ Pipeline 设计

1. **订阅 Feed**：从 config.json 中读取 feed 列表。  
2. **检查新文章**：利用 RSS 的 `GUID/DOI/Link` 去重。  
3. **抓取网页**：解析 HTML / JSON-LD / Meta 标签，提取核心元信息。  
4. **调用大模型翻译**：标题与摘要由大模型（如 qwen-flash）翻译为中文。  
5. **输出 Markdown**：生成日报文件（包含开场白 + 每篇文章详情）。  
6. **状态存储**：SQLite (`rss_state.db`) 保存已读记录与 Feed 缓存头。  

---

## 📦 安装依赖

```bash
git clone https://github.com/yourname/rss-daily-pipeline.git
cd rss-daily-pipeline
pip install -r requirements.txt
```

依赖：
- requests  
- beautifulsoup4  
- lxml  

---

## ⚙️ 配置方法

所有参数通过 `config.json` 配置，无需命令行传参。  

示例：

```json
{
  "feeds": [
    "https://www.nature.com/ncomms.rss",
    "https://www.nature.com/nature.rss"
  ],
  "out_dir": "./reports",
  "db": "./rss_state.db",
  "translator": "openai",
  "http_timeout": 25,
  "sleep_between_fetches": 0.5,
  "sleep_between_translations": 0.4,

  "openai": {
    "api_key": "YOUR_API_KEY",
    "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
    "model": "qwen-flash"
  }
}
```

说明：
- `feeds`：要订阅的 RSS 地址（可多个）。  
- `out_dir`：Markdown 报告保存目录。  
- `db`：SQLite 数据库路径。  
- `translator`：翻译后端（`none` 或 `openai`）。  
- `openai.api_key`：大模型 API Key。  
- `openai.base_url`：兼容 OpenAI API 的端点（阿里云百炼用 DashScope）。  
- `openai.model`：模型名称（推荐 `qwen-flash`）。  

---

## ▶️ 使用方法

运行：

```bash
python rss_daily_pipeline.py
```

输出：  
- 每个期刊一份 Markdown 文件，存放在 `./reports` 下。  
- 文件名格式：`rss_report_{期刊名}_{YYYYMMDD_HHMMSS}.md`  
- 报告开头会自动生成一段开场白，解释项目动机与设计，并邀请读者反馈。  

---

## 📖 输出示例

```markdown
# Nature Communications RSS Report — 2025-08-28 — 2025-08-30

大家好！

作为科研工作者/科研爱好者，我常常被浩如烟海的文献淹没……
（开场白略）

**1. Title of the paper (EN)**

**标题（CN）**：论文标题（中文翻译）

**发表期刊**：Nature Communications

**类型**：Article

**发表日期**：2025-08-28

**DOI**：10.1038/s41467-025-XXXXX

**文章链接**：https://www.nature.com/articles/xxxx

**Abstract (EN)**：

英文摘要……

**Abstract (CN)**：

中文摘要……

---
```

---

## 📅 定时任务（可选）

在 Linux / macOS 上，可以通过 `cron` 定时运行，例如每天早上 10 点自动生成报告：

```bash
0 10 * * * /usr/bin/python3 /path/to/rss-daily-pipeline/rss_daily_pipeline.py >> /path/to/pipeline.log 2>&1
```

---

## 🔮 TODO / Roadmap

- [ ] 支持更多翻译后端（DeepL、Claude、Gemini）。  
- [ ] 支持按学科标签自动筛选论文。  
- [ ] 报告输出为 HTML / PDF，方便直接分享。  
- [ ] 公众号自动推送集成。  

---

## 🤝 贡献

欢迎提出 issue 或 PR，尤其是：  
- 想要添加哪些期刊？  
- 希望报告里再包含哪些信息？  
- 有没有更好的呈现方式？

---

## 📜 License

MIT License
