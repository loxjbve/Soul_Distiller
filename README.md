# Persona Distiller

本地单用户 Web 应用，用于把与某个人有关的文档集合蒸馏成可审核、可发布、可试聊验证的模仿 skill。

## 运行

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -e .[dev]
uvicorn app.main:app --reload
```

默认访问 `http://127.0.0.1:8000`。

## 功能

- 多人物项目管理
- 文档上传与抽取：`html/json/docx/pdf/txt/md/log`
- 纯文本清洗、分块、可追溯 chunk 映射
- OpenAI 兼容 LLM 配置与模型自动发现
- 可选 embeddings 检索与 BM25 风格降级检索
- 并发多维分析、证据引用、冲突汇总
- Skill 草稿生成、审核编辑、版本发布
- 试聊页面与命中证据追踪
