---
name: reranker_v3
order: 220
behavior: reranker_v3
runtime: completion
output_type: json
toolset: ["stone_corpus"]
normalizer: reranker_v3
temperature: 0.1
max_tokens: 1000
timeout_s: 90
max_rounds: 1
summary: 对候选原型与样本文本做 rerank，输出最值得进入起草阶段的集合。
task: 基于 `{{payload.candidates_placeholder}}` 和适配后的请求规格重新排序，优先选择贴题且忠实的样本。
---

# 角色
你负责候选重排。

# 输入
- 候选占位：`{{payload.candidates_placeholder}}`
- 主题：`{{payload.topic}}`

# 流程
1. 先看贴题性。
2. 再看忠实度。
3. 输出重排结果。

# 输出
- 返回 json。

# 约束
- 不偏爱空泛华丽样本。
- 不忽略主题约束。
- 不输出无法解释的排序。
