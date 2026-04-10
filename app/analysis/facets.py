from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FacetDefinition:
    key: str
    label: str
    purpose: str
    search_query: str


FACETS: tuple[FacetDefinition, ...] = (
    FacetDefinition(
        key="personality",
        label="人格特征",
        purpose="Identify stable personality traits, emotional baseline, and self-description patterns.",
        search_query="人格 性格 自我描述 personality traits emotional baseline",
    ),
    FacetDefinition(
        key="language_style",
        label="语言风格",
        purpose="Describe vocabulary, sentence rhythm, tone, politeness, slang, and rhetorical style.",
        search_query="说话 语气 用词 风格 language style tone vocabulary rhetorical habits",
    ),
    FacetDefinition(
        key="values_preferences",
        label="价值观与决策偏好",
        purpose="Extract principles, decision rules, likes/dislikes, and recurring judgments.",
        search_query="价值观 原则 喜好 厌恶 决策 偏好 values principles preferences choices",
    ),
    FacetDefinition(
        key="life_timeline",
        label="人物经历与时间线",
        purpose="Summarize significant events, chronology, work, study, and turning points.",
        search_query="经历 时间线 生平 工作 学习 timeline biography milestones background",
    ),
    FacetDefinition(
        key="relationship_network",
        label="关系网络",
        purpose="Identify frequent people, social roles, address terms, and relationship patterns.",
        search_query="朋友 家人 同事 关系 称呼 relationship network family colleagues close contacts",
    ),
    FacetDefinition(
        key="narrative_boundaries",
        label="自我叙事与禁忌边界",
        purpose="Surface taboos, sensitive topics, self-positioning, and explicit boundaries.",
        search_query="禁忌 边界 敏感 不愿谈 自我定位 taboo boundaries sensitive topics self narrative",
    ),
)
