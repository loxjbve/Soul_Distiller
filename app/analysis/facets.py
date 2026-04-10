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
        purpose="识别稳定气质、情绪基线、反复出现的自我定位，以及用户的默认能量状态。",
        search_query="人格 性格 情绪基线 自我定位 自我描述 气质 情绪状态",
    ),
    FacetDefinition(
        key="language_style",
        label="语言风格",
        purpose="描述黑话、句子节奏、语气切换、排版习惯、反复出现的口头禅，以及回复节奏。",
        search_query="语言风格 口头禅 语气 句式 黑话 排版 表情包 回复节奏",
    ),
    FacetDefinition(
        key="values_preferences",
        label="价值观与决策偏好",
        purpose="提取原则、好恶、判断规则、底线，以及反复出现的决策启发式。",
        search_query="价值观 原则 偏好 底线 讨厌 喜欢 决策 判断 取舍",
    ),
    FacetDefinition(
        key="life_timeline",
        label="人物经历与时间线",
        purpose="总结人生事件、工作或学习背景、时间线索、转折点，以及重要经历锚点。",
        search_query="经历 时间线 生平 工作 学习 转折点 背景 成长轨迹",
    ),
    FacetDefinition(
        key="relationship_network",
        label="关系网络",
        purpose="识别高频出现的人物、社交角色、称呼方式、依附模式，以及关系聚类。",
        search_query="朋友 家人 同事 群友 称呼 关系 亲疏 关系网 社交圈",
    ),
    FacetDefinition(
        key="narrative_boundaries",
        label="自我叙事与禁区边界",
        purpose="识别敏感议题、禁区边界、防御性自我叙事，以及用户会回避或重构的话题。",
        search_query="禁区 边界 敏感 不愿说 防御 自我叙事 回避话题",
    ),
    FacetDefinition(
        key="physical_anchor",
        label="现实锚点与生存状态",
        purpose="推断现实压力、阶层质感、作息、工作方式、物质约束，以及补偿性行为。",
        search_query="现实 生存状态 作息 压力 阶层 工作 通勤 消费 现实锚点",
    ),
    FacetDefinition(
        key="social_niche",
        label="群体生态位与权力感知",
        purpose="刻画用户在群体中的角色、对权力的敏感度、地位姿态、对权威的态度，以及对新人的处理方式。",
        search_query="群体生态位 权力 权威 新人 地位 角色 群聊 站位 话语权",
    ),
    FacetDefinition(
        key="interpersonal_mechanics",
        label="待人接物与冲突机制",
        purpose="分析同理方式、帮助方式、冲突反应、防御触发点，以及情绪交换模式。",
        search_query="待人接物 冲突 防御 同理心 求助 争执 阴阳怪气 情绪",
    ),
    FacetDefinition(
        key="subculture_refuge",
        label="亚文化偏好与精神避难所",
        purpose="识别亚文化标记、审美偏好、舒适话题、逃避性兴趣，以及精神避难所模式。",
        search_query="亚文化 偏好 精神避难所 美学 游戏 音乐 动漫 黑话 舒适区",
    ),
)
