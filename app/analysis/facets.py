from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FacetDefinition:
    key: str
    label: str
    purpose: str
    search_query: str


@dataclass(frozen=True, slots=True)
class FacetPromptProfile:
    focus: str
    bullet_labels: tuple[str, ...]
    relevance_terms: tuple[str, ...]


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


DEFAULT_FACET_PROMPT_PROFILE = FacetPromptProfile(
    focus="只抽取当前维度直接支撑的行为证据，不补写其他维度的人设总卡。",
    bullet_labels=("核心观察", "关键证据", "稳定模式", "例外情况"),
    relevance_terms=("边界", "判断", "表达", "模式", "行为"),
)


FACET_PROMPT_PROFILES: dict[str, FacetPromptProfile] = {
    "personality": FacetPromptProfile(
        focus="稳定自我定位、情绪基线、默认姿态、长期人格张力，以及在哪些情境下会露出失真或脆弱面。",
        bullet_labels=("稳定自我定位", "情绪基线", "默认姿态", "人格张力", "失真触发点"),
        relevance_terms=("自我定位", "情绪", "气质", "人格", "克制", "冲动", "冷静", "焦虑", "底色", "脆弱"),
    ),
    "language_style": FacetPromptProfile(
        focus="高频词、口头禅、句式、断句、节奏、语气切换、排版习惯、幽默/反击方式，以及回复起手式。",
        bullet_labels=("高频词与口头禅", "句式与断句", "语气切换", "回复节奏", "幽默/反击", "排版习惯"),
        relevance_terms=("口头禅", "语气", "句式", "断句", "排版", "节奏", "黑话", "短句", "回复", "反问", "措辞"),
    ),
    "values_preferences": FacetPromptProfile(
        focus="原则、底线、偏好、厌恶、取舍逻辑、风险判断，以及可重复调用的决策捷径。",
        bullet_labels=("核心原则", "底线", "偏好/厌恶", "取舍逻辑", "决策捷径", "高压触发点"),
        relevance_terms=("原则", "底线", "偏好", "讨厌", "喜欢", "取舍", "风险", "代价", "判断", "决策"),
    ),
    "life_timeline": FacetPromptProfile(
        focus="关键阶段、转折事件、长期背景、时间锚点，以及这些节点如何改变他的判断方式、表达方式或风险偏好。",
        bullet_labels=("关键阶段", "转折事件", "长期背景", "时间锚点", "残留影响", "阶段变化"),
        relevance_terms=("以前", "后来", "当时", "一直", "那年", "经历", "背景", "转折", "工作", "学习", "成长"),
    ),
    "relationship_network": FacetPromptProfile(
        focus="关键人物、圈层、称呼方式、影响方向、自己人/外人区分，以及关系结构中的站位。",
        bullet_labels=("关键人物/圈层", "称呼方式", "影响方向", "自己人/外人", "关系站位", "互动密度"),
        relevance_terms=("朋友", "家人", "同事", "群友", "老师", "称呼", "关系", "熟人", "外人", "自己人", "影响"),
    ),
    "narrative_boundaries": FacetPromptProfile(
        focus="敏感禁区、回避触发点、收口方式、自保或重构话题的手法、可说范围，以及诚实边界。",
        bullet_labels=("敏感禁区", "回避触发点", "收口方式", "自保/重构", "可说范围", "诚实边界"),
        relevance_terms=("禁区", "敏感", "回避", "收口", "自保", "重构", "缩写", "昵称", "避开", "不展开", "边界", "诚实"),
    ),
    "physical_anchor": FacetPromptProfile(
        focus="现实处境、资源约束、作息、工作方式、消费感、压力来源，以及围绕生存条件形成的补偿策略。",
        bullet_labels=("现实处境", "资源约束", "作息/工作", "压力来源", "消费/物质感", "生存策略"),
        relevance_terms=("现实", "工作", "通勤", "钱", "消费", "房租", "作息", "压力", "资源", "阶层", "收入", "生存"),
    ),
    "social_niche": FacetPromptProfile(
        focus="群内站位、权力感知、资格判断、对权威的态度、话语权策略，以及对新人/外人的处理方式。",
        bullet_labels=("群内站位", "权力感知", "资格判断", "对权威态度", "话语权策略", "对新人处理"),
        relevance_terms=("群里", "权力", "权威", "新人", "地位", "资格", "站位", "话语权", "主导", "生态位"),
    ),
    "interpersonal_mechanics": FacetPromptProfile(
        focus="同理方式、帮助方式、冲突反应、防御触发点、反击手法、亲疏切换，以及关系中的交换规则。",
        bullet_labels=("同理方式", "帮助方式", "冲突反应", "防御触发点", "反击手法", "亲疏切换"),
        relevance_terms=("同理", "帮助", "求助", "冲突", "争执", "防御", "反击", "亲疏", "冒犯", "情绪", "安慰"),
    ),
    "subculture_refuge": FacetPromptProfile(
        focus="亚文化母体、圈层黑话、审美来源、舒适话题、精神避难所，以及借此完成身份识别的方式。",
        bullet_labels=("亚文化母体", "圈层黑话", "审美来源", "舒适话题", "精神避难所", "身份识别"),
        relevance_terms=("亚文化", "黑话", "圈层", "审美", "动漫", "游戏", "音乐", "梗", "避难所", "舒适区", "母体"),
    ),
}


def get_facet_prompt_profile(facet_key: str) -> FacetPromptProfile:
    return FACET_PROMPT_PROFILES.get(facet_key, DEFAULT_FACET_PROMPT_PROFILE)
