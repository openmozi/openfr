"""
工具描述增强

为工具提供结构化的描述，包括"何时使用"和"何时不使用"。
"""

from typing import Dict


class ToolDescription:
    """
    工具描述类

    提供结构化的工具使用指南。
    """

    def __init__(
        self,
        name: str,
        description: str,
        when_to_use: list[str],
        when_not_to_use: list[str],
        examples: list[str] | None = None,
    ):
        """
        初始化工具描述

        Args:
            name: 工具名称
            description: 简短描述
            when_to_use: 何时使用（使用场景列表）
            when_not_to_use: 何时不使用（避免场景列表）
            examples: 使用示例（可选）
        """
        self.name = name
        self.description = description
        self.when_to_use = when_to_use
        self.when_not_to_use = when_not_to_use
        self.examples = examples or []

    def to_prompt(self) -> str:
        """
        转换为提示词格式

        Returns:
            格式化的工具描述
        """
        sections = [
            f"## {self.name}",
            f"{self.description}",
            "",
            "**何时使用:**",
        ]

        for item in self.when_to_use:
            sections.append(f"- {item}")

        sections.append("")
        sections.append("**何时不使用:**")

        for item in self.when_not_to_use:
            sections.append(f"- {item}")

        if self.examples:
            sections.append("")
            sections.append("**示例:**")
            for example in self.examples:
                sections.append(f"- {example}")

        return "\n".join(sections)


# 工具描述注册表
TOOL_DESCRIPTIONS: Dict[str, ToolDescription] = {}


def register_tool_description(desc: ToolDescription) -> None:
    """
    注册工具描述

    Args:
        desc: 工具描述对象
    """
    TOOL_DESCRIPTIONS[desc.name] = desc


def get_tool_description(tool_name: str) -> ToolDescription | None:
    """
    获取工具描述

    Args:
        tool_name: 工具名称

    Returns:
        工具描述，如果不存在返回 None
    """
    return TOOL_DESCRIPTIONS.get(tool_name)


def get_all_tool_descriptions() -> str:
    """
    获取所有工具描述的格式化文本

    Returns:
        所有工具描述
    """
    if not TOOL_DESCRIPTIONS:
        return "暂无工具描述"

    sections = []
    for desc in TOOL_DESCRIPTIONS.values():
        sections.append(desc.to_prompt())
        sections.append("")

    return "\n".join(sections)


# ==================== 预定义工具描述 ====================

# A股工具描述
register_tool_description(
    ToolDescription(
        name="get_stock_realtime",
        description="获取A股实时行情数据（当前价格、涨跌幅、成交量等）",
        when_to_use=[
            '用户询问"今天股价"、"现在多少钱"、"最新价格"',
            "需要当前的市值、涨跌幅、成交量数据",
            "查询股票的实时行情快照",
        ],
        when_not_to_use=[
            "查询历史数据（使用 get_stock_history）",
            "查询财务报表（使用 get_stock_financial_analysis）",
            "搜索股票代码（使用 search_stock）",
            "港股查询（使用 get_stock_hk_realtime）",
        ],
        examples=[
            '"贵州茅台今天股价" → symbol="600519"',
            '"平安银行最新行情" → symbol="000001"',
            '"查询 000858 的实时数据" → symbol="000858"',
        ],
    )
)

register_tool_description(
    ToolDescription(
        name="get_stock_history",
        description="获取A股历史行情数据（指定时间范围的价格走势）",
        when_to_use=[
            '用户询问"最近一周走势"、"历史数据"、"过去一个月"',
            "需要绘制K线图或趋势分析",
            "计算历史涨跌幅、波动率",
        ],
        when_not_to_use=[
            "查询当前实时价格（使用 get_stock_realtime）",
            "查询财务指标（使用 get_stock_financial_analysis）",
            "查询新闻资讯（使用 get_stock_news）",
        ],
        examples=[
            '"贵州茅台最近一周的走势" → symbol="600519", period="weekly"',
            '"查询 600519 从 20240101 到 20240131 的历史" → start_date="20240101", end_date="20240131"',
            '"平安银行最近一个月股价" → symbol="000001", period="monthly"',
        ],
    )
)

register_tool_description(
    ToolDescription(
        name="search_stock",
        description="搜索A股股票代码（根据公司名称或关键词）",
        when_to_use=[
            "用户提供公司名称但没有股票代码",
            '询问"搜索茅台"、"查找宁德时代"',
            "需要确认准确的股票代码",
        ],
        when_not_to_use=[
            "已经有明确的股票代码（直接调用其他工具）",
            "港股搜索（使用 search_stock_hk）",
            "查询行情数据（先搜索代码，再调用行情工具）",
        ],
        examples=[
            '"搜索茅台" → keyword="茅台"',
            '"查找新能源汽车相关股票" → keyword="新能源汽车"',
            '"平安银行代码是多少" → keyword="平安银行"',
        ],
    )
)

register_tool_description(
    ToolDescription(
        name="get_stock_financial_analysis",
        description="获取A股财务数据和分析指标（ROE、PE、营收、利润等）",
        when_to_use=[
            "查询公司基本面数据（ROE、ROA、毛利率）",
            "需要估值指标（PE、PB、PS）",
            "分析盈利能力、财务健康度",
            '用户询问"财务状况"、"盈利能力"、"估值水平"',
        ],
        when_not_to_use=[
            "查询实时股价（使用 get_stock_realtime）",
            "查询历史价格走势（使用 get_stock_history）",
            "查询新闻（使用 get_stock_news）",
        ],
        examples=[
            '"贵州茅台的ROE是多少" → symbol="600519"',
            '"分析宁德时代的财务状况" → symbol="300750"',
            '"平安银行的估值水平" → symbol="000001"',
        ],
    )
)

register_tool_description(
    ToolDescription(
        name="get_industry_board_detail",
        description="获取行业板块的整体数据（涨跌幅、平均PE/PB、领涨股等）",
        when_to_use=[
            "分析整个行业的表现",
            "需要行业平均估值水平",
            '用户询问"白酒板块"、"半导体行业"、"新能源汽车板块"',
            "进行行业对比或板块轮动分析",
        ],
        when_not_to_use=[
            "查询单只股票（使用个股工具）",
            "查询概念板块成分股（使用 get_concept_stocks）",
            "查询宏观数据（使用宏观工具）",
        ],
        examples=[
            '"白酒行业整体表现如何" → board_name="白酒"',
            '"半导体板块的平均估值" → board_name="半导体"',
            '"新能源汽车行业分析" → board_name="新能源汽车"',
        ],
    )
)

# 港股工具描述
register_tool_description(
    ToolDescription(
        name="get_stock_hk_realtime",
        description="获取港股实时行情数据",
        when_to_use=[
            "查询港股代码（如 00700、09988）的实时价格",
            '用户明确提到"港股"、"HK"',
            "查询在港股上市的公司",
        ],
        when_not_to_use=[
            "A股查询（使用 get_stock_realtime）",
            "美股查询（暂不支持）",
            "历史数据（使用 get_stock_hk_history）",
        ],
        examples=[
            '"腾讯控股今天股价" → symbol="00700"',
            '"港股 09988 最新行情" → symbol="09988"',
            '"阿里巴巴港股价格" → symbol="09988"',
        ],
    )
)

register_tool_description(
    ToolDescription(
        name="search_stock_hk",
        description="搜索港股股票代码",
        when_to_use=[
            "用户提供港股公司名称但没有代码",
            "需要确认港股代码",
            '询问"搜索港股腾讯"、"理想汽车港股代码"',
        ],
        when_not_to_use=[
            "A股搜索（使用 search_stock）",
            "已有港股代码（直接调用其他港股工具）",
        ],
        examples=[
            '"搜索港股腾讯" → keyword="腾讯"',
            '"理想汽车港股代码" → keyword="理想汽车"',
        ],
    )
)

# 指数和板块工具
register_tool_description(
    ToolDescription(
        name="get_index_realtime",
        description="获取指数实时数据（上证指数、创业板指等）",
        when_to_use=[
            "查询大盘指数表现",
            '用户询问"上证指数"、"创业板指"、"沪深300"',
            "分析市场整体走势",
        ],
        when_not_to_use=[
            "查询个股（使用个股工具）",
            "查询行业板块（使用板块工具）",
        ],
        examples=[
            '"上证指数今天走势" → index_code="000001"',
            '"创业板指最新数据" → index_code="399006"',
        ],
    )
)

# 宏观经济工具
register_tool_description(
    ToolDescription(
        name="get_macro_china_cpi",
        description="获取中国CPI（消费者价格指数）数据",
        when_to_use=[
            "分析通货膨胀情况",
            '用户询问"CPI"、"物价水平"、"通胀"',
            "宏观经济分析",
        ],
        when_not_to_use=[
            "查询GDP（使用 get_macro_china_gdp）",
            "查询PMI（使用 get_macro_china_pmi）",
            "查询个股或行业（使用对应工具）",
        ],
        examples=[
            '"最新的CPI数据" → 返回最近的CPI数据',
            '"中国通胀情况" → 查询CPI走势',
        ],
    )
)
