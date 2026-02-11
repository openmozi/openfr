"""
Tool registry for managing all available tools.
"""

from typing import Callable

from openfr.tools.stock import (
    get_stock_realtime,
    get_stock_history,
    get_stock_info,
    get_stock_financials,
    search_stock,
    search_stock_any,
    get_stock_news,
    get_hot_stocks,
    get_industry_boards,
    get_industry_board_detail,
)
from openfr.tools.stock_ext import (
    get_stock_bid_ask,
    get_stock_fund_flow,
    get_stock_lhb_detail,
    get_stock_lhb_dates,
    get_stock_lhb_rank,
    get_stock_yjyg,
    get_stock_yjbb,
    get_stock_profit_forecast,
)
from openfr.tools.stock_hk import (
    get_stock_hk_realtime,
    get_stock_hk_history,
    search_stock_hk,
)
from openfr.tools.fund import (
    get_fund_list,
    get_etf_realtime,
    get_etf_history,
    get_fund_rank,
)
from openfr.tools.futures import (
    get_futures_realtime,
    get_futures_history,
    get_futures_inventory,
)
from openfr.tools.index import (
    get_index_realtime,
    get_index_history,
)
from openfr.tools.macro import (
    get_macro_cpi,
    get_macro_ppi,
    get_macro_pmi,
    get_macro_gdp,
    get_money_supply,
)


# Tool categories
STOCK_TOOLS = [
    get_stock_realtime,
    get_stock_history,
    get_stock_info,
    get_stock_financials,
    search_stock,
    search_stock_any,
    get_stock_news,
    get_hot_stocks,
    get_industry_boards,
    get_industry_board_detail,
    # 扩展：五档、资金流、龙虎榜、业绩预告/快报、盈利预测
    get_stock_bid_ask,
    get_stock_fund_flow,
    get_stock_lhb_detail,
    get_stock_lhb_dates,
    get_stock_lhb_rank,
    get_stock_yjyg,
    get_stock_yjbb,
    get_stock_profit_forecast,
]

STOCK_HK_TOOLS = [
    get_stock_hk_realtime,
    get_stock_hk_history,
    search_stock_hk,
]

FUND_TOOLS = [
    get_fund_list,
    get_etf_realtime,
    get_etf_history,
    get_fund_rank,
]

FUTURES_TOOLS = [
    get_futures_realtime,
    get_futures_history,
    get_futures_inventory,
]

INDEX_TOOLS = [
    get_index_realtime,
    get_index_history,
]

MACRO_TOOLS = [
    get_macro_cpi,
    get_macro_ppi,
    get_macro_pmi,
    get_macro_gdp,
    get_money_supply,
]


def get_all_tools(
    include_stock: bool = True,
    include_stock_hk: bool = True,
    include_fund: bool = True,
    include_futures: bool = True,
    include_index: bool = True,
    include_macro: bool = True,
) -> list:
    """
    Get all available tools based on configuration.

    Args:
        include_stock: Include stock data tools (A股)
        include_stock_hk: Include HK stock data tools (港股)
        include_fund: Include fund data tools
        include_futures: Include futures data tools
        include_index: Include index data tools
        include_macro: Include macro data tools

    Returns:
        List of tool functions
    """
    tools = []

    if include_stock:
        tools.extend(STOCK_TOOLS)
    if include_stock_hk:
        tools.extend(STOCK_HK_TOOLS)
    if include_fund:
        tools.extend(FUND_TOOLS)
    if include_futures:
        tools.extend(FUTURES_TOOLS)
    if include_index:
        tools.extend(INDEX_TOOLS)
    if include_macro:
        tools.extend(MACRO_TOOLS)

    return tools


def get_tool_descriptions() -> str:
    """
    Get formatted descriptions of all tools for display.

    Returns:
        Formatted string with tool names and descriptions
    """
    all_tools = get_all_tools()
    descriptions = []

    categories = [
        ("股票数据 (A股)", STOCK_TOOLS),
        ("股票数据 (港股)", STOCK_HK_TOOLS),
        ("基金数据", FUND_TOOLS),
        ("期货数据", FUTURES_TOOLS),
        ("指数数据", INDEX_TOOLS),
        ("宏观数据", MACRO_TOOLS),
    ]

    for category_name, tools in categories:
        descriptions.append(f"\n{category_name}:")
        for tool in tools:
            name = tool.name
            desc = tool.description.split("\n")[0] if tool.description else "无描述"
            descriptions.append(f"  - {name}: {desc}")

    return "\n".join(descriptions)
