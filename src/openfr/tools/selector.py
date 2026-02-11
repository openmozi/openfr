"""
智能工具选择器。

根据问题类型动态选择相关工具子集，减少 LLM 的选择复杂度。
"""

from typing import List, Callable
import re


class ToolSelector:
    """智能工具选择器"""

    def __init__(self, all_tools: List[Callable]):
        self.all_tools = all_tools
        self.tool_map = {tool.name: tool for tool in all_tools}

        # 工具分类
        self.categories = {
            "stock_search": ["search_stock", "search_stock_any", "search_stock_hk"],
            "stock_realtime": ["get_stock_realtime", "get_stock_bid_ask"],
            "stock_history": ["get_stock_history"],
            "stock_info": ["get_stock_info", "get_stock_financials"],
            "stock_news": ["get_stock_news", "get_hot_stocks"],
            "stock_analysis": ["get_stock_fund_flow", "get_stock_lhb_detail", "get_stock_lhb_dates",
                              "get_stock_lhb_rank", "get_stock_yjyg", "get_stock_yjbb", "get_stock_profit_forecast"],
            "board": ["get_industry_boards", "get_industry_board_detail"],
            "fund": ["get_fund_list", "get_etf_realtime", "get_etf_history", "get_fund_rank"],
            "futures": ["get_futures_realtime", "get_futures_history", "get_futures_inventory"],
            "index": ["get_index_realtime", "get_index_history"],
            "macro": ["get_macro_cpi", "get_macro_ppi", "get_macro_pmi", "get_macro_gdp", "get_money_supply"],
        }

        # 关键词映射
        self.keyword_map = {
            # 股票相关
            "股票|股价|个股|A股|港股": ["stock_search", "stock_realtime", "stock_info"],
            "搜索|查找|找到": ["stock_search"],
            "实时|当前|现在|最新": ["stock_realtime"],
            "历史|走势|K线|涨跌": ["stock_history"],
            "财务|业绩|报表|利润|营收": ["stock_info", "stock_analysis"],
            "新闻|消息|公告": ["stock_news"],
            "龙虎榜|资金流|主力": ["stock_analysis"],
            "行业|板块|概念": ["board"],

            # 基金相关
            "基金|ETF": ["fund"],

            # 期货相关
            "期货|合约": ["futures"],

            # 指数相关
            "指数|大盘|上证|深证|创业板": ["index"],

            # 宏观相关
            "宏观|经济|GDP|CPI|PPI|PMI|货币": ["macro"],
        }

    def select_tools(self, query: str, max_tools: int = 15) -> List[Callable]:
        """
        根据查询选择相关工具。

        Args:
            query: 用户查询
            max_tools: 最大工具数量

        Returns:
            相关工具列表
        """
        # 匹配关键词
        matched_categories = set()

        for pattern, categories in self.keyword_map.items():
            if re.search(pattern, query, re.IGNORECASE):
                matched_categories.update(categories)

        # 如果没有匹配到，返回常用工具
        if not matched_categories:
            matched_categories = {"stock_search", "stock_realtime", "stock_info", "board"}

        # 收集工具
        selected_tools = []
        for category in matched_categories:
            tool_names = self.categories.get(category, [])
            for name in tool_names:
                tool = self.tool_map.get(name)
                if tool and tool not in selected_tools:
                    selected_tools.append(tool)

        # 限制数量
        return selected_tools[:max_tools]

    def get_tool_by_name(self, name: str) -> Callable | None:
        """根据名称获取工具"""
        return self.tool_map.get(name)

    def get_all_tools(self) -> List[Callable]:
        """获取所有工具"""
        return self.all_tools
