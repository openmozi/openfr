"""
并行工具调用支持。

提供工具的并行执行能力，提升性能。
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable
import logging

logger = logging.getLogger(__name__)


def execute_tools_parallel(
    tool_calls: list[dict[str, Any]],
    get_tool_func: Callable[[str], Any],
    max_workers: int = 5,
    timeout: float = 30.0,
) -> list[dict[str, Any]]:
    """
    并行执行多个工具调用。

    Args:
        tool_calls: 工具调用列表，每个包含 name 和 args
        get_tool_func: 根据名称获取工具的函数
        max_workers: 最大并行数
        timeout: 总超时时间（秒）

    Returns:
        结果列表，每个包含 tool_name, args, result, error
    """
    if not tool_calls:
        return []

    # 单个工具调用直接执行
    if len(tool_calls) == 1:
        tool_call = tool_calls[0]
        tool_name = tool_call.get("name")
        tool_args = tool_call.get("args", {})
        tool = get_tool_func(tool_name)

        if not tool:
            return [{
                "tool_name": tool_name,
                "args": tool_args,
                "result": None,
                "error": f"未找到工具: {tool_name}"
            }]

        try:
            result = tool.invoke(tool_args)
            return [{
                "tool_name": tool_name,
                "args": tool_args,
                "result": result,
                "error": None
            }]
        except Exception as e:
            return [{
                "tool_name": tool_name,
                "args": tool_args,
                "result": None,
                "error": str(e)
            }]

    # 多个工具调用并行执行
    results = []

    def execute_single(tool_call: dict) -> dict:
        """执行单个工具调用"""
        tool_name = tool_call.get("name")
        tool_args = tool_call.get("args", {})
        tool = get_tool_func(tool_name)

        if not tool:
            return {
                "tool_name": tool_name,
                "args": tool_args,
                "result": None,
                "error": f"未找到工具: {tool_name}"
            }

        try:
            result = tool.invoke(tool_args)
            return {
                "tool_name": tool_name,
                "args": tool_args,
                "result": result,
                "error": None
            }
        except Exception as e:
            logger.error(f"工具 {tool_name} 执行失败: {e}")
            return {
                "tool_name": tool_name,
                "args": tool_args,
                "result": None,
                "error": str(e)
            }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_call = {
            executor.submit(execute_single, call): call
            for call in tool_calls
        }

        # 收集结果（保持原始顺序）
        call_to_result = {}
        try:
            for future in as_completed(future_to_call, timeout=timeout):
                call = future_to_call[future]
                try:
                    result = future.result(timeout=1.0)  # 单个任务额外 1 秒超时
                    call_to_result[id(call)] = result
                except Exception as e:
                    logger.error(f"工具执行异常: {e}")
                    call_to_result[id(call)] = {
                        "tool_name": call.get("name"),
                        "args": call.get("args", {}),
                        "result": None,
                        "error": str(e)
                    }
        except TimeoutError:
            # 超时时，收集已完成的结果，未完成的标记为超时
            logger.warning(f"并行工具执行超时 ({timeout}s)")
            for call in tool_calls:
                if id(call) not in call_to_result:
                    call_to_result[id(call)] = {
                        "tool_name": call.get("name"),
                        "args": call.get("args", {}),
                        "result": None,
                        "error": f"执行超时 ({timeout}s)"
                    }

        # 按原始顺序返回结果
        results = [call_to_result.get(id(call), {
            "tool_name": call.get("name"),
            "args": call.get("args", {}),
            "result": None,
            "error": "未知错误"
        }) for call in tool_calls]

    return results


def can_parallelize(tool_calls: list[dict[str, Any]]) -> bool:
    """
    判断工具调用是否可以并行执行。

    简单策略：如果是相同类型的查询工具（如多次 get_stock_realtime），
    或者是完全独立的工具，则可以并行。

    Args:
        tool_calls: 工具调用列表

    Returns:
        是否可以并行
    """
    if len(tool_calls) <= 1:
        return False

    # 保护性黑名单：这些工具内部可能触发 libmini_racer/py_mini_racer（V8）相关逻辑，
    # 在多线程并行下可能直接导致进程崩溃（无法用 try/except 捕获）。
    # 发现新增不稳定工具时优先加到这里。
    unsafe_tools = {
        "get_industry_boards",
        "get_industry_board_detail",
        "get_concept_boards",
        "get_concept_stocks",
        "get_index_realtime",
        "get_index_history",
    }

    # 查询类工具（只读，无副作用）可并行
    read_only_tools = {
        "search_stock", "search_stock_any", "search_stock_hk",
        "get_stock_realtime", "get_stock_history", "get_stock_info",
        "get_stock_financials", "get_stock_news", "get_hot_stocks",
        "get_industry_boards", "get_industry_board_detail",
        "get_stock_bid_ask", "get_stock_fund_flow",
        "get_stock_lhb_detail", "get_stock_lhb_dates", "get_stock_lhb_rank",
        "get_stock_yjyg", "get_stock_yjbb", "get_stock_profit_forecast",
        "get_stock_hk_realtime", "get_stock_hk_history",
        "get_fund_list", "get_etf_realtime", "get_etf_history", "get_fund_rank",
        "get_futures_realtime", "get_futures_history", "get_futures_inventory",
        "get_index_realtime", "get_index_history",
        "get_macro_cpi", "get_macro_ppi", "get_macro_pmi", "get_macro_gdp",
        "get_money_supply",
    }

    tool_names = [call.get("name") for call in tool_calls]

    if any(name in unsafe_tools for name in tool_names):
        return False

    # 所有工具都是只读工具
    if all(name in read_only_tools for name in tool_names):
        return True

    return False
