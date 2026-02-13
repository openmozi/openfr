"""
A 股工具公共逻辑：多数据源尝试、代码规范化、子工具调用等。
"""

from typing import Callable
import os
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError, as_completed

import pandas as pd

# 是否启用“多数据源并行尝试”（同花顺相关接口在各函数内强制串行以避免 libmini_racer 崩溃）
_ENABLE_PARALLEL_SOURCES = os.getenv("OPENFR_ENABLE_PARALLEL_SOURCES", "true").lower() == "true"


def try_multiple_sources(fetch_functions: list, delay: float = 1.0) -> pd.DataFrame:
    """
    尝试多个数据源接口，返回第一个成功的结果（串行，按优先级）。
    """
    import time
    last_error = None
    for i, fetch_func in enumerate(fetch_functions):
        try:
            if i > 0:
                time.sleep(delay)
            result = fetch_func()
            if result is not None and isinstance(result, pd.DataFrame) and not result.empty:
                return result
        except Exception as e:
            last_error = e
            continue
    return pd.DataFrame()


def try_multiple_sources_parallel(
    fetch_functions: list,
    timeout_per_source: float = 20.0,
    max_workers: int | None = None,
) -> pd.DataFrame:
    """
    并行尝试多个数据源，返回第一个成功且非空的结果。
    """
    if not fetch_functions:
        return pd.DataFrame()
    max_workers = max_workers or min(len(fetch_functions), 6)

    def fetch_one(fetch_func: Callable[[], pd.DataFrame]) -> pd.DataFrame | None:
        try:
            result = fetch_func()
            if result is not None and isinstance(result, pd.DataFrame) and not result.empty:
                return result
        except Exception:
            pass
        return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_func = {executor.submit(fetch_one, f): f for f in fetch_functions}
        try:
            for future in as_completed(future_to_func, timeout=timeout_per_source * len(fetch_functions)):
                result = future.result(timeout=1.0)
                if result is not None and not result.empty:
                    return result
        except (FutureTimeoutError, TimeoutError):
            pass
    return pd.DataFrame()


def is_parallel_sources_enabled() -> bool:
    """是否启用多数据源并行尝试。"""
    return _ENABLE_PARALLEL_SOURCES


def _norm_code(s: str) -> str:
    """将代码规范为 6 位数字便于比较。"""
    s = str(s).strip()
    s = re.sub(r"\D", "", s)
    return s.zfill(6)[-6:] if len(s) >= 6 else s.zfill(6)


def _to_em_symbol(symbol: str) -> str:
    """6 位代码转东方财富格式：600519 -> sh600519, 000001 -> sz000001"""
    s = re.sub(r"\D", "", str(symbol).strip())[-6:].zfill(6)
    if s.startswith("6") or s.startswith("5") or s.startswith("9"):
        return f"sh{s}"
    return f"sz{s}"


def _to_em_symbol_dot(symbol: str) -> str:
    """6 位代码转东财带点格式：600519 -> 600519.SH, 000001 -> 000001.SZ"""
    s = re.sub(r"\D", "", str(symbol).strip())[-6:].zfill(6)
    if s.startswith("6") or s.startswith("5") or s.startswith("9"):
        return f"{s}.SH"
    return f"{s}.SZ"


def _call_ak_with_symbol_or_stock(func, symbol: str):
    """部分 akshare 版本用 symbol，部分用 stock，兼容两种参数名。"""
    for kw in ("symbol", "stock"):
        try:
            return func(**{kw: symbol})
        except TypeError:
            continue
    raise TypeError("财务接口需要 symbol 或 stock 参数")


def _invoke_sub_tool(tool_obj, args: dict) -> str:
    """
    在工具内部安全调用其他工具或普通函数。
    兼容 LangChain StructuredTool（.invoke）与普通可调用函数。
    """
    if hasattr(tool_obj, "invoke"):
        try:
            out = tool_obj.invoke(args)
            return out if isinstance(out, str) else str(out)
        except TypeError as e:
            if "not callable" in str(e).lower():
                return (
                    f"子工具调用方式异常（{type(tool_obj).__name__} 请使用 .invoke），"
                    "请稍后重试或改用 search_stock / search_stock_hk。"
                )
            raise
        except Exception as e:
            raise
    if callable(tool_obj):
        return tool_obj(**args)
    raise TypeError(f"不支持的子工具类型: {type(tool_obj)}")
