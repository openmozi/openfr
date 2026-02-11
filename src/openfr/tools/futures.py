"""
Futures data tools based on AKShare.
"""

import akshare as ak
import pandas as pd
from langchain_core.tools import tool

from openfr.tools.base import format_dataframe, retry_on_network_error


# 为 AKShare 调用添加重试装饰器（期货接口易断开，加重试与静默）
@retry_on_network_error(max_retries=3, base_delay=1.2, silent=True)
def _fetch_futures_spot() -> pd.DataFrame:
    """获取期货实时行情（带重试）"""
    return ak.futures_zh_spot()


@retry_on_network_error(max_retries=3, base_delay=1.0, silent=True)
def _fetch_futures_history(symbol: str) -> pd.DataFrame:
    """获取期货历史行情 - 新浪（带重试）"""
    return ak.futures_zh_daily_sina(symbol=symbol)


@retry_on_network_error(max_retries=3, base_delay=1.2, silent=True)
def _fetch_futures_inventory(symbol: str) -> pd.DataFrame:
    """获取期货库存数据 - 东财（带重试）"""
    return ak.futures_inventory_em(symbol=symbol)


@tool
def get_futures_realtime(symbol: str = "") -> str:
    """
    获取期货实时行情。

    Args:
        symbol: 期货品种代码，如 "螺纹钢", "沪铜", "原油"，留空则返回主要品种

    Returns:
        期货实时行情数据
    """
    try:
        # Get all futures realtime data
        df = _fetch_futures_spot()

        if df.empty:
            return "暂无期货行情数据"

        if symbol:
            # Filter by symbol
            mask = df["symbol"].str.contains(symbol, case=False, na=False) | df["name"].str.contains(
                symbol, na=False
            )
            df = df[mask]
            if df.empty:
                return f"未找到期货品种 {symbol}"

        return f"期货实时行情:\n\n{format_dataframe(df.head(30))}"
    except Exception as e:
        return f"获取期货行情失败: {str(e)[:200]}"


@tool
def get_futures_history(symbol: str, start_date: str = "", end_date: str = "") -> str:
    """
    获取期货历史行情（主力连续合约）。

    Args:
        symbol: 期货品种代码，如 "RB0" (螺纹钢主力), "CU0" (沪铜主力)
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD

    Returns:
        期货历史K线数据
    """
    try:
        df = _fetch_futures_history(symbol=symbol)

        if df.empty:
            return f"未找到期货 {symbol} 的历史数据"

        # Filter by date if provided
        if "date" in df.columns:
            df["date"] = df["date"].astype(str)
            if start_date:
                start = start_date.replace("-", "")
                df = df[df["date"] >= start]
            if end_date:
                end = end_date.replace("-", "")
                df = df[df["date"] <= end]

        return f"期货 {symbol} 历史行情:\n\n{format_dataframe(df)}"
    except Exception as e:
        return f"获取期货历史数据失败: {str(e)[:200]}"


@tool
def get_futures_inventory(symbol: str) -> str:
    """
    获取期货库存数据。

    Args:
        symbol: 期货品种，如 "沪铜", "螺纹钢", "铁矿石"

    Returns:
        期货库存数据
    """
    try:
        df = _fetch_futures_inventory(symbol=symbol)

        if df.empty:
            return f"未找到 {symbol} 的库存数据"

        return f"{symbol} 库存数据:\n\n{format_dataframe(df)}"
    except Exception as e:
        return f"获取期货库存失败: {str(e)[:200]}"
