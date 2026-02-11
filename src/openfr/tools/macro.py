"""
Macroeconomic data tools based on AKShare.
"""

import akshare as ak
import pandas as pd
from langchain_core.tools import tool

from openfr.tools.base import format_dataframe, retry_on_network_error


# 为 AKShare 调用添加重试装饰器（宏观接口偶发断开，静默重试）
@retry_on_network_error(max_retries=3, base_delay=1.0, silent=True)
def _fetch_macro_cpi() -> pd.DataFrame:
    """获取CPI数据（带重试）"""
    return ak.macro_china_cpi()


@retry_on_network_error(max_retries=3, base_delay=1.0, silent=True)
def _fetch_macro_ppi() -> pd.DataFrame:
    """获取PPI数据（带重试）"""
    return ak.macro_china_ppi()


@retry_on_network_error(max_retries=3, base_delay=1.0, silent=True)
def _fetch_macro_pmi() -> pd.DataFrame:
    """获取PMI数据（带重试）"""
    return ak.macro_china_pmi()


@retry_on_network_error(max_retries=3, base_delay=1.0, silent=True)
def _fetch_macro_gdp() -> pd.DataFrame:
    """获取GDP数据（带重试）"""
    return ak.macro_china_gdp()


@retry_on_network_error(max_retries=3, base_delay=1.0, silent=True)
def _fetch_money_supply() -> pd.DataFrame:
    """获取货币供应量数据（带重试）"""
    return ak.macro_china_money_supply()


@tool
def get_macro_cpi() -> str:
    """
    获取中国CPI(居民消费价格指数)数据。

    Returns:
        历史CPI数据，包括同比和环比变化
    """
    try:
        df = _fetch_macro_cpi()

        if df.empty:
            return "暂无CPI数据"

        return f"中国CPI数据:\n\n{format_dataframe(df)}"
    except Exception as e:
        return f"获取CPI数据失败: {str(e)[:200]}"


@tool
def get_macro_ppi() -> str:
    """
    获取中国PPI(工业生产者出厂价格指数)数据。

    Returns:
        历史PPI数据
    """
    try:
        df = _fetch_macro_ppi()

        if df.empty:
            return "暂无PPI数据"

        return f"中国PPI数据:\n\n{format_dataframe(df)}"
    except Exception as e:
        return f"获取PPI数据失败: {str(e)[:200]}"


@tool
def get_macro_pmi() -> str:
    """
    获取中国PMI(采购经理人指数)数据。

    Returns:
        历史PMI数据，包括制造业和非制造业PMI
    """
    try:
        # 制造业PMI
        df = _fetch_macro_pmi()

        if df.empty:
            return "暂无PMI数据"

        return f"中国PMI数据:\n\n{format_dataframe(df)}"
    except Exception as e:
        return f"获取PMI数据失败: {str(e)[:200]}"


@tool
def get_macro_gdp() -> str:
    """
    获取中国GDP数据。

    Returns:
        季度GDP数据，包括GDP总量和增速
    """
    try:
        df = _fetch_macro_gdp()

        if df.empty:
            return "暂无GDP数据"

        return f"中国GDP数据:\n\n{format_dataframe(df)}"
    except Exception as e:
        return f"获取GDP数据失败: {str(e)[:200]}"


@tool
def get_money_supply() -> str:
    """
    获取中国货币供应量数据(M0, M1, M2)。

    Returns:
        货币供应量历史数据
    """
    try:
        df = _fetch_money_supply()

        if df.empty:
            return "暂无货币供应数据"

        return f"中国货币供应量数据:\n\n{format_dataframe(df)}"
    except Exception as e:
        return f"获取货币供应数据失败: {str(e)[:200]}"
