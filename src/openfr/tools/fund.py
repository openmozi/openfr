"""
Fund data tools based on AKShare.
"""

import time
import akshare as ak
import pandas as pd
from langchain_core.tools import tool

from openfr.tools.base import format_dataframe, retry_on_network_error

# ETF 实时行情缓存（避免频繁拉取大表）
ETF_SPOT_CACHE_TTL = 600.0  # 秒
_ETF_SPOT_CACHE_DF: pd.DataFrame | None = None
_ETF_SPOT_CACHE_TS: float | None = None


def _try_multiple_sources(fetch_functions: list, delay: float = 1.0) -> pd.DataFrame:
    """多数据源依次尝试，返回第一个非空结果。"""
    for i, fetch_func in enumerate(fetch_functions):
        try:
            if i > 0:
                time.sleep(delay)
            result = fetch_func()
            if result is not None and not result.empty:
                return result
        except Exception:
            continue
    return pd.DataFrame()


# 为 AKShare 调用添加重试装饰器
@retry_on_network_error(max_retries=2, base_delay=1.0, silent=True)
def _fetch_fund_etf_spot_em() -> pd.DataFrame:
    """获取ETF实时行情 - 东方财富"""
    return ak.fund_etf_spot_em()


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_fund_etf_spot_ths() -> pd.DataFrame:
    """获取ETF实时行情 - 同花顺备用，列名统一为东财风格"""
    df = ak.fund_etf_spot_ths(date="")
    if df.empty:
        return df
    rename = {
        "基金代码": "代码",
        "基金名称": "名称",
        "增长率": "涨跌幅",
        "当前-单位净值": "最新价",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    if "涨跌幅" in df.columns:
        df["涨跌幅"] = pd.to_numeric(df["涨跌幅"], errors="coerce")
    return df


def _fetch_fund_etf_spot() -> pd.DataFrame:
    """ETF实时行情（东财 -> 同花顺备用）"""
    return _try_multiple_sources(
        [_fetch_fund_etf_spot_em, _fetch_fund_etf_spot_ths], delay=1.0
    )


def _get_fund_etf_spot_cached() -> pd.DataFrame:
    """
    带进程内缓存的 ETF 实时行情。

    - 在一个进程会话内多次调用 ETF 相关工具时，避免重复拉取全市场 ETF 大表
    - 数据源全部失败时不写入缓存，方便后续重试
    """
    global _ETF_SPOT_CACHE_DF, _ETF_SPOT_CACHE_TS

    now = time.time()
    if _ETF_SPOT_CACHE_DF is not None and _ETF_SPOT_CACHE_TS is not None:
        if now - _ETF_SPOT_CACHE_TS < ETF_SPOT_CACHE_TTL:
            return _ETF_SPOT_CACHE_DF

    df = _fetch_fund_etf_spot()
    if df is not None and not df.empty:
        _ETF_SPOT_CACHE_DF = df
        _ETF_SPOT_CACHE_TS = now
    return df


@retry_on_network_error(max_retries=3, base_delay=1.0)
def _fetch_fund_lof_spot() -> pd.DataFrame:
    """获取LOF实时行情（带重试）"""
    return ak.fund_lof_spot_em()


@retry_on_network_error(max_retries=3, base_delay=1.0)
def _fetch_fund_name() -> pd.DataFrame:
    """获取基金名称列表（带重试）"""
    return ak.fund_name_em()


def _sina_etf_symbol(symbol: str) -> str:
    """东财代码转新浪 ETF 代码：510300 -> sh510300，159xxx -> sz159xxx"""
    s = str(symbol).strip()
    if s.startswith(("sh", "sz")):
        return s
    if s.startswith("5") or s.startswith("51"):
        return "sh" + s
    return "sz" + s


@retry_on_network_error(max_retries=2, base_delay=1.0, silent=True)
def _fetch_fund_etf_history_em(**kwargs) -> pd.DataFrame:
    """获取ETF历史行情 - 东方财富"""
    return ak.fund_etf_hist_em(**kwargs)


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_fund_etf_history_sina(symbol: str) -> pd.DataFrame:
    """获取ETF历史行情 - 新浪备用（列名与东财略有不同）"""
    sina_code = _sina_etf_symbol(symbol)
    df = ak.fund_etf_hist_sina(symbol=sina_code)
    if df.empty:
        return df
    # 新浪列 date, open, high, low, close, volume；统一为东财风格便于展示
    col_map = {"date": "日期", "open": "开盘", "high": "最高", "low": "最低", "close": "收盘", "volume": "成交量"}
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    return df


def _fetch_fund_etf_history(**kwargs) -> pd.DataFrame:
    """ETF历史行情（东财 -> 新浪备用）"""
    symbol = kwargs.get("symbol", "")
    df = _fetch_fund_etf_history_em(**kwargs)
    if not df.empty:
        return df
    time.sleep(1.0)
    df = _fetch_fund_etf_history_sina(symbol=symbol)
    if df.empty:
        return df
    # 新浪无 start_date/end_date 参数，按日期过滤
    date_col = "日期" if "日期" in df.columns else "date"
    if date_col not in df.columns:
        return df
    start_date = kwargs.get("start_date", "")
    end_date = kwargs.get("end_date", "")
    if start_date or end_date:
        try:
            dr = pd.to_datetime(df[date_col], errors="coerce")
            mask = pd.Series(True, index=df.index)
            if start_date:
                t0 = pd.Timestamp(start_date[:4] + "-" + start_date[4:6] + "-" + start_date[6:8])
                mask = mask & (dr >= t0)
            if end_date:
                t1 = pd.Timestamp(end_date[:4] + "-" + end_date[4:6] + "-" + end_date[6:8])
                mask = mask & (dr <= t1)
            df = df.loc[mask]
        except Exception:
            pass
    return df


@retry_on_network_error(max_retries=3, base_delay=1.0)
def _fetch_fund_rank(symbol: str) -> pd.DataFrame:
    """获取基金排行（带重试）"""
    return ak.fund_open_fund_rank_em(symbol=symbol)



@tool
def get_fund_list(fund_type: str = "all") -> str:
    """
    获取基金列表。

    Args:
        fund_type: 基金类型，可选 "all"(全部), "etf", "lof"

    Returns:
        基金列表
    """
    try:
        if fund_type == "etf":
            df = _get_fund_etf_spot_cached()
        elif fund_type == "lof":
            df = _fetch_fund_lof_spot()
        else:
            df = _fetch_fund_name()

        if df.empty:
            return "暂无基金数据"

        return f"基金列表 ({fund_type}):\n\n{format_dataframe(df.head(30))}"
    except Exception as e:
        return f"获取基金列表失败: {str(e)[:200]}"


@tool
def get_etf_realtime(symbol: str = "") -> str:
    """
    获取ETF实时行情。

    Args:
        symbol: ETF代码，如 "510300"，留空则返回所有ETF

    Returns:
        ETF实时行情数据
    """
    try:
        df = _get_fund_etf_spot_cached()

        if df.empty:
            return "暂无ETF数据"

        if symbol:
            df = df[df["代码"].str.contains(symbol)]
            if df.empty:
                return f"未找到ETF代码 {symbol}"

        cols = ["代码", "名称", "最新价", "涨跌幅", "成交额"]
        available_cols = [c for c in cols if c in df.columns]

        return f"ETF实时行情:\n\n{format_dataframe(df[available_cols].head(20))}"
    except Exception as e:
        return f"获取ETF行情失败: {str(e)[:200]}"


@tool
def get_etf_history(symbol: str, start_date: str = "", end_date: str = "") -> str:
    """
    获取ETF历史行情。

    Args:
        symbol: ETF代码，如 "510300"
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD

    Returns:
        ETF历史K线数据
    """
    try:
        kwargs = {"symbol": symbol, "period": "daily", "adjust": "qfq"}

        if start_date:
            kwargs["start_date"] = start_date.replace("-", "")
        if end_date:
            kwargs["end_date"] = end_date.replace("-", "")

        df = _fetch_fund_etf_history(**kwargs)

        if df.empty:
            return f"未找到ETF {symbol} 的历史数据"

        return f"ETF {symbol} 历史行情:\n\n{format_dataframe(df)}"
    except Exception as e:
        return f"获取ETF历史数据失败: {str(e)[:200]}"


@tool
def get_fund_rank(fund_type: str = "全部", sort_by: str = "近1年") -> str:
    """
    获取基金业绩排行。

    Args:
        fund_type: 基金类型，可选 "全部", "股票型", "混合型", "债券型", "指数型", "QDII"
        sort_by: 排序依据，可选 "近1周", "近1月", "近3月", "近6月", "近1年", "近2年", "近3年"

    Returns:
        基金业绩排行榜
    """
    try:
        df = _fetch_fund_rank(symbol=fund_type)

        if df.empty:
            return "暂无基金排行数据"

        # Sort by the specified column if exists
        if sort_by in df.columns:
            df = df.sort_values(sort_by, ascending=False)

        # Select key columns
        cols = ["基金代码", "基金简称", sort_by, "手续费"]
        available_cols = [c for c in cols if c in df.columns]

        return f"基金业绩排行 ({fund_type}, 按{sort_by}):\n\n{format_dataframe(df[available_cols].head(20))}"
    except Exception as e:
        return f"获取基金排行失败: {str(e)[:200]}"
