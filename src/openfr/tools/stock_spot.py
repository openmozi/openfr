"""
A 股行情与列表：实时行情、代码列表、历史、个股详情、新闻、热门。
"""

import time
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd

from openfr.tools.base import retry_on_network_error
from openfr.tools.constants import DEFAULT_MAX_RETRIES, STOCK_LIST_CACHE_TTL
from openfr.tools.cache import cached
from openfr.tools.stock_common import (
    try_multiple_sources,
    try_multiple_sources_parallel,
    is_parallel_sources_enabled,
)


# 个股详情接口易卡死，设短超时并静默重试
STOCK_INFO_TIMEOUT = 6


@retry_on_network_error(max_retries=DEFAULT_MAX_RETRIES, base_delay=1.0, silent=True)
def _fetch_stock_spot_em() -> pd.DataFrame:
    """获取A股实时行情数据 - 东方财富接口"""
    return ak.stock_zh_a_spot_em()


@retry_on_network_error(max_retries=DEFAULT_MAX_RETRIES, base_delay=0.8, silent=True)
def _fetch_stock_spot_sina() -> pd.DataFrame:
    """获取A股实时行情数据 - 新浪接口"""
    return ak.stock_zh_a_spot()


@cached(ttl=STOCK_LIST_CACHE_TTL)
def _fetch_stock_spot() -> pd.DataFrame:
    """获取A股实时行情数据（默认串行；可选并行尝试多个数据源）"""
    sources = [_fetch_stock_spot_em, _fetch_stock_spot_sina]
    if is_parallel_sources_enabled():
        df = try_multiple_sources_parallel(sources, timeout_per_source=25.0)
        if not df.empty:
            return df
    return try_multiple_sources(sources, delay=1.0)


@retry_on_network_error(max_retries=DEFAULT_MAX_RETRIES, base_delay=0.8, silent=True)
@cached(ttl=STOCK_LIST_CACHE_TTL)
def _fetch_stock_list_code_name() -> pd.DataFrame:
    """A股代码+名称列表（交易所数据），用于行情接口全挂时的搜索备用"""
    df = ak.stock_info_a_code_name()
    if df.empty:
        return df
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    rename_map = {
        "证券代码": "代码",
        "证券简称": "名称",
        "A股代码": "代码",
        "A股简称": "名称",
    }
    out = out.rename(columns={k: v for k, v in rename_map.items() if k in out.columns})
    if "代码" not in out.columns:
        for c in list(out.columns):
            if "代码" in c:
                out = out.rename(columns={c: "代码"})
                break
    if "名称" not in out.columns:
        for c in list(out.columns):
            if ("简称" in c) or ("名称" in c):
                out = out.rename(columns={c: "名称"})
                break
    if ("代码" not in out.columns or "名称" not in out.columns) and out.shape[1] >= 2:
        c0, c1 = out.columns[0], out.columns[1]
        if "代码" not in out.columns:
            out = out.rename(columns={c0: "代码"})
        if "名称" not in out.columns and c1 != "代码":
            out = out.rename(columns={c1: "名称"})
    if "代码" in out.columns:
        out["代码"] = (
            out["代码"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(6)
        )
    if "名称" in out.columns:
        out["名称"] = out["名称"].astype(str)
    if "代码" in out.columns and "名称" in out.columns:
        return out[["代码", "名称"]]
    return out


_STOCK_LIST_CACHE_TTL_SECONDS = 6 * 60 * 60
_STOCK_LIST_CACHE_TS = 0.0
_STOCK_LIST_CACHE_DF: pd.DataFrame | None = None


def _get_stock_list_code_name_cached() -> pd.DataFrame:
    """带 TTL 的 A 股代码名称列表缓存。"""
    global _STOCK_LIST_CACHE_TS, _STOCK_LIST_CACHE_DF
    now = time.time()
    if (
        _STOCK_LIST_CACHE_DF is not None
        and not _STOCK_LIST_CACHE_DF.empty
        and ("代码" in _STOCK_LIST_CACHE_DF.columns and "名称" in _STOCK_LIST_CACHE_DF.columns)
        and (now - _STOCK_LIST_CACHE_TS) < _STOCK_LIST_CACHE_TTL_SECONDS
    ):
        return _STOCK_LIST_CACHE_DF
    df = _fetch_stock_list_code_name()
    if df is not None and not df.empty and ("代码" in df.columns and "名称" in df.columns):
        _STOCK_LIST_CACHE_DF = df
        _STOCK_LIST_CACHE_TS = now
    return df


@retry_on_network_error(max_retries=3, base_delay=1.5)
def _fetch_stock_history(**kwargs) -> pd.DataFrame:
    """获取A股历史行情数据（带重试）"""
    return ak.stock_zh_a_hist(**kwargs)


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_stock_info(symbol: str) -> pd.DataFrame:
    """获取个股基本信息 - 东财接口，带超时防卡死"""
    return ak.stock_individual_info_em(symbol=symbol, timeout=STOCK_INFO_TIMEOUT)


@retry_on_network_error(max_retries=3, base_delay=1.0)
def _fetch_stock_news(symbol: str) -> pd.DataFrame:
    """获取个股新闻（带重试）"""
    return ak.stock_news_em(symbol=symbol)


@retry_on_network_error(max_retries=2, base_delay=1.0)
def _fetch_hot_stocks_em() -> pd.DataFrame:
    """获取热门股票 - 东方财富接口"""
    return ak.stock_hot_rank_em()


def _fetch_hot_stocks() -> pd.DataFrame:
    """获取热门股票（智能切换）。注意：接口经常不可用，失败时返回空。"""
    return try_multiple_sources([_fetch_hot_stocks_em], delay=1.5)


def _realtime_from_spot_row(symbol: str, row: pd.Series) -> str:
    """从全市场行情的一行组装实时行情文案（个股接口失败时的降级）。兼容东财/新浪列名。"""

    def _col(*keys):
        for k in keys:
            if k in row.index and pd.notna(row.get(k)):
                return row.get(k)
        return "N/A"

    output = f"股票 {symbol} 实时行情（来自行情列表）:\n"
    output += f"  股票代码: {_col('代码', 'code', 'symbol') or symbol}\n"
    output += f"  股票简称: {_col('名称', 'name')}\n"
    output += f"  最新价: {_col('最新价', '最新', 'close', 'price')}\n"
    output += f"  涨跌幅: {_col('涨跌幅', 'pct_chg', 'change')}\n"
    output += f"  今开: {_col('今开', '开盘', '开盘价', 'open')}\n"
    output += f"  昨收: {_col('昨收', '昨收价', 'pre_close', 'close')}\n"
    output += f"  最高: {_col('最高', '最高价', 'high')}\n"
    output += f"  最低: {_col('最低', '最低价', 'low')}\n"
    output += f"  成交量: {_col('成交量', 'volume')}\n"
    output += f"  成交额: {_col('成交额', 'amount')}\n"
    output += f"  总市值: {_col('总市值', '总市值')}\n"
    output += f"  流通市值: {_col('流通市值', '流通市值')}\n"
    return output
