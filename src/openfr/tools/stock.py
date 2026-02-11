"""
Stock data tools based on AKShare.
"""

from typing import Callable
import os

import akshare as ak
import pandas as pd
from langchain_core.tools import tool
import requests
import re
import time
from datetime import datetime, timedelta
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError, as_completed

from openfr.tools.base import format_dataframe, validate_stock_code, validate_date, retry_on_network_error
from openfr.tools.stock_hk import search_stock_hk
from openfr.tools.constants import (
    CONCEPT_STOCKS_TOTAL_TIMEOUT,
    DEFAULT_MAX_RETRIES,
    EM_MAX_RETRIES,
    STOCK_LIST_CACHE_TTL,
    MAX_SEARCH_RESULTS,
    MAX_BOARD_RESULTS,
)
from openfr.tools.cache import cached

# 是否启用“多数据源并行尝试”
# 默认开启，但对同花顺相关数据源会在各函数内强制串行以避免 libmini_racer 崩溃。
_ENABLE_PARALLEL_SOURCES = os.getenv("OPENFR_ENABLE_PARALLEL_SOURCES", "true").lower() == "true"

def try_multiple_sources(fetch_functions: list, delay: float = 1.0) -> pd.DataFrame:
    """
    尝试多个数据源接口，返回第一个成功的结果（串行，按优先级）。

    Args:
        fetch_functions: 接口函数列表，按优先级排序
        delay: 每次尝试之间的延迟（秒）

    Returns:
        成功获取的 DataFrame，如果全部失败则返回空 DataFrame
    """
    last_error = None

    for i, fetch_func in enumerate(fetch_functions):
        try:
            if i > 0:
                time.sleep(delay)  # 延迟以避免频繁请求

            result = fetch_func()
            if result is not None and isinstance(result, pd.DataFrame) and not result.empty:
                return result
        except Exception as e:
            last_error = e
            continue

    # 所有接口都失败，返回空 DataFrame
    return pd.DataFrame()


def try_multiple_sources_parallel(
    fetch_functions: list,
    timeout_per_source: float = 20.0,
    max_workers: int | None = None,
) -> pd.DataFrame:
    """
    并行尝试多个数据源，返回第一个成功且非空的结果，避免串行等待。

    Args:
        fetch_functions: 接口函数列表
        timeout_per_source: 每个源的超时（秒）
        max_workers: 最大并行数，默认与源数量一致

    Returns:
        成功获取的 DataFrame，全部失败则返回空 DataFrame
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


# 为 AKShare 调用添加重试装饰器（东方财富易 RemoteDisconnected，加大重试与间隔）
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
    """
    获取A股实时行情数据（默认串行；可选并行尝试多个数据源）
    """
    sources = [
        _fetch_stock_spot_em,
        _fetch_stock_spot_sina,
    ]
    if _ENABLE_PARALLEL_SOURCES:
        df = try_multiple_sources_parallel(sources, timeout_per_source=25.0)
        if not df.empty:
            return df
    return try_multiple_sources(sources, delay=1.0)


@retry_on_network_error(max_retries=DEFAULT_MAX_RETRIES, base_delay=0.8, silent=True)
@cached(ttl=STOCK_LIST_CACHE_TTL)
def _fetch_stock_list_code_name() -> pd.DataFrame:
    """A股代码+名称列表（交易所数据），用于行情接口全挂时的搜索备用，无最新价/涨跌幅"""
    df = ak.stock_info_a_code_name()
    if df.empty:
        return df
    # 统一列名便于与 spot 一致（不同源可能带空格或使用 A股代码/A股简称）
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    rename_map = {
        "证券代码": "代码",
        "证券简称": "名称",
        "A股代码": "代码",
        "A股简称": "名称",
    }
    out = out.rename(columns={k: v for k, v in rename_map.items() if k in out.columns})
    # 兜底：更宽松的列名推断（避免不同环境列名略有差异导致无法搜索）
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
    # 再兜底：若只有两列且仍未命中，按顺序强制映射
    if ("代码" not in out.columns or "名称" not in out.columns) and out.shape[1] >= 2:
        c0, c1 = out.columns[0], out.columns[1]
        if "代码" not in out.columns:
            out = out.rename(columns={c0: "代码"})
        if "名称" not in out.columns and c1 != "代码":
            out = out.rename(columns={c1: "名称"})
    if "代码" in out.columns:
        out["代码"] = (
            out["代码"]
            .astype(str)
            .str.replace(r"\D", "", regex=True)
            .str.zfill(6)
        )
    if "名称" in out.columns:
        out["名称"] = out["名称"].astype(str)
    # 尽量只返回需要的两列，避免下游误用
    if "代码" in out.columns and "名称" in out.columns:
        return out[["代码", "名称"]]
    return out


# 搜索用的代码名称列表缓存（避免每次搜索都全量拉行情/列表导致很慢）
_STOCK_LIST_CACHE_TTL_SECONDS = 6 * 60 * 60  # 6 小时
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
    if (
        df is not None
        and not df.empty
        and ("代码" in df.columns and "名称" in df.columns)
    ):
        _STOCK_LIST_CACHE_DF = df
        _STOCK_LIST_CACHE_TS = now
    return df


@retry_on_network_error(max_retries=3, base_delay=1.5)
def _fetch_stock_history(**kwargs) -> pd.DataFrame:
    """获取A股历史行情数据（带重试）"""
    return ak.stock_zh_a_hist(**kwargs)


# 个股详情接口易卡死，设短超时并静默重试
STOCK_INFO_TIMEOUT = 6


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
    """
    获取热门股票（智能切换）

    注意：热门股票接口经常不可用，如果失败会返回空数据
    """
    sources = [
        _fetch_hot_stocks_em,
    ]

    return try_multiple_sources(sources, delay=1.5)


@retry_on_network_error(max_retries=3, base_delay=1.2, silent=True)
def _fetch_industry_boards_em() -> pd.DataFrame:
    """获取行业板块 - 东方财富接口（易 RemoteDisconnected，多一次重试）"""
    return ak.stock_board_industry_name_em()


@retry_on_network_error(max_retries=2, base_delay=1.0, silent=True)
def _fetch_industry_boards_ths() -> pd.DataFrame:
    """获取行业板块 - 同花顺 summary，列名统一为东方财富风格便于展示"""
    df = ak.stock_board_industry_summary_ths()
    if df is None or df.empty:
        return pd.DataFrame()
    # 统一列名：同花顺 板块 -> 板块名称，领涨股 -> 领涨股票
    rename = {"板块": "板块名称", "领涨股": "领涨股票"}
    if "领涨股-涨跌幅" in df.columns and "领涨股票-涨跌幅" not in df.columns:
        rename["领涨股-涨跌幅"] = "领涨股票-涨跌幅"
    if "领涨股-最新价" in df.columns and "领涨股票-最新价" not in df.columns:
        rename["领涨股-最新价"] = "领涨股票-最新价"
    df = df.rename(columns=rename)
    return df


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_industry_boards_name_ths() -> pd.DataFrame:
    """获取行业板块 - 同花顺仅名称列表（无涨跌幅），作为最后备用"""
    df = ak.stock_board_industry_name_ths()
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={"name": "板块名称", "code": "代码"})
    if "涨跌幅" not in df.columns:
        df["涨跌幅"] = float("nan")
    return df


def _fetch_industry_boards() -> pd.DataFrame:
    """
    获取行业板块（默认串行三重备用；可选并行尝试多个数据源）
    """
    sources = [
        _fetch_industry_boards_em,
        _fetch_industry_boards_ths,
        _fetch_industry_boards_name_ths,
    ]
    # 强制串行：同花顺相关接口在多线程下可能触发 libmini_racer 崩溃
    return try_multiple_sources(sources, delay=1.0)


@retry_on_network_error(max_retries=2, base_delay=1.0, silent=True)
def _fetch_concept_boards_em() -> pd.DataFrame:
    """获取概念板块 - 东方财富接口（静默重试）"""
    return ak.stock_board_concept_name_em()


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_concept_boards_ths() -> pd.DataFrame:
    """获取概念板块 - 同花顺备用；仅名称+代码，无涨跌幅时填 NaN 便于统一展示"""
    df = ak.stock_board_concept_name_ths()
    if df.empty:
        return df
    # 列名统一：name -> 板块名称, code -> 代码；同花顺无涨跌幅，补一列便于 sort 不报错
    df = df.rename(columns={"name": "板块名称", "code": "代码"})
    if "涨跌幅" not in df.columns:
        df["涨跌幅"] = float("nan")
    return df


def _fetch_concept_boards() -> pd.DataFrame:
    """
    获取概念板块（东方财富 -> 同花顺备用）
    """
    sources = [
        _fetch_concept_boards_em,
        _fetch_concept_boards_ths,
    ]
    return try_multiple_sources(sources, delay=1.0)



def _realtime_from_spot_row(symbol: str, row: pd.Series) -> str:
    """从全市场行情的一行组装实时行情文案（个股接口失败时的降级）。兼容东财/新浪列名。"""
    def _col(*keys):
        for k in keys:
            if k in row.index and pd.notna(row.get(k)):
                return row.get(k)
        return "N/A"
    code_col = next((c for c in ("代码", "code", "symbol") if c in row.index), None)
    name_col = next((c for c in ("名称", "name") if c in row.index), None)
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


@tool
def get_stock_realtime(symbol: str) -> str:
    """
    获取A股实时行情数据。

    Args:
        symbol: 股票代码，如 "000001" (平安银行) 或 "600519" (贵州茅台)

    Returns:
        股票的实时行情信息，包括最新价、涨跌幅、成交量等
    """
    try:
        symbol = validate_stock_code(symbol)

        # 优先个股信息接口；失败时降级为全市场行情列表按代码筛选
        try:
            df = _fetch_stock_info(symbol)
            if not df.empty:
                info_dict = {}
                for _, row in df.iterrows():
                    info_dict[row['item']] = row['value']
                output = f"股票 {symbol} 实时行情:\n"
                output += f"  股票代码: {info_dict.get('股票代码', symbol)}\n"
                output += f"  股票简称: {info_dict.get('股票简称', 'N/A')}\n"
                output += f"  最新价: {info_dict.get('最新', 'N/A')}\n"
                output += f"  涨跌幅: {info_dict.get('涨跌幅', 'N/A')}\n"
                output += f"  今开: {info_dict.get('今开', 'N/A')}\n"
                output += f"  昨收: {info_dict.get('昨收', 'N/A')}\n"
                output += f"  最高: {info_dict.get('最高', 'N/A')}\n"
                output += f"  最低: {info_dict.get('最低', 'N/A')}\n"
                output += f"  成交量: {info_dict.get('成交量', 'N/A')}\n"
                output += f"  成交额: {info_dict.get('成交额', 'N/A')}\n"
                output += f"  总市值: {info_dict.get('总市值', 'N/A')}\n"
                output += f"  流通市值: {info_dict.get('流通市值', 'N/A')}\n"
                return output
        except Exception:
            pass

        # 降级：全市场行情列表（东财→新浪）按代码查，统一用 _norm_code 匹配
        def _find_in_spot(df: pd.DataFrame) -> pd.Series | None:
            if df is None or df.empty:
                return None
            code_col = next((c for c in ("代码", "code", "symbol") if c in df.columns), df.columns[0] if len(df.columns) else None)
            if code_col is None:
                return None
            target = _norm_code(symbol)
            code_ser = df[code_col].astype(str).apply(lambda x: _norm_code(x))
            mask = code_ser == target
            if not mask.any():
                return None
            return df.loc[mask].iloc[0]

        spot_df = _fetch_stock_spot()
        row = _find_in_spot(spot_df)
        if row is not None:
            return _realtime_from_spot_row(symbol, row)
        # 东财常断连时单独试新浪行情
        try:
            sina_df = _fetch_stock_spot_sina()
            row = _find_in_spot(sina_df)
            if row is not None:
                return _realtime_from_spot_row(symbol, row)
        except Exception:
            pass
        # 兜底：用最近交易日日线当「最新行情」（东财/新浪 spot 均不可用时）
        try:
            end_d = datetime.now().strftime("%Y%m%d")
            start_d = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
            hist = _fetch_stock_history(symbol=symbol, period="daily", start_date=start_d, end_date=end_d)
            if hist is not None and not hist.empty:
                last = hist.iloc[-1]
                out = f"股票 {symbol} 实时行情（来自最近交易日）:\n"
                out += f"  股票代码: {symbol}\n"
                out += f"  股票简称: （见 get_stock_info）\n"
                out += f"  日期: {last.get('日期', 'N/A')}\n"
                out += f"  最新价(收盘): {last.get('收盘', 'N/A')}\n"
                out += f"  涨跌幅: {last.get('涨跌幅', 'N/A')}\n"
                out += f"  今开: {last.get('开盘', 'N/A')}\n"
                out += f"  昨收: {last.get('昨收', hist.iloc[-2].get('收盘', 'N/A') if len(hist) >= 2 else 'N/A')}\n"
                out += f"  最高/最低: {last.get('最高', 'N/A')} / {last.get('最低', 'N/A')}\n"
                out += f"  成交量: {last.get('成交量', 'N/A')}\n"
                out += f"  成交额: {last.get('成交额', 'N/A')}\n"
                out += "  （数据来自日线，非实时；交易时间请以交易所为准）\n"
                return out
        except Exception:
            pass
        return f"未找到股票代码 {symbol} 的数据"
    except Exception as e:
        return f"获取实时行情失败: {str(e)[:200]}"


@tool
def get_stock_history(
    symbol: str,
    start_date: str = "",
    end_date: str = "",
    period: str = "daily",
    adjust: str = "qfq",
) -> str:
    """
    获取A股历史行情数据。

    Args:
        symbol: 股票代码，如 "000001"
        start_date: 开始日期，格式 YYYYMMDD，如 "20230101"
        end_date: 结束日期，格式 YYYYMMDD，如 "20231231"
        period: 周期，可选 "daily"(日), "weekly"(周), "monthly"(月)
        adjust: 复权类型，"qfq"(前复权), "hfq"(后复权), ""(不复权)

    Returns:
        历史K线数据
    """
    try:
        symbol = validate_stock_code(symbol)

        kwargs = {
            "symbol": symbol,
            "period": period,
            "adjust": adjust,
        }

        if start_date:
            kwargs["start_date"] = validate_date(start_date)
        if end_date:
            kwargs["end_date"] = validate_date(end_date)

        df = _fetch_stock_history(**kwargs)

        if df.empty:
            return f"未找到股票 {symbol} 的历史数据"

        return f"股票 {symbol} 历史行情 ({period}):\n\n{format_dataframe(df)}"
    except Exception as e:
        return f"获取历史行情失败: {str(e)[:200]}"


@tool
def get_stock_info(symbol: str) -> str:
    """
    获取个股基本信息。

    Args:
        symbol: 股票代码，如 "000001"

    Returns:
        股票的基本信息，包括公司名称、行业、市值等
    """
    try:
        symbol = validate_stock_code(symbol)

        # 先试全市场行情（东财/新浪），按代码取一行，通常比个股详情接口更稳定
        spot_df = _fetch_stock_spot()
        fallback_row = None
        if not spot_df.empty and "代码" in spot_df.columns:
            code_str = spot_df["代码"].astype(str)
            mask = (
                (code_str == symbol)
                | code_str.str.endswith("." + symbol)
                | (code_str.str.replace(r"\D", "", regex=True) == symbol)
            )
            stock_data = spot_df.loc[mask]
            if not stock_data.empty:
                fallback_row = stock_data.iloc[0]

        # 再试东财个股详情（公司名、行业等更全）；失败则用上面行情行
        try:
            df = _fetch_stock_info(symbol)
            if not df.empty:
                output = f"股票 {symbol} 基本信息:\n"
                for _, row in df.iterrows():
                    output += f"  {row['item']}: {row['value']}\n"
                return output
        except Exception:
            pass

        if fallback_row is not None:
            row = fallback_row
            output = f"股票 {symbol} 基本信息（来自行情列表）:\n"
            for col in ["代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "总市值", "流通市值", "今开", "昨收", "最高", "最低"]:
                if col in row.index and pd.notna(row.get(col)) and str(row.get(col)).strip() != "":
                    output += f"  {col}: {row[col]}\n"
            return output

        return f"未找到股票 {symbol} 的基本信息"
    except Exception as e:
        return f"获取股票信息失败: {str(e)[:200]}"


def _call_ak_with_symbol_or_stock(func, symbol: str):
    """部分 akshare 版本用 symbol，部分用 stock，兼容两种参数名。"""
    for kw in ("symbol", "stock"):
        try:
            return func(**{kw: symbol})
        except TypeError:
            continue
    raise TypeError("财务接口需要 symbol 或 stock 参数")


def _to_em_symbol(symbol: str) -> str:
    """6 位代码转东方财富格式：600519 -> sh600519, 000001 -> sz000001"""
    s = re.sub(r"\D", "", str(symbol).strip())[-6:].zfill(6)
    if s.startswith("6") or s.startswith("5") or s.startswith("9"):
        return f"sh{s}"
    return f"sz{s}"


def _to_em_symbol_dot(symbol: str) -> str:
    """6 位代码转东财带点格式（部分接口要求）：600519 -> 600519.SH, 000001 -> 000001.SZ"""
    s = re.sub(r"\D", "", str(symbol).strip())[-6:].zfill(6)
    if s.startswith("6") or s.startswith("5") or s.startswith("9"):
        return f"{s}.SH"
    return f"{s}.SZ"


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_stock_financial_analysis_indicator(symbol: str) -> pd.DataFrame | None:
    """
    获取 A股财务分析指标原始数据。
    兼容：接口名 _em 后缀、参数 symbol/stock；尝试 6 位码、sh/sz 前缀、600519.SH 带点格式；空表时尝试 stock_financial_abstract。
    """
    for sym in (symbol, _to_em_symbol_dot(symbol), _to_em_symbol(symbol)):
        funcs = []
        if hasattr(ak, "stock_financial_analysis_indicator"):
            funcs.append(ak.stock_financial_analysis_indicator)
        if hasattr(ak, "stock_financial_analysis_indicator_em"):
            funcs.append(ak.stock_financial_analysis_indicator_em)
        for f in funcs:
            try:
                df = _call_ak_with_symbol_or_stock(f, sym)
                if df is not None and not df.empty:
                    return df
            except Exception:
                continue
        if hasattr(ak, "stock_financial_abstract"):
            try:
                ab = _call_ak_with_symbol_or_stock(ak.stock_financial_abstract, sym)
                if ab is not None and not ab.empty:
                    return ab
            except Exception:
                pass
    return None


def _norm_code(s: str) -> str:
    """将代码规范为 6 位数字便于比较。"""
    s = str(s).strip()
    s = re.sub(r"\D", "", s)
    return s.zfill(6)[-6:] if len(s) >= 6 else s.zfill(6)


# 东财财务分析接口 stock_financial_analysis_indicator_em 返回英文字段名（参见 AKShare 数据字典-财务报表-主要指标-东方财富）
_EM_FINANCE_ROW_MAP = {
    "roe": ["ROEJQ", "ROEKCJQ", "ROE_AVG"],  # 净资产收益率(当季/扣非/平均)，单位%
    "rev_g": ["TOTALOPERATEREVETZ", "YYSRTB"],  # 营业总收入同比增长，单位%
    "prof_g": ["PARENTNETPROFITTZ", "JLRTB"],   # 归属净利润同比增长，单位%
}


def _parse_em_finance_row(row: pd.Series) -> tuple[object, object, object]:
    """从东财财务接口的一行（英文字段）解析 ROE、营收同比、净利润同比。"""
    roe, rev_g, prof_g = None, None, None
    for key in _EM_FINANCE_ROW_MAP["roe"]:
        if key in row.index:
            v = row.get(key)
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                roe = v
                break
    for key in _EM_FINANCE_ROW_MAP["rev_g"]:
        if key in row.index:
            v = row.get(key)
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                rev_g = v
                break
    for key in _EM_FINANCE_ROW_MAP["prof_g"]:
        if key in row.index:
            v = row.get(key)
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                prof_g = v
                break
    return roe, rev_g, prof_g


@retry_on_network_error(max_retries=1, base_delay=0.6, silent=True)
def _fetch_roe_revg_profg_fallback(symbol: str) -> tuple[object, object, object]:
    """ROE/营收增速/利润增速 主数据源无时，从新浪摘要与东财同行比较接口补数。"""
    roe, rev_g, prof_g = None, None, None
    # 1) 新浪 stock_financial_abstract：列 选项、指标、报告期(如20241231)，从指标行取最近一期
    try:
        for sym in (symbol, _to_em_symbol(symbol)):
            ab = _call_ak_with_symbol_or_stock(ak.stock_financial_abstract, sym)
            if ab is None or ab.empty or "指标" not in ab.columns:
                continue
            # 报告期列：选项、指标 外的列，取最近一期（列名为 20241231 等）
            period_cols = [c for c in ab.columns if c not in ("选项", "指标")]
            if not period_cols:
                continue
            latest_col = sorted(period_cols, reverse=True)[0]
            for _, r in ab.iterrows():
                ind = str(r.get("指标", ""))
                val = r.get(latest_col)
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    continue
                if roe is None and ("净资产收益率" in ind or "ROE" in ind.upper()):
                    roe = val
                if rev_g is None and ("营业收入" in ind and ("同比" in ind or "增长" in ind)):
                    rev_g = val
                if prof_g is None and ("净利润" in ind and ("同比" in ind or "增长" in ind)):
                    prof_g = val
            if roe is not None or rev_g is not None or prof_g is not None:
                return roe, rev_g, prof_g
    except Exception:
        pass
    # 2) 东财 杜邦比较 -> ROE；成长性比较 -> 营收/净利润增长率
    em_sym = _to_em_symbol(symbol).upper()  # sh600519 -> SH600519
    for func_name, col_roe, col_rev, col_prof in [
        ("stock_zh_dupont_comparison_em", "ROE-24A", None, None),
        ("stock_zh_growth_comparison_em", None, "营业收入增长率-24A", "净利润增长率-24A"),
    ]:
        try:
            func = getattr(ak, func_name, None)
            if func is None:
                continue
            df = func(symbol=em_sym)
            if df is None or df.empty:
                continue
            row = df.iloc[0]
            if col_roe and roe is None and col_roe in row.index:
                v = row.get(col_roe)
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    roe = v
            if col_rev and rev_g is None and col_rev in row.index:
                v = row.get(col_rev)
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    rev_g = v
            if col_prof and prof_g is None and col_prof in row.index:
                v = row.get(col_prof)
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    prof_g = v
        except Exception:
            continue
    return roe, rev_g, prof_g


def _get_pe_pb_from_spot(symbol: str) -> tuple[str, str]:
    """从全市场行情中取单只股票的市盈率、市净率（东方财富/新浪行情）。"""
    def _v(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "N/A"
        return str(v)
    target = _norm_code(symbol)
    try:
        spot_df = _fetch_stock_spot()
        row = None
        if not spot_df.empty:
            code_col = next((c for c in ("代码", "code", "symbol", "Symbol") if c in spot_df.columns), spot_df.columns[0] if len(spot_df.columns) else None)
            if code_col is not None:
                target = _norm_code(symbol)
                mask = spot_df[code_col].astype(str).apply(lambda x: _norm_code(x) == target)
                if mask.any():
                    row = spot_df.loc[mask].iloc[0]
        if row is None:
            try:
                sina_df = _fetch_stock_spot_sina()
                if not sina_df.empty:
                    code_col_s = next((c for c in ("代码", "code", "symbol") if c in sina_df.columns), sina_df.columns[0] if len(sina_df.columns) else None)
                    if code_col_s is not None:
                        target = _norm_code(symbol)
                        mask = sina_df[code_col_s].astype(str).apply(lambda x: _norm_code(x) == target)
                        if mask.any():
                            row = sina_df.loc[mask].iloc[0]
            except Exception:
                pass
        if row is None:
            # 无行情行时优先用 akshare 其它接口直接取 PE/PB，最后才用 EPS/BPS 估算
            pe2, pb2 = _get_pe_pb_from_lg(symbol)
            if pe2 != "N/A" or pb2 != "N/A":
                return pe2, pb2
            pe2, pb2 = _get_pe_pb_from_stock_info(symbol)
            if pe2 != "N/A" or pb2 != "N/A":
                return pe2, pb2
            pe2, pb2 = _get_pe_pb_from_eps_bps(symbol, None)
            if pe2 != "N/A" or pb2 != "N/A":
                return pe2, pb2
            return "N/A", "N/A"
        # 有行情行，检查 PE/PB 列
        pe_col = next(
            (
                c
                for c in row.index
                if "市盈" in str(c)
                or ("pe" in str(c).lower() and "peg" not in str(c).lower())
                or str(c).strip() in ("PE", "pe_ttm", "市盈率-动态", "动态市盈率")
            ),
            None,
        )
        pb_col = next(
            (c for c in row.index if "市净" in str(c) or "pb" in str(c).lower() or str(c).strip() in ("PB", "市净率")),
            None,
        )
        pe = row.get(pe_col) if pe_col else None
        pb = row.get(pb_col) if pb_col else None
        if _v(pe) != "N/A" or _v(pb) != "N/A":
            return _v(pe), _v(pb)
        # 东财行情常无 PE/PB 列，再单独试新浪行情（列名可能不同）
        try:
            sina_df = _fetch_stock_spot_sina()
            if not sina_df.empty:
                code_col_sina = next((c for c in ("代码", "code", "symbol") if c in sina_df.columns), sina_df.columns[0] if len(sina_df.columns) else None)
                if code_col_sina is not None:
                    mask_sina = sina_df[code_col_sina].astype(str).apply(lambda x: _norm_code(x)) == target
                    if mask_sina.any():
                        row_sina = sina_df.loc[mask_sina].iloc[0]
                        pe_s = next((row_sina.get(c) for c in row_sina.index if "市盈" in str(c) or "pe" in str(c).lower()), None)
                        pb_s = next((row_sina.get(c) for c in row_sina.index if "市净" in str(c) or "pb" in str(c).lower()), None)
                        if _v(pe_s) != "N/A" or _v(pb_s) != "N/A":
                            return _v(pe_s), _v(pb_s)
        except Exception:
            pass
        # 乐咕乐股
        pe2, pb2 = _get_pe_pb_from_lg(symbol)
        if pe2 != "N/A" or pb2 != "N/A":
            return pe2, pb2
        pe2, pb2 = _get_pe_pb_from_stock_info(symbol)
        if pe2 != "N/A" or pb2 != "N/A":
            return pe2, pb2
        # 仅当 akshare 无接口可直接返回 PE/PB 时，才用 最新价/每股收益、每股净资产 估算
        pe2, pb2 = _get_pe_pb_from_eps_bps(symbol, row)
        if pe2 != "N/A" or pb2 != "N/A":
            return pe2, pb2
        return "N/A", "N/A"
    except Exception:
        pe2, pb2 = _get_pe_pb_from_lg(symbol)
        if pe2 != "N/A" or pb2 != "N/A":
            return pe2, pb2
        pe2, pb2 = _get_pe_pb_from_stock_info(symbol)
        if pe2 != "N/A" or pb2 != "N/A":
            return pe2, pb2
        try:
            spot_df = _fetch_stock_spot()
            if not spot_df.empty:
                code_col = next((c for c in ("代码", "code", "symbol") if c in spot_df.columns), spot_df.columns[0])
                mask = spot_df[code_col].astype(str).apply(lambda x: _norm_code(x) == target)
                if mask.any():
                    pe2, pb2 = _get_pe_pb_from_eps_bps(symbol, spot_df.loc[mask].iloc[0])
                    if pe2 != "N/A" or pb2 != "N/A":
                        return pe2, pb2
            pe2, pb2 = _get_pe_pb_from_eps_bps(symbol, None)
            if pe2 != "N/A" or pb2 != "N/A":
                return pe2, pb2
        except Exception:
            pass
        return "N/A", "N/A"


def _get_pe_pb_from_eps_bps(symbol: str, spot_row: pd.Series | None = None) -> tuple[str, str]:
    """仅当 akshare 无直接返回 PE/PB 的接口可用时兜底：用 最新价/每股收益、最新价/每股净资产 估算。"""
    def _v(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "N/A"
        return str(round(float(v), 2))
    try:
        price = None
        if spot_row is not None:
            price_col = next((c for c in spot_row.index if "最新" in str(c) or str(c).strip() in ("最新价", "close", "price")), None)
            if price_col is not None:
                price = pd.to_numeric(spot_row.get(price_col), errors="coerce")
        if price is None or pd.isna(price) or price <= 0:
            try:
                sina = _fetch_stock_spot_sina()
                if not sina.empty:
                    code_col = next((c for c in ("代码", "code", "symbol") if c in sina.columns), sina.columns[0] if len(sina.columns) else None)
                    if code_col is not None:
                        mask = sina[code_col].astype(str).apply(lambda x: _norm_code(x)) == _norm_code(symbol)
                        if mask.any():
                            row = sina.loc[mask].iloc[0]
                            price_col = next((c for c in row.index if "最新" in str(c) or str(c).strip() in ("最新价", "close")), None)
                            if price_col is not None:
                                price = pd.to_numeric(row.get(price_col), errors="coerce")
            except Exception:
                pass
        if price is None or pd.isna(price) or price <= 0:
            # 用最近交易日收盘价当「最新价」算 PE/PB（新浪/东财 spot 均不可用时）
            try:
                end_d = datetime.now().strftime("%Y%m%d")
                start_d = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
                hist = _fetch_stock_history(symbol=symbol, period="daily", start_date=start_d, end_date=end_d)
                if hist is not None and not hist.empty and "收盘" in hist.columns:
                    price = pd.to_numeric(hist["收盘"].iloc[-1], errors="coerce")
            except Exception:
                pass
        if price is None or pd.isna(price) or price <= 0:
            return "N/A", "N/A"
        eps, bps = None, None
        # 新浪财报：宽表，每行一期，列名为指标（基本每股收益、每股净资产等）
        if hasattr(ak, "stock_financial_report_sina"):
            for sym in (symbol, _to_em_symbol(symbol)):
                try:
                    # 利润表 -> 每股收益
                    df_income = ak.stock_financial_report_sina(stock=sym, symbol="利润表")
                    if df_income is not None and not df_income.empty:
                        for col in ("基本每股收益", "稀释每股收益", "每股收益"):
                            if col in df_income.columns:
                                val = pd.to_numeric(df_income[col].iloc[0], errors="coerce")
                                if pd.notna(val) and val > 0:
                                    eps = val
                                    break
                        if eps is not None:
                            break
                except Exception:
                    continue
            for sym in (symbol, _to_em_symbol(symbol)):
                try:
                    # 资产负债表 -> 每股净资产
                    df_balance = ak.stock_financial_report_sina(stock=sym, symbol="资产负债表")
                    if df_balance is not None and not df_balance.empty:
                        bps_col = next((c for c in df_balance.columns if "每股净资产" in str(c)), None)
                        if bps_col:
                            val = pd.to_numeric(df_balance[bps_col].iloc[0], errors="coerce")
                            if pd.notna(val) and val > 0:
                                bps = val
                                break
                except Exception:
                    continue
        if eps is not None and not pd.isna(eps) and eps > 0:
            pe_est = price / eps
        else:
            pe_est = None
        if bps is not None and not pd.isna(bps) and bps > 0:
            pb_est = price / bps
        else:
            pb_est = None
        return _v(pe_est) if pe_est is not None else "N/A", _v(pb_est) if pb_est is not None else "N/A"
    except Exception:
        return "N/A", "N/A"


def _get_pe_pb_from_stock_info(symbol: str) -> tuple[str, str]:
    """从东财个股详情 stock_individual_info_em 的 item/value 中取市盈率、市净率（1.18 未映射该字段则恒为 N/A）。"""
    def _v(v):
        if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "":
            return "N/A"
        return str(v).strip()

    def _parse(df: pd.DataFrame) -> tuple[str, str]:
        if df is None or df.empty:
            return "N/A", "N/A"
        name_col = "item" if "item" in df.columns else (df.columns[0] if len(df.columns) >= 2 else None)
        value_col = "value" if "value" in df.columns else (df.columns[1] if len(df.columns) >= 2 else None)
        if name_col is None or value_col is None:
            return "N/A", "N/A"
        pe, pb = None, None
        for _, row in df.iterrows():
            name = str(row.get(name_col, "")).strip()
            val = row.get(value_col)
            if "市盈" in name or (len(name) <= 8 and "pe" in name.lower()):
                pe = val
            elif "市净" in name or (len(name) <= 8 and "pb" in name.lower()):
                pb = val
        return _v(pe), _v(pb)

    try:
        for sym in (symbol, _to_em_symbol(symbol)):
            df = _fetch_stock_info(sym)
            pe, pb = _parse(df)
            if pe != "N/A" or pb != "N/A":
                return pe, pb
    except Exception:
        pass
    return "N/A", "N/A"


@retry_on_network_error(max_retries=1, base_delay=0.5, silent=True)
def _get_pe_pb_from_lg(symbol: str) -> tuple[str, str]:
    """从乐咕乐股 stock_a_lg_indicator 取单股 PE/PB；单股失败时尝试 symbol=all 再按代码筛选。"""
    def _v(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "N/A"
        return str(v)

    def _row_to_pe_pb(row: pd.Series) -> tuple[str, str]:
        pe = row.get("pe", row.get("pe_ttm", row.get("市盈率", None)))
        pb = row.get("pb", row.get("市净率", None))
        return _v(pe), _v(pb)

    try:
        if not hasattr(ak, "stock_a_lg_indicator"):
            return "N/A", "N/A"
        # 先试单股
        for sym in (symbol, _to_em_symbol(symbol)):
            try:
                df = _call_ak_with_symbol_or_stock(ak.stock_a_lg_indicator, sym)
                if df is not None and not df.empty:
                    return _row_to_pe_pb(df.iloc[-1])
            except Exception:
                continue
        # 单股常失败时，试拉全量再按代码筛（akshare 部分版本单股会 JSONDecodeError）
        try:
            df = None
            for kw, val in [("symbol", "all"), ("stock", "all")]:
                try:
                    df = ak.stock_a_lg_indicator(**{kw: val})
                    break
                except TypeError:
                    continue
            if df is None or df.empty:
                return "N/A", "N/A"
            code_col = next((c for c in ("code", "symbol", "代码", "股票代码") if c in df.columns), None)
            if code_col is None and len(df.columns) > 0:
                code_col = df.columns[0]
            if code_col:
                target = _norm_code(symbol)
                code_ser = df[code_col].astype(str).apply(lambda x: _norm_code(x))
                sub = df.loc[code_ser == target]
                if not sub.empty:
                    return _row_to_pe_pb(sub.iloc[-1])
        except Exception:
            pass
    except Exception:
        pass
    return "N/A", "N/A"


def _extract_growth_from_abstract(df: pd.DataFrame) -> tuple[object, object]:
    """
    从新浪财报摘要（宽表格式）中提取营业收入和净利润的同比增长率。

    Args:
        df: 新浪 stock_financial_abstract 返回的数据，格式为：
            列1: 选项（如"常用指标"）
            列2: 指标（如"营业总收入"、"归母净利润"等）
            列3+: 报告期（如20250930, 20250630等，按时间倒序）

    Returns:
        (营业收入同比增速, 净利润同比增速)，单位为百分比或None
    """
    if df is None or df.empty or len(df.columns) < 4:
        return None, None

    # 确定指标名称列
    indicator_col = None
    for col in ["指标", "项目", "名称"]:
        if col in df.columns:
            indicator_col = col
            break
    if indicator_col is None:
        # 兜底：假设第二列是指标名
        indicator_col = df.columns[1] if len(df.columns) >= 2 else None
    if indicator_col is None:
        return None, None

    # 获取报告期列（排除"选项"和"指标"列，剩余的数字列）
    period_cols = [c for c in df.columns if c not in ["选项", "指标", "项目", "名称"]]
    if len(period_cols) < 2:  # 至少需要2期数据才能计算增长率
        return None, None

    # 按期排序（降序，最新的在前）
    try:
        period_cols_sorted = sorted(period_cols, key=lambda x: str(x), reverse=True)
    except:
        period_cols_sorted = period_cols

    rev_growth = None
    prof_growth = None

    # 尝试查找包含"增长率"或"同比"的行
    for _, row in df.iterrows():
        indicator_name = str(row.get(indicator_col, ""))

        # 营业收入增长率
        if rev_growth is None and any(k in indicator_name for k in ["营业收入增长率", "营业收入同比增长", "营收增长率", "收入增长率"]):
            # 取最近一期数据
            val = row.get(period_cols_sorted[0])
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                try:
                    rev_growth = float(val)
                except:
                    pass

        # 净利润增长率
        if prof_growth is None and any(k in indicator_name for k in ["净利润增长率", "净利润同比增长", "归母净利润增长率"]):
            val = row.get(period_cols_sorted[0])
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                try:
                    prof_growth = float(val)
                except:
                    pass

    # 如果找到了直接的增长率数据，返回
    if rev_growth is not None and prof_growth is not None:
        return rev_growth, prof_growth

    # 否则尝试从绝对值计算同比增长率（需要去年同期数据）
    # 查找最近年报期（例如20231231）和去年年报期（20221231）
    year_periods = [p for p in period_cols_sorted if str(p).endswith("1231")]
    if len(year_periods) >= 2:
        latest_year = year_periods[0]
        prev_year = year_periods[1]

        # 计算营业收入同比增长
        if rev_growth is None:
            for _, row in df.iterrows():
                indicator_name = str(row.get(indicator_col, ""))
                if any(k in indicator_name for k in ["营业总收入", "营业收入"]) and "增长" not in indicator_name:
                    current = row.get(latest_year)
                    previous = row.get(prev_year)
                    if current is not None and previous is not None:
                        try:
                            c = float(current)
                            p = float(previous)
                            if p != 0:
                                rev_growth = ((c - p) / abs(p)) * 100
                                break
                        except:
                            pass

        # 计算净利润同比增长
        if prof_growth is None:
            for _, row in df.iterrows():
                indicator_name = str(row.get(indicator_col, ""))
                if any(k in indicator_name for k in ["归母净利润", "净利润"]) and "增长" not in indicator_name:
                    current = row.get(latest_year)
                    previous = row.get(prev_year)
                    if current is not None and previous is not None:
                        try:
                            c = float(current)
                            p = float(previous)
                            if p != 0:
                                prof_growth = ((c - p) / abs(p)) * 100
                                break
                        except:
                            pass

    return rev_growth, prof_growth


def _fmt_finance_val(val, as_pct: bool = False) -> str:
    """格式化财务指标：as_pct 时按百分比显示（东财接口多为%单位，小数则乘100）。"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/A"
    try:
        v = float(val)
    except (TypeError, ValueError):
        return str(val)
    if as_pct:
        if -1 <= v <= 1 and v != 0 and abs(v) != 1:
            return f"{round(v * 100, 2)}%"
        return f"{round(v, 2)}%"
    return str(round(v, 2)) if isinstance(v, float) else str(v)


@tool
def get_stock_financials(symbol: str) -> str:
    """
    获取 A股核心财务指标（PE、PB、ROE、营收/利润增速等）。

    主要包含：
    - 估值类：市盈率（PE）、市净率（PB）
    - 盈利能力：净资产收益率（ROE）
    - 成长性：营业收入同比增速、净利润同比增速
    数据来源：东方财富主要指标/新浪关键指标，与东财页面口径一致时优先取年报。
    """
    def _fmt(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "N/A"
        return str(val)

    try:
        symbol = validate_stock_code(symbol)
        df = _fetch_stock_financial_analysis_indicator(symbol)
        pe, pb, roe, rev_g, prof_g = None, None, None, None, None
        report_period = None

        if df is not None and not df.empty:
            # 长表格式：列含 指标名称、报告期、指标值（兼容 指标/名称/项目 + 值/数值/value）
            name_candidates = ["指标名称", "指标", "名称", "项目", "item", "name"]
            value_candidates = ["指标值", "数值", "值", "value", "数据"]
            name_col = next((c for c in name_candidates if c in df.columns), None)
            value_col = next((c for c in value_candidates if c in df.columns), None)
            if name_col and value_col:
                period_col = next((c for c in ["报告期", "日期", "date", "报告日期"] if c in df.columns), None)
                if period_col:
                    df = df.sort_values(period_col, ascending=False)
                    report_period = str(df[period_col].iloc[0])
                    first_period = df[period_col].iloc[0]
                    sub = df[df[period_col] == first_period]
                else:
                    sub = df
                for _, r in sub.iterrows():
                    name = str(r.get(name_col, ""))
                    val = r.get(value_col)
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        continue
                    if "净资产收益率" in name or "ROE" in name.upper():
                        roe = val
                    elif "市盈率" in name or ("PE" in name.upper() and "PEG" not in name.upper()):
                        pe = val
                    elif "市净率" in name or "PB" in name.upper():
                        pb = val
                    elif "主营业务收入增长率" in name or ("营业收入" in name and "增长" in name):
                        rev_g = val
                    elif "净利润增长率" in name or ("净利润" in name and "增长" in name):
                        prof_g = val
            if (name_col is None or value_col is None) or (pe is None and pb is None and roe is None):
                # 宽表：每行一期，列为指标名（含东财英文字段 REPORT_DATE, ROEJQ 等）
                period_col = next((c for c in ["REPORT_DATE", "报告期", "date", "报告日期"] if c in df.columns), None)
                if period_col:
                    df = df.sort_values(period_col, ascending=False)
                # 优先取年报（REPORT_DATE 末四位 1231），与东财页面展示一致
                row = None
                if "REPORT_DATE" in df.columns:
                    rd_ser = df["REPORT_DATE"].astype(str)
                    year_end_mask = rd_ser.str.endswith("1231", na=False)
                    if year_end_mask.any():
                        row = df.loc[year_end_mask].iloc[0]  # 取最新年报行（已按 REPORT_DATE 降序）
                    if row is None:
                        row = df.iloc[0]
                else:
                    row = df.iloc[0]
                for k in ["REPORT_DATE", "报告期", "date", "报告日期"]:
                    if k in row.index:
                        report_period = str(row.get(k))
                        break

                def _pick(keys: list[str]):
                    for col, val in row.items():
                        if any(k in str(col) for k in keys):
                            return val
                    return None

                pe = _pick(["市盈率", "PE", "pe", "动态市盈率"])
                pb = _pick(["市净率", "PB", "pb"])
                roe = _pick(["净资产收益率", "ROE", "净资产报酬率"])
                rev_g = _pick(["营业收入同比", "收入同比", "营收同比", "营业收入增长率", "营业收入同比增长"])
                prof_g = _pick(["净利润同比", "利润同比", "净利润增长率", "净利润同比增长"])

                # 东财接口返回英文字段：ROEJQ/PARENTNETPROFITTZ/TOTALOPERATEREVETZ 等（单位均为%）
                if (roe is None or rev_g is None or prof_g is None) and "REPORT_DATE" in df.columns:
                    em_roe, em_rev_g, em_prof_g = _parse_em_finance_row(row)
                    if roe is None:
                        roe = em_roe
                    if rev_g is None:
                        rev_g = em_rev_g
                    if prof_g is None:
                        prof_g = em_prof_g
                    # 若年报行缺营收/利润增速，用最近一期（季报）补
                    if (rev_g is None or prof_g is None) and len(df) > 1:
                        latest_row = df.iloc[0]
                        if not (row is not None and latest_row.equals(row)):
                            em2_roe, em2_rev_g, em2_prof_g = _parse_em_finance_row(latest_row)
                            if rev_g is None:
                                rev_g = em2_rev_g
                            if prof_g is None:
                                prof_g = em2_prof_g

            # 若仍全为空，尝试按列名模糊匹配（兼容不同数据源列名）
            if pe is None and pb is None and roe is None and rev_g is None and prof_g is None:
                row = df.iloc[0] if "报告期" in df.columns else df.iloc[-1]
                for col in row.index:
                    c = str(col)
                    v = row.get(col)
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        continue
                    if ("市盈" in c or "pe" in c.lower()) and pe is None:
                        pe = v
                    elif ("市净" in c or "pb" in c.lower()) and pb is None:
                        pb = v
                    elif ("净资产收益" in c or "roe" in c.lower()) and roe is None:
                        roe = v
                    elif (("收入" in c and "同比" in c) or ("营收" in c and "增" in c)) and rev_g is None:
                        rev_g = v
                    elif ("净利润" in c and ("同比" in c or "增" in c)) and prof_g is None:
                        prof_g = v

        # ROE/营收增速/利润增速 主数据源未解析到时，从新浪摘要与东财同行比较接口补数
        if roe is None or rev_g is None or prof_g is None:
            # 首先尝试从已获取的 df 中提取（如果是新浪摘要格式）
            if df is not None and not df.empty and "指标" in df.columns:
                rev_g_new, prof_g_new = _extract_growth_from_abstract(df)
                if rev_g is None and rev_g_new is not None:
                    rev_g = rev_g_new
                if prof_g is None and prof_g_new is not None:
                    prof_g = prof_g_new

            # 如果还是缺失，调用备用接口
            if roe is None or rev_g is None or prof_g is None:
                roe_fb, rev_g_fb, prof_g_fb = _fetch_roe_revg_profg_fallback(symbol)
                if roe is None:
                    roe = roe_fb
                if rev_g is None:
                    rev_g = rev_g_fb
                if prof_g is None:
                    prof_g = prof_g_fb

        # 财务接口无数据或缺少 PE/PB 时，从行情兜底取市盈率、市净率
        pe_spot, pb_spot = _get_pe_pb_from_spot(symbol)
        if pb_spot == "N/A" and hasattr(ak, "stock_zh_valuation_comparison_em"):
            try:
                em_sym = _to_em_symbol(symbol).upper()
                vdf = ak.stock_zh_valuation_comparison_em(symbol=em_sym)
                if vdf is not None and not vdf.empty:
                    for col in ("市净率-MRQ", "市净率-24A", "市净率"):
                        if col in vdf.columns:
                            v = vdf.iloc[0].get(col)
                            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                                pb_spot = str(round(float(v), 2))
                                break
            except Exception:
                pass
        if (pe is None or str(pe) == "nan") and (pb is None or str(pb) == "nan"):
            if pe_spot != "N/A" or pb_spot != "N/A":
                header = f"股票 {symbol} 核心财务指标（估值来自行情）\n"
                header += f"  市盈率(动态) PE: {pe_spot}\n"
                header += f"  市净率 PB: {pb_spot}\n"
                if roe is not None or rev_g is not None or prof_g is not None:
                    header += f"  净资产收益率 ROE: {_fmt_finance_val(roe, as_pct=True)}\n"
                    header += f"  营业收入同比增速: {_fmt_finance_val(rev_g, as_pct=True)}\n"
                    header += f"  净利润同比增速: {_fmt_finance_val(prof_g, as_pct=True)}\n"
                else:
                    header += "  （财报类指标 ROE/营收与利润增速 当前数据源暂不可用，可稍后再试或结合行情做估值参考。）"
                return header
        elif pe is None or str(pe) == "nan":
            pe = pe_spot if pe_spot != "N/A" else None
        elif pb is None or str(pb) == "nan":
            pb = pb_spot if pb_spot != "N/A" else None

        # 无可用数据时：df 为空，或 df 有表但解析后五项全为空，均走行情/个股信息兜底
        no_usable_from_df = (
            df is None
            or df.empty
            or (
                pe is None
                and pb is None
                and roe is None
                and rev_g is None
                and prof_g is None
            )
        )
        if no_usable_from_df:
            if pe_spot != "N/A" or pb_spot != "N/A":
                header = f"股票 {symbol} 核心财务指标（估值来自行情/个股信息）\n"
                header += f"  市盈率(动态) PE: {pe_spot}\n"
                header += f"  市净率 PB: {pb_spot}\n"
                header += "  （财报类指标 ROE/营收与利润增速 当前数据源暂不可用，可稍后再试或结合行情做估值参考。）"
                return header
            return (
                f"暂时无法获取股票 {symbol} 的财务分析指标数据（可能尚未披露或数据源不可用）。\n\n"
                "提示：你可以改用市值、市盈率等简单指标进行大致估值，或稍后再试。"
            )

        # 报告期格式化为 YYYY-MM-DD，并标注年报/季报
        period_label = ""
        if report_period:
            s = str(report_period).strip()
            if len(s) >= 8 and s[:8].isdigit():
                period_label = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
                if s.endswith("1231"):
                    period_label += "（年报）"
                else:
                    period_label += "（报告期）"
        header = f"股票 {symbol} 核心财务指标"
        if period_label:
            header += f"（{period_label}）"
        header += ":\n"

        output = header
        output += f"  市盈率 PE: {_fmt(pe)}\n"
        output += f"  市净率 PB: {_fmt(pb)}\n"
        output += f"  净资产收益率 ROE: {_fmt_finance_val(roe, as_pct=True)}\n"
        output += f"  营业收入同比增速: {_fmt_finance_val(rev_g, as_pct=True)}\n"
        output += f"  净利润同比增速: {_fmt_finance_val(prof_g, as_pct=True)}\n"
        output += "\n以上指标可用于基本面的估值与成长性分析。"
        return output
    except Exception as e:
        return f"获取核心财务指标失败: {str(e)[:200]}"


@tool
def search_stock(keyword: str) -> str:
    """
    根据关键词搜索股票。

    Args:
        keyword: 搜索关键词，可以是股票名称或代码的一部分

    Returns:
        匹配的股票列表
    """
    try:
        kw = (keyword or "").strip()
        if not kw:
            return (
                "请输入搜索关键词（股票名称或代码的一部分）。\n\n"
                "示例: 平安、茅台、600519、000001"
            )

        # 搜索默认用「代码+名称列表」（更快、更稳定）；实时行情需要时再用 get_stock_realtime 查单只
        df = _get_stock_list_code_name_cached()
        list_only = True
        if df.empty:
            # 兜底：代码列表也失败时再尝试全市场行情（可能较慢/易断连）
            df = _fetch_stock_spot()
            list_only = False
            if df.empty:
                return (
                    "暂时无法获取股票列表/行情数据，请稍后重试。\n\n"
                    "也可直接使用 6 位代码查询，如: 000001、600519、300750"
                )

        # 确保用于筛选的列存在且为字符串，避免 代码 为数值时 .str 报错
        if "代码" not in df.columns or "名称" not in df.columns:
            # 兜底：尝试从全市场行情里搜索（可能较慢，但列名一般规范）
            try:
                df2 = _fetch_stock_spot()
                if not df2.empty and "代码" in df2.columns and "名称" in df2.columns:
                    df = df2
                    list_only = False
                else:
                    cols = ", ".join([str(c) for c in list(df.columns)[:8]])
                    return (
                        "暂时无法解析股票列表数据格式，无法完成搜索。\n\n"
                        f"当前返回列(部分): {cols}\n\n"
                        "建议：直接用 6 位股票代码查询，或稍后重试。"
                    )
            except Exception:
                cols = ", ".join([str(c) for c in list(df.columns)[:8]])
                return (
                    "暂时无法获取/解析股票列表数据，无法完成搜索。\n\n"
                    f"当前返回列(部分): {cols}\n\n"
                    "建议：直接用 6 位股票代码查询，或稍后重试。"
                )

        code_ser = df["代码"].astype(str).str.replace(r"\D", "", regex=True)
        # 代码统一为 6 位便于匹配：用户搜 "1" 或 "000001" 都能命中
        code_ser = code_ser.str.zfill(6)
        name_ser = df["名称"].astype(str).fillna("")
        kw_clean = kw.replace(" ", "")
        mask = (
            code_ser.str.contains(kw_clean, case=False, na=False)
            | name_ser.str.contains(kw, case=False, na=False)
        )
        result_df = df.loc[mask]

        # 只选取存在的列，避免 KeyError（代码列表通常只有 代码/名称）
        out_cols = [c for c in ["代码", "名称", "最新价", "涨跌幅"] if c in result_df.columns]
        if not out_cols:
            return f"未找到与 '{kw}' 相关的股票。请检查关键词或改用 6 位代码查询。"
        result_df = result_df[out_cols].head(20)

        if result_df.empty:
            return (
                f"未找到与 '{kw}' 相关的股票。\n\n"
                f"提示: 请用 6 位数股票代码查询，例如 000001、600519、300750"
            )
        header = f"搜索 '{kw}' 的结果（前20个）"
        if list_only:
            header += "（快速匹配：仅代码与名称；需要实时价格请用 get_stock_realtime）"
        return f"{header}:\n\n{format_dataframe(result_df)}"
    except Exception as e:
        return f"搜索股票失败: {str(e)[:200]}\n\n建议直接使用 6 位股票代码查询"


def _invoke_sub_tool(tool_obj, args: dict) -> str:
    """
    在工具内部安全调用其他工具或普通函数。

    兼容以下两种情况：
    - LangChain 的 StructuredTool / BaseTool（带 .invoke）
    - 普通可调用函数（直接调用）
    """
    # LangChain 工具: 必须使用 .invoke(args)，不能写成 tool_obj(args)，否则会报 'StructuredTool' object is not callable
    if hasattr(tool_obj, "invoke"):
        try:
            out = tool_obj.invoke(args)
            return out if isinstance(out, str) else str(out)
        except TypeError as e:
            if "not callable" in str(e).lower():
                return f"子工具调用方式异常（{type(tool_obj).__name__} 请使用 .invoke），请稍后重试或改用 search_stock / search_stock_hk。"
            raise
        except Exception as e:
            raise

    # 普通 Python 函数
    if callable(tool_obj):
        return tool_obj(**args)

    raise TypeError(f"不支持的子工具类型: {type(tool_obj)}")


@tool
def search_stock_any(keyword: str) -> str:
    """
    智能搜索 A股、港股股票。

    当用户只说“搜索股票”或不指定市场时，推荐优先使用本工具：
    - 能自动根据关键词特征判断更可能的市场
    - 若无法确定，会依次尝试 A股 -> 港股，只要有结果就返回

    Args:
        keyword: 搜索关键词，可以是股票名称或代码的一部分

    Returns:
        匹配的股票列表，并在可能时标注来源市场
    """
    try:
        kw = (keyword or "").strip()
        if not kw:
            return (
                "请输入搜索关键词（股票名称或代码的一部分）。\n\n"
                "示例: 平安、茅台、600519、00700、AAPL"
            )

        kw_upper = kw.upper()
        is_digits = kw.isdigit()

        def _call_a() -> str:
            return _invoke_sub_tool(search_stock, {"keyword": kw})

        def _call_hk() -> str:
            return _invoke_sub_tool(search_stock_hk, {"keyword": kw})

        order: list[tuple[str, Callable[[], str]]] = []

        # 简单规则判断优先市场
        if is_digits and len(kw) == 6:
            # 6 位纯数字更像 A 股
            order = [("A股", _call_a), ("港股", _call_hk)]
        elif is_digits and len(kw) == 5:
            # 5 位纯数字更像港股
            order = [("港股", _call_hk), ("A股", _call_a)]
        elif kw_upper.startswith("HK") or "港股" in kw or "HK:" in kw_upper:
            order = [("港股", _call_hk), ("A股", _call_a)]
        elif any(tag in kw_upper for tag in ["US:", "NASDAQ", "NYSE"]):
            # 当前版本不再支持美股数据，直接提示
            return (
                "当前版本暂不支持美股数据查询。\n\n"
                "请使用 A股或港股代码/名称进行搜索，例如 A股 600519、港股 00700。"
            )
        elif re.match(r"^[A-Z][A-Z0-9\.\-]{0,10}$", kw_upper):
            # 纯英文代码，当前版本不支持美股
            return (
                "检测到可能为美股代码，但当前版本暂不支持美股数据查询。\n\n"
                "请使用 A股或港股代码/名称进行搜索，例如 A股 600519、港股 00700。"
            )
        else:
            # 自然语言或模糊关键词，默认先试 A 股
            order = [("A股", _call_a), ("港股", _call_hk)]

        last_msg = ""

        for market_name, fn in order:
            try:
                msg = fn()
            except Exception as e:
                err = str(e)
                if "not callable" in err.lower() or "StructuredTool" in err:
                    last_msg = f"{market_name} 搜索暂时不可用，请使用上方 A 股结果或直接输入 6 位代码（如 600519）查询。"
                else:
                    last_msg = f"{market_name} 搜索失败: {err[:120]}"
                continue

            # 若返回的是明显的“未找到”提示，则继续尝试其它市场
            if "未找到与" in msg and "相关的" in msg:
                last_msg = msg
                continue
            # 明确的数据源故障/超时提示，也尝试其它市场
            if "无法获取" in msg or "超时" in msg:
                last_msg = msg
                continue

            # 命中有效结果，附带来源市场标注（若原文中尚未包含）
            if (
                "搜索 '" in msg
                and "的结果" in msg
                and "（前20个）" in msg
                and "（来源：" not in msg
            ):
                return msg + f"\n\n（来源：{market_name}）"
            return msg

        if last_msg:
            return last_msg

        return (
            f"未在 A股、港股中找到与 '{kw}' 相关的股票。\n\n"
            "提示: 也可以直接使用具体代码查询，例如 A股 600519、港股 00700。"
        )
    except Exception as e:
        return f"搜索股票失败: {str(e)[:200]}"


@tool
def get_stock_news(symbol: str) -> str:
    """
    获取个股新闻资讯。

    Args:
        symbol: 股票代码，如 "000001"

    Returns:
        最近的新闻标题和时间
    """
    try:
        symbol = validate_stock_code(symbol)
        df = _fetch_stock_news(symbol)

        if df.empty:
            return f"未找到股票 {symbol} 的相关新闻"

        # Select relevant columns and limit results
        result_df = df.head(10)
        output = f"股票 {symbol} 最新新闻:\n\n"
        for _, row in result_df.iterrows():
            output += f"- [{row.get('发布时间', 'N/A')}] {row.get('新闻标题', 'N/A')}\n"
        return output
    except Exception as e:
        return f"获取新闻失败: {str(e)[:200]}"


@tool
def get_hot_stocks() -> str:
    """
    获取当前热门股票排行。

    Returns:
        热门股票列表，按人气排名
    """
    try:
        df = _fetch_hot_stocks()

        if df.empty:
            return "暂无热门股票数据"

        # Select available columns dynamically
        preferred_cols = ["排名", "序号", "代码", "股票代码", "名称", "股票名称", "最新价", "涨跌幅"]
        available_cols = [c for c in preferred_cols if c in df.columns]
        if not available_cols:
            available_cols = list(df.columns[:5])

        result_df = df.head(20)[available_cols]
        return f"热门股票排行:\n\n{format_dataframe(result_df)}"
    except Exception as e:
        return f"获取热门股票失败: {str(e)[:200]}"


@tool
def get_industry_boards() -> str:
    """
    获取行业板块列表及行情。

    Returns:
        行业板块列表，包括涨跌幅和领涨股
    """
    try:
        df = _fetch_industry_boards()

        if df.empty:
            return (
                "❌ 无法获取行业板块数据\n\n"
                "可能原因：\n"
                "1. 当前时段非交易时间\n"
                "2. 数据源接口临时不可用\n"
                "3. 网络连接问题\n\n"
                "💡 建议：\n"
                "- 改为查询具体股票\n"
                "- 稍后再试"
            )

        # Sort by change percentage
        if "涨跌幅" in df.columns:
            df = df.sort_values("涨跌幅", ascending=False)

        result_df = df.head(20)
        return f"行业板块排行:\n\n{format_dataframe(result_df)}"
    except Exception as e:
        return (
            f"❌ 获取行业板块失败\n\n"
            f"错误信息: {str(e)[:150]}\n\n"
            f"💡 建议：改为查询具体股票或稍后重试"
        )


@retry_on_network_error(max_retries=3, base_delay=1.2, silent=True)
def _fetch_industry_cons_em(symbol: str) -> pd.DataFrame:
    """获取指定行业板块成分股（东方财富）。symbol 为板块名称，如 酿酒行业、小金属。"""
    return ak.stock_board_industry_cons_em(symbol=symbol)


@tool
def get_industry_board_detail(industry_name: str) -> str:
    """
    获取指定行业板块的整体涨跌幅及行业平均估值（PE、PB）。

    用于回答某行业（如白酒、酿酒、食品饮料、新能源）的整体走势与估值水平。
    先根据关键词匹配行业名称，再返回该板块指数涨跌幅、领涨股及成分股平均市盈率/市净率。

    Args:
        industry_name: 行业名称或关键词，如 "白酒"、"酿酒"、"食品饮料"、"新能源"、"电池"

    Returns:
        该行业板块的整体涨跌幅、领涨股、成分股数量及行业平均 PE/PB
    """
    try:
        name = (industry_name or "").strip()
        if not name:
            return "请输入行业名称或关键词，例如：白酒、酿酒、食品饮料、新能源、电池。可先调用 get_industry_boards 查看全部行业板块名称。"

        def _industry_fallback_msg(keyword: str) -> str:
            return (
                "暂时无法获取行业板块列表（东财/同花顺接口连接异常或限流）。\n\n"
                "💡 建议：\n"
                "- 先查询具体股票，如 贵州茅台(600519)、五粮液 等了解个股行情\n"
                "- 稍后重试 get_industry_boards 或 get_industry_board_detail\n"
                f"- 白酒相关在列表中多为「酿酒行业」，可恢复后搜「酿酒」"
            )

        try:
            df = _fetch_industry_boards()
        except Exception:
            return _industry_fallback_msg(name)
        if df.empty:
            return _industry_fallback_msg(name)

        # 板块名称列可能为 "板块名称" 或 "行业名称" 等
        name_col = None
        for col in df.columns:
            if "名称" in str(col) and ("板块" in str(col) or "行业" in str(col)):
                name_col = col
                break
        if name_col is None:
            name_col = "板块名称" if "板块名称" in df.columns else df.columns[0]

        names = df[name_col].astype(str).str.strip()
        # 常见别名（东方财富板块名称多为「XX行业」）
        alias = {"白酒": "酿酒", "锂电": "能源金属", "光伏": "光伏设备", "芯片": "半导体"}
        search_name = alias.get(name, name)
        # 精确匹配
        match = names.str.lower() == search_name.lower()
        if not match.any():
            # 模糊匹配：关键词包含在板块名称中
            match = names.str.contains(search_name, case=False, na=False)
        if not match.any():
            match = names.str.contains(name, case=False, na=False)
        if not match.any():
            return (
                f"未找到与「{name}」匹配的行业板块。\n\n"
                "请先调用 get_industry_boards 查看完整行业列表，或使用更通用的关键词（如 酿酒、食品饮料、电池）。"
            )

        row = df.loc[match].iloc[0]
        board_name = str(row.get(name_col, name))
        # 东方财富成分股接口需使用精确的板块名称
        code_col = "板块代码" if "板块代码" in df.columns else None
        board_code = str(row[code_col]) if code_col and code_col in row.index else ""

        change = row.get("涨跌幅", "N/A")
        latest = row.get("最新价", "N/A")
        leader = row.get("领涨股票", row.get("领涨股", "N/A"))
        leader_change = row.get("领涨股票-涨跌幅", row.get("领涨股-涨跌幅", ""))

        avg_pe = None
        avg_pb = None
        cons_count = 0
        cons_df = None
        try:
            cons_df = _fetch_industry_cons_em(board_name)
        except Exception:
            pass
        if cons_df is not None and not cons_df.empty:
            cons_count = len(cons_df)
            pe_col = None
            pb_col = None
            for c in cons_df.columns:
                if "市盈" in str(c) or "PE" in str(c):
                    pe_col = c
                if "市净" in str(c) or "PB" in str(c):
                    pb_col = c
            if pe_col:
                vals = pd.to_numeric(cons_df[pe_col], errors="coerce").dropna()
                vals = vals[vals > 0][vals < 1e5]
                if not vals.empty:
                    avg_pe = round(vals.mean(), 2)
            if pb_col:
                vals = pd.to_numeric(cons_df[pb_col], errors="coerce").dropna()
                vals = vals[vals > 0][vals < 1e4]
                if not vals.empty:
                    avg_pb = round(vals.mean(), 2)

        def _fmt_num(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return "N/A"
            if isinstance(v, (int, float)):
                return f"{v}%"
            return str(v)

        lines = [
            f"行业板块：{board_name}",
            f"板块整体涨跌幅：{_fmt_num(change) if isinstance(change, (int, float)) else change}",
            f"板块最新价：{latest}",
            f"领涨股票：{leader} {leader_change}%" if leader_change != "" and leader_change != "N/A" else f"领涨股票：{leader}",
            f"成分股数量：{cons_count}",
        ]
        if avg_pe is not None:
            lines.append(f"行业平均市盈率（PE）：{avg_pe}")
        if avg_pb is not None:
            lines.append(f"行业平均市净率（PB）：{avg_pb}")
        if avg_pe is None and avg_pb is None and cons_count > 0:
            lines.append("（成分股 PE/PB 暂未统计，部分标的可能无估值数据）")
        if cons_count == 0 and (avg_pe is None and avg_pb is None):
            lines.append("（行业平均估值因网络波动暂时无法获取，请稍后再试或仅参考上方板块涨跌幅与领涨股）")

        return "\n".join(lines)
    except Exception as e:
        return f"获取行业板块详情失败: {str(e)[:200]}"


@tool
def get_concept_boards() -> str:
    """
    获取概念板块列表及行情。

    Returns:
        概念板块列表，包括涨跌幅和领涨股
    """
    try:
        df = _fetch_concept_boards()

        if df.empty:
            return (
                "❌ 无法获取概念板块数据\n\n"
                "可能原因：\n"
                "1. 当前时段非交易时间\n"
                "2. 数据源接口临时不可用\n"
                "3. 网络连接问题\n\n"
                "💡 建议：\n"
                "- 改为查询具体股票\n"
                "- 稍后再试"
            )

        # Sort by change percentage
        if "涨跌幅" in df.columns:
            df = df.sort_values("涨跌幅", ascending=False)

        # 返回较多条以便「AI概念」等推荐场景能命中相关板块（如 人工智能、ChatGPT概念）
        result_df = df.head(50)
        return f"概念板块排行:\n\n{format_dataframe(result_df)}"
    except Exception as e:
        return (
            f"❌ 获取概念板块失败\n\n"
            f"错误信息: {str(e)[:150]}\n\n"
            f"💡 建议：改为查询具体股票或稍后重试"
        )


@retry_on_network_error(max_retries=2, base_delay=1.0, silent=True)
def _fetch_concept_stocks_em(concept_name: str) -> pd.DataFrame:
    """获取概念板块成分股 - 东方财富，symbol 为板块名称或板块代码(BKxxxx)"""
    return ak.stock_board_concept_cons_em(symbol=concept_name)


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_concept_stocks_em_direct(board_code: str) -> pd.DataFrame:
    """
    东方财富概念板块成分股（直连接口 + 显式 timeout + 分页）

    目的：避免 akshare 内部 fetch_paginated_data 在特定网络下卡住/返回空时无法诊断。
    """
    board_code = (board_code or "").strip().upper()
    if not re.match(r"^BK\d+", board_code):
        raise ValueError(f"东方财富板块代码不合法: {board_code}")

    hosts = [
        "29.push2.eastmoney.com",
        "79.push2.eastmoney.com",
        "39.push2.eastmoney.com",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://quote.eastmoney.com/",
    }

    fields = "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152,f45"
    rows: list[dict] = []
    last_err: Exception | None = None
    for host in hosts:
        url = f"https://{host}/api/qt/clist/get"
        try:
            rows = []
            for pn in range(1, 11):  # 最多取 10 页 * 100 = 1000
                params = {
                    "pn": str(pn),
                    "pz": "100",
                    "po": "1",
                    "np": "1",
                    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                    "fltt": "2",
                    "invt": "2",
                    "fid": "f12",
                    "fs": f"b:{board_code} f:!50",
                    "fields": fields,
                }
                r = requests.get(url, params=params, headers=headers, timeout=6)
                r.raise_for_status()
                data = r.json()
                diff = (((data or {}).get("data") or {}).get("diff")) or []
                if not diff:
                    break
                rows.extend(diff)
                if len(diff) < 100:
                    break
            if rows:
                break
        except Exception as e:
            last_err = e
            continue

    if not rows:
        if last_err:
            raise last_err
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # 字段映射与 akshare 保持一致的核心列
    col_map = {
        "f12": "代码",
        "f14": "名称",
        "f2": "最新价",
        "f3": "涨跌幅",
        "f4": "涨跌额",
        "f6": "成交额",
        "f5": "成交量",
    }
    df = df.rename(columns=col_map)
    keep = [c for c in ["代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交额", "成交量"] if c in df.columns]
    df = df[keep] if keep else df
    return _normalize_concept_stocks_df(df)


@lru_cache(maxsize=1)
def _ths_v_cookie() -> str:
    """
    生成同花顺访问所需的 v Cookie（若依赖缺失则返回空字符串）。

    注意：由于 py_mini_racer 在某些环境下可能导致崩溃，默认禁用。
    如需启用，设置环境变量 OPENFR_ENABLE_THS_JS=1
    """
    import os

    # 默认禁用 py_mini_racer 以避免崩溃
    if os.getenv("OPENFR_ENABLE_THS_JS", "0") != "1":
        return ""

    try:
        from akshare.datasets import get_ths_js
        import py_mini_racer  # type: ignore

        setting_file_path = get_ths_js("ths.js")
        with open(setting_file_path, encoding="utf-8") as f:
            js_content = f.read()
        js_code = py_mini_racer.MiniRacer()
        js_code.eval(js_content)
        v_code = js_code.call("v")
        return str(v_code)
    except Exception:
        return ""


def _resolve_em_concept_board_code(concept_name: str) -> str | None:
    """
    尝试从东方财富概念板块列表里解析板块代码(BKxxxx)。
    支持精确匹配与包含匹配（解决名称不完全一致问题）。
    """
    name = (concept_name or "").strip()
    if not name:
        return None
    try:
        df = _fetch_concept_boards_em()
        if df is None or df.empty:
            return None
        if "板块名称" not in df.columns or "板块代码" not in df.columns:
            return None

        s = df["板块名称"].astype(str).str.strip()
        exact = df.loc[s == name, "板块代码"]
        if not exact.empty:
            return str(exact.values[0]).strip()

        # 包含匹配：例如 “ChatGPT” vs “ChatGPT概念”
        contains = df.loc[s.str.contains(re.escape(name), na=False), "板块代码"]
        if not contains.empty:
            return str(contains.values[0]).strip()
    except Exception:
        return None
    return None


def _resolve_ths_concept_code(concept_name: str) -> str | None:
    """从同花顺概念列表里解析概念 code(数字)。"""
    name = (concept_name or "").strip()
    if not name:
        return None
    try:
        df = ak.stock_board_concept_name_ths()
        if df is None or df.empty or "name" not in df.columns or "code" not in df.columns:
            return None
        s = df["name"].astype(str).str.strip()
        exact = df.loc[s == name, "code"]
        if not exact.empty:
            return str(exact.values[0]).strip()
        contains = df.loc[s.str.contains(re.escape(name), na=False), "code"]
        if not contains.empty:
            return str(contains.values[0]).strip()
    except Exception:
        return None
    return None


def _normalize_concept_stocks_df(df: pd.DataFrame) -> pd.DataFrame:
    """统一概念成分股字段并做基础清洗。"""
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    rename_map: dict[str, str] = {}
    for c in df.columns:
        cc = str(c).strip()
        if cc in ("证券代码", "股票代码"):
            rename_map[c] = "代码"
        elif cc in ("证券简称", "股票简称"):
            rename_map[c] = "名称"
        elif cc == "现价":
            rename_map[c] = "最新价"
    if rename_map:
        df = df.rename(columns=rename_map)

    if "代码" in df.columns:
        s = df["代码"].astype(str).str.strip()
        s = s.str.replace(r"\D", "", regex=True).str.zfill(6)
        df["代码"] = s

    if "涨跌幅" in df.columns:
        s = df["涨跌幅"].astype(str).str.replace("%", "", regex=False)
        df["涨跌幅"] = pd.to_numeric(s, errors="coerce")

    return df


@retry_on_network_error(max_retries=2, base_delay=1.0, silent=True)
def _fetch_concept_stocks_ths(concept_name: str) -> pd.DataFrame:
    """
    获取概念板块成分股 - 同花顺网页解析兜底

    说明：东财概念成分股接口在部分网络环境下可能经常断连/返回空；
    这里抓取同花顺概念详情页表格作为备用来源。
    """
    concept_name = (concept_name or "").strip()
    if not concept_name:
        return pd.DataFrame()

    name_df = ak.stock_board_concept_name_ths()
    if (
        name_df is None
        or name_df.empty
        or "name" not in name_df.columns
        or "code" not in name_df.columns
    ):
        raise RuntimeError("同花顺概念板块列表获取失败或格式异常")

    match = name_df[name_df["name"].astype(str).str.strip() == concept_name]
    if match.empty:
        raise ValueError(f"同花顺未找到概念名称: {concept_name}")

    symbol_code = str(match["code"].values[0]).strip()
    if not re.match(r"^\d+$", symbol_code):
        raise RuntimeError(f"同花顺概念 code 异常: {symbol_code}")

    v_code = _ths_v_cookie()
    url = f"https://q.10jqka.com.cn/gn/detail/code/{symbol_code}/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://q.10jqka.com.cn/gn/",
    }
    if v_code:
        headers["Cookie"] = f"v={v_code}"

    r = requests.get(url, headers=headers, timeout=8)
    r.raise_for_status()

    # 页面包含多个表格：挑出“代码/名称”齐全的一张
    # 有时会返回“访问受限/验证码”，提前给出更明确的错误
    if any(k in r.text for k in ("验证码", "访问受限", "403", "请开启JavaScript")):
        raise RuntimeError("同花顺页面可能触发反爬/验证码，无法解析成分股")

    tables = pd.read_html(r.text)
    for t in tables:
        if t is None or t.empty:
            continue
        t.columns = [str(c).strip() for c in t.columns]
        cols = set(t.columns)
        has_code = bool(cols.intersection({"代码", "证券代码", "股票代码"}))
        has_name = bool(cols.intersection({"名称", "证券简称", "股票简称"}))
        if has_code and has_name:
            return _normalize_concept_stocks_df(t)

    return pd.DataFrame()


def _get_concept_stocks_impl(concept_name: str) -> str:
    """
    获取指定概念板块的成分股列表及行情，用于概念股推荐。

    可先调用 get_concept_boards 查看板块名称，再传入本接口。例如：人工智能、ChatGPT概念、AI芯片、机器人概念 等。

    Args:
        concept_name: 概念板块名称或东财板块代码(BK开头)，如 "人工智能"、"ChatGPT概念"

    Returns:
        该概念下的成分股列表，含代码、名称、最新价、涨跌幅等
    """
    concept_name = (concept_name or "").strip()
    if not concept_name:
        return "请传入概念板块名称，如：人工智能、ChatGPT概念。可先调用 get_concept_boards 查看可选板块。"

    # 若传入的是东方财富 BK 代码，直接走直连（更稳、更快）
    if re.match(r"^BK\\d+", concept_name.upper()):
        df0 = _fetch_concept_stocks_em_direct(concept_name.upper())
        if not df0.empty:
            if "涨跌幅" in df0.columns:
                df0 = df0.sort_values("涨跌幅", ascending=False)
            out_cols = [c for c in ["代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交额"] if c in df0.columns]
            result_df0 = df0[out_cols].head(30) if out_cols else df0.head(30)
            return f"概念「{concept_name.upper()}」成分股（按涨跌幅）:\n\n{format_dataframe(result_df0)}"

    # 当用户/Agent 问「AI概念」时，东财可能无此精确名称，先试常见 AI 相关板块名
    aliases: list[str] = []
    if any(k in concept_name for k in ("AI", "ai", "人工智能", "人工")):
        aliases = ["人工智能", "ChatGPT概念", "AI芯片", "AIGC概念"]
    to_try = [concept_name] + [a for a in aliases if a != concept_name]

    df = pd.DataFrame()
    used_name = concept_name
    errors: list[str] = []
    for name in to_try:
        name = (name or "").strip()
        if not name:
            continue

        # 先把名称解析成东方财富 BK 代码再查（避免名称不完全一致导致 akshare 内部映射失败）
        em_code = _resolve_em_concept_board_code(name)
        if em_code:
            try:
                tmp = _fetch_concept_stocks_em_direct(em_code)
                tmp = _normalize_concept_stocks_df(tmp)
                if not tmp.empty:
                    df = tmp
                    used_name = name
                    break
            except Exception as e:
                errors.append(f"{name}(东财直连:{str(e)[:120]})")

        # 兼容：仍尝试 akshare 原接口（可能已缓存/可用），再失败用同花顺兜底
        last_err: str | None = None
        for fetcher, tag in ((_fetch_concept_stocks_em, "东财"), (_fetch_concept_stocks_ths, "同花顺")):
            try:
                tmp = fetcher(name)
                tmp = _normalize_concept_stocks_df(tmp)
                if not tmp.empty:
                    df = tmp
                    used_name = name
                    break
            except Exception as e:
                last_err = f"{tag}:{str(e)[:120]}"

        if not df.empty:
            break
        if last_err:
            errors.append(f"{name}({last_err})")

    if df.empty:
        detail = ""
        if errors:
            detail = "\n\n最近错误(部分):\n- " + "\n- ".join(errors[-3:])
        tried = "、".join([t for t in to_try if t])
        return (
            f"未获取到概念「{concept_name}」的成分股数据。\n\n"
            f"请先调用 get_concept_boards 确认板块名称（如：人工智能、ChatGPT概念、AI芯片）。"
            f"\n\n本次已尝试: {tried}"
            f"{detail}"
        )

    if "涨跌幅" in df.columns:
        df = df.sort_values("涨跌幅", ascending=False)
    out_cols = [c for c in ["代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交额"] if c in df.columns]
    result_df = df[out_cols].head(30) if out_cols else df.head(30)
    return f"概念「{used_name}」成分股（按涨跌幅）:\n\n{format_dataframe(result_df)}"


@tool
def get_concept_stocks(concept_name: str) -> str:
    """
    带整体超时保护的外层工具封装，避免在网络异常时卡住整轮思考。
    """
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_get_concept_stocks_impl, concept_name)
            return future.result(timeout=CONCEPT_STOCKS_TOTAL_TIMEOUT)
    except FutureTimeoutError:
        return (
            "获取概念成分股超时，数据源响应过慢或网络不稳定。\n\n"
            "建议：\n"
            "- 先调用 get_concept_boards 查看板块列表，确认板块代码(BK 开头) 后再查；\n"
            "- 或稍后重试，必要时缩小概念范围，例如改用具体细分概念名称。"
        )
    except Exception as e:
        err = str(e)[:200]
        if "板块名称" in err or "values" in err or "KeyError" in err or "IndexError" in err:
            return (
                f"未找到概念「{concept_name}」。请先调用 get_concept_boards 查看准确板块名称（如：人工智能、ChatGPT概念）后再试。"
            )
        return f"获取概念成分股失败: {err}"
