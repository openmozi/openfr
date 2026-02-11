"""
Hong Kong stock data tools based on AKShare.

数据源策略：优先东方财富，失败时使用新浪 stock_hk_spot 备用，避免 RemoteDisconnected。
"""

import akshare as ak
import pandas as pd
from langchain_core.tools import tool
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

from openfr.tools.base import format_dataframe, retry_on_network_error

# 搜索类操作总超时时间，避免在网络异常时挂住一整轮
HK_STOCK_SEARCH_TIMEOUT = 6.0

# 港股全市场行情缓存（减少重复拉取导致的超时）
HK_SPOT_CACHE_TTL = 600.0  # 秒
_HK_SPOT_CACHE_DF: pd.DataFrame | None = None
_HK_SPOT_CACHE_TS: float | None = None


def _normalize_sina_hk_spot(df: pd.DataFrame) -> pd.DataFrame:
    """将新浪港股 spot 列名统一为与东方财富一致，便于复用展示逻辑。"""
    if df.empty:
        return df
    if "名称" in df.columns:
        return df
    if "中文名称" in df.columns:
        out = df.copy()
        out["名称"] = out["中文名称"].astype(str)
        return out
    return df


def _try_multiple_sources(fetch_functions: list, delay: float = 1.0) -> pd.DataFrame:
    """依次尝试多个数据源，返回第一个成功且非空的结果；全部失败时返回空 DataFrame，不抛错。"""
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


@retry_on_network_error(max_retries=2, base_delay=1.0, silent=True)
def _fetch_stock_hk_spot_em() -> pd.DataFrame:
    """获取港股实时行情 - 东方财富"""
    return ak.stock_hk_spot_em()


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_stock_hk_spot_sina() -> pd.DataFrame:
    """获取港股实时行情 - 新浪备用（可能返回非 JSON 导致 Expecting value 错误）"""
    df = ak.stock_hk_spot()
    return _normalize_sina_hk_spot(df)


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_stock_hk_hot_rank() -> pd.DataFrame:
    """
    获取港股人气榜（东方财富），用于快速搜索热门港股，避免每次都拉全市场大表。
    """
    df = ak.stock_hk_hot_rank_em()
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    # 统一列名：代码 / 名称 / 最新价 / 涨跌幅
    rename_map = {}
    if "股票名称" in df.columns and "名称" not in df.columns:
        rename_map["股票名称"] = "名称"
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_stock_hk_spot_main_board_em() -> pd.DataFrame:
    """获取港股实时行情 - 东财主板备用（与 spot_em 列名一致）"""
    return ak.stock_hk_main_board_spot_em()


def _fetch_stock_hk_spot() -> pd.DataFrame:
    """
    获取港股实时行情（多数据源：新浪 -> 东财全市场 -> 东财主板）
    全部失败时返回空 DataFrame，不抛错，由调用方提示友好文案。
    """
    return _try_multiple_sources(
        [
            _fetch_stock_hk_spot_sina,
            _fetch_stock_hk_spot_em,
            _fetch_stock_hk_spot_main_board_em,
        ],
        delay=0.6,
    )


def _get_stock_hk_spot_cached() -> pd.DataFrame:
    """
    带进程内缓存的港股全市场行情。

    - 成功获取一次后，在 TTL 内后续调用都会直接复用缓存，避免频繁拉取大表导致超时
    - 全部数据源都失败时不写入缓存，方便下次重试
    """
    global _HK_SPOT_CACHE_DF, _HK_SPOT_CACHE_TS

    now = time.time()
    if _HK_SPOT_CACHE_DF is not None and _HK_SPOT_CACHE_TS is not None:
        if now - _HK_SPOT_CACHE_TS < HK_SPOT_CACHE_TTL:
            return _HK_SPOT_CACHE_DF

    df = _fetch_stock_hk_spot()
    if df is not None and not df.empty:
        _HK_SPOT_CACHE_DF = df
        _HK_SPOT_CACHE_TS = now
    return df


@retry_on_network_error(max_retries=3, base_delay=1.5)
def _fetch_stock_hk_history(**kwargs) -> pd.DataFrame:
    """获取港股历史数据"""
    return ak.stock_hk_hist(**kwargs)


@tool
def get_stock_hk_realtime(symbol: str) -> str:
    """
    获取港股实时行情。

    Args:
        symbol: 港股代码，如 "00700"(腾讯控股), "09988"(阿里巴巴-SW), "02015"(理想汽车-W)

    Returns:
        港股的实时行情信息，包括最新价、涨跌幅、成交量等
    """
    try:
        # 标准化港股代码（5位数字）
        symbol = symbol.strip().replace("HK", "").replace(".", "")
        if len(symbol) < 5:
            symbol = symbol.zfill(5)

        # 获取全市场数据（多源 + 进程内缓存，失败不抛错）
        df = _get_stock_hk_spot_cached()

        if df.empty:
            return (
                "暂时无法获取港股行情（数据源暂时不可用）。\n\n"
                "请稍后重试，或使用 5 位港股代码直接查历史：如 00700(腾讯)、09988(阿里)。"
            )

        # 查找指定股票（代码列可能为数值，统一转字符串比较）
        code_col = "代码" if "代码" in df.columns else None
        if not code_col:
            return "暂时无法获取港股行情（数据列异常），请稍后重试。"
        code_ser = df[code_col].astype(str).str.replace(r"\D", "", regex=True).str.zfill(5)
        stock_data = df[code_ser == symbol]

        if stock_data.empty:
            return f"未找到港股代码 {symbol} 的数据\n\n提示：请使用5位数港股代码，如 00700(腾讯)、09988(阿里)"

        row = stock_data.iloc[0]

        output = f"港股 {symbol} 实时行情:\n"
        output += f"  股票代码: {row.get('代码', symbol)}\n"
        output += f"  股票名称: {row.get('名称', 'N/A')}\n"
        output += f"  最新价: {row.get('最新价', 'N/A')}\n"
        output += f"  涨跌额: {row.get('涨跌额', 'N/A')}\n"
        output += f"  涨跌幅: {row.get('涨跌幅', 'N/A')}%\n"
        output += f"  今开: {row.get('今开', 'N/A')}\n"
        output += f"  最高: {row.get('最高', 'N/A')}\n"
        output += f"  最低: {row.get('最低', 'N/A')}\n"
        output += f"  昨收: {row.get('昨收', 'N/A')}\n"
        output += f"  成交量: {row.get('成交量', 'N/A')}\n"
        output += f"  成交额: {row.get('成交额', 'N/A')}\n"

        return output
    except Exception as e:
        return f"获取港股实时行情失败: {str(e)[:200]}"


@tool
def get_stock_hk_history(
    symbol: str,
    start_date: str = "20240101",
    end_date: str = "",
    period: str = "daily",
    adjust: str = "qfq",
) -> str:
    """
    获取港股历史行情数据。

    Args:
        symbol: 港股代码，如 "00700"
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD
        period: 周期，可选 "daily"(日), "weekly"(周), "monthly"(月)
        adjust: 复权类型，"qfq"(前复权), "hfq"(后复权), ""(不复权)

    Returns:
        港股历史K线数据
    """
    try:
        # 标准化港股代码
        symbol = symbol.strip().replace("HK", "").replace(".", "")
        if len(symbol) < 5:
            symbol = symbol.zfill(5)

        kwargs = {
            "symbol": symbol,
            "period": period,
            "start_date": start_date.replace("-", ""),
            "adjust": adjust,
        }

        if end_date:
            kwargs["end_date"] = end_date.replace("-", "")

        df = _fetch_stock_hk_history(**kwargs)

        if df.empty:
            return f"未找到港股 {symbol} 的历史数据"

        return f"港股 {symbol} 历史行情 ({period}):\n\n{format_dataframe(df)}"
    except Exception as e:
        return f"获取港股历史行情失败: {str(e)[:200]}"


@tool
def search_stock_hk(keyword: str) -> str:
    """
    搜索港股股票。

    Args:
        keyword: 搜索关键词，可以是公司名称或代码的一部分

    Returns:
        匹配的港股列表
    """
    try:
        kw = (keyword or "").strip()
        if not kw:
            return "请输入港股搜索关键词，例如公司名称的一部分或 5 位代码，如 00700、腾讯 等。"

        # 1) 优先在港股人气榜中搜索（小表，速度快，覆盖大部分常用热门股）
        hot_df = _fetch_stock_hk_hot_rank()
        if hot_df is not None and not hot_df.empty:
            if "代码" in hot_df.columns and "名称" in hot_df.columns:
                hot_codes = hot_df["代码"].astype(str)
                hot_names = hot_df["名称"].astype(str)
                hot_mask = hot_codes.str.contains(kw, case=False, na=False) | hot_names.str.contains(kw, case=False, na=False)
                hot_result = hot_df.loc[
                    hot_mask,
                    [c for c in ["代码", "名称", "最新价", "涨跌幅"] if c in hot_df.columns],
                ].head(20)
                if not hot_result.empty:
                    return f"（来自人气榜）搜索 '{kw}' 的港股结果（前20个）:\n\n{format_dataframe(hot_result)}"

        # 2) 热门股未命中时，再尝试全市场大表（带总超时保护）
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_get_stock_hk_spot_cached)
            try:
                df = future.result(timeout=HK_STOCK_SEARCH_TIMEOUT)
            except FutureTimeoutError:
                return (
                    "搜索港股超时，数据源响应过慢或网络不稳定。\n\n"
                    "建议：\n"
                    "- 直接使用 5 位数港股代码查询，例如 00700(腾讯)、09988(阿里)、01810(小米)；\n"
                    "- 或稍后重试。"
                )

        if df is None or df.empty:
            return (
                "无法获取港股列表数据（数据源暂时不可用）。\n\n"
                "提示: 请使用 5 位数港股代码直接查询，例如:\n"
                "  - 腾讯控股: 00700\n"
                "  - 阿里巴巴: 09988\n"
                "  - 小米集团: 01810"
            )

        # 确保存在用于搜索的列并转为字符串，避免数值列 .str 报错
        if "代码" not in df.columns or "名称" not in df.columns:
            cols_preview = ", ".join([str(c) for c in list(df.columns)[:8]])
            return (
                "当前港股列表数据列名异常，无法完成搜索。\n\n"
                f"部分列名: {cols_preview}\n\n"
                "建议: 直接使用 5 位数港股代码查询，例如 00700、09988、01810。"
            )

        codes = df["代码"].astype(str)
        names = df["名称"].astype(str)

        # 搜索匹配（支持代码和名称，忽略大小写）
        mask = codes.str.contains(kw, case=False, na=False) | names.str.contains(kw, case=False, na=False)
        result_df = df.loc[mask, ["代码", "名称", "最新价", "涨跌幅"]].head(20)

        if not result_df.empty:
            return f"搜索 '{kw}' 的港股结果（前20个）:\n\n{format_dataframe(result_df)}"

        return (
            f"未找到与 '{kw}' 相关的港股。\n\n"
            f"提示: 请使用 5 位数港股代码查询，例如:\n"
            f"  - 腾讯控股: 00700\n"
            f"  - 阿里巴巴: 09988\n"
            f"  - 小米集团: 01810"
        )

    except Exception as e:
        return f"搜索港股失败: {str(e)[:200]}\n\n建议直接使用港股代码查询"
