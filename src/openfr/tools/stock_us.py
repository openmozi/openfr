"""
US stock data tools based on AKShare.

数据源策略：
- 实时/列表：优先东方财富 stock_us_spot_em，失败时使用新浪 stock_us_spot 备用
- 历史：优先东方财富 stock_us_hist，失败时使用新浪 stock_us_daily 备用
"""

import akshare as ak
import pandas as pd
from langchain_core.tools import tool
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

from openfr.tools.base import format_dataframe, retry_on_network_error

# 美股全市场行情缓存（减少重复拉取导致的超时）
US_SPOT_CACHE_TTL = 600.0  # 秒
_US_SPOT_CACHE_DF: pd.DataFrame | None = None
_US_SPOT_CACHE_TS: float | None = None

# 搜索类操作总超时时间，避免在网络异常时挂住一整轮（适当偏小以保证交互流畅）
US_STOCK_SEARCH_TIMEOUT = 4.0


def _normalize_sina_us_spot(df: pd.DataFrame) -> pd.DataFrame:
    """
    将新浪美股 spot 的列名统一为与东方财富一致的格式，便于后续复用同一套展示逻辑。
    新浪返回列可能包含 symbol, name, cname 等，无最新价时仅用于搜索。
    """
    if df.empty:
        return df
    # 东方财富格式: 代码, 名称, 最新价, 涨跌幅, 涨跌额, 开盘价, 最高价, 最低价, 昨收价, 成交量, 成交额
    if "代码" in df.columns and "名称" in df.columns:
        return df
    out = pd.DataFrame()
    if "symbol" in df.columns:
        out["代码"] = df["symbol"].astype(str)
    elif "Symbol" in df.columns:
        out["代码"] = df["Symbol"].astype(str)
    else:
        return df
    if "cname" in df.columns:
        out["名称"] = df["cname"].astype(str)
    elif "name" in df.columns:
        out["名称"] = df["name"].astype(str)
    elif "Name" in df.columns:
        out["名称"] = df["Name"].astype(str)
    else:
        out["名称"] = out["代码"]
    for col, em_col in [
        ("price", "最新价"),
        ("Price", "最新价"),
        ("latest", "最新价"),
        ("change", "涨跌额"),
        ("pct", "涨跌幅"),
        ("percent", "涨跌幅"),
    ]:
        if col in df.columns and em_col not in out.columns:
            out[em_col] = pd.to_numeric(df[col], errors="coerce")
    if "最新价" not in out.columns:
        out["最新价"] = float("nan")
    if "涨跌幅" not in out.columns:
        out["涨跌幅"] = float("nan")
    if "涨跌额" not in out.columns:
        out["涨跌额"] = float("nan")
    return out


def _normalize_us_symbol(symbol: str) -> str:
    """
    标准化美股代码。

    美股代码需要交易所前缀：
    - 105 = NASDAQ (纳斯达克)
    - 106 = NYSE (纽约证券交易所)
    - 107 = 其他交易所

    Args:
        symbol: 用户输入的代码，如 "AAPL" 或 "105.AAPL"

    Returns:
        带前缀的代码，如 "105.AAPL"
    """
    symbol = symbol.strip().upper()

    # 如果已经有前缀，直接返回
    if '.' in symbol and symbol.split('.')[0] in ['105', '106', '107']:
        return symbol

    # 否则尝试添加 NASDAQ 前缀（大多数科技股在 NASDAQ）
    return f"105.{symbol}"


def try_multiple_exchanges(symbol: str, fetch_func, **kwargs) -> pd.DataFrame:
    """
    尝试多个交易所前缀。

    Args:
        symbol: 股票代码
        fetch_func: 数据获取函数
        **kwargs: 其他参数

    Returns:
        成功获取的 DataFrame
    """
    # 如果已经有前缀，直接尝试
    if '.' in symbol:
        try:
            return fetch_func(symbol=symbol, **kwargs)
        except Exception:
            return pd.DataFrame()

    # 否则尝试不同的交易所前缀
    prefixes = ['105', '106', '107']  # NASDAQ, NYSE, 其他

    for prefix in prefixes:
        try:
            prefixed_symbol = f"{prefix}.{symbol}"
            result = fetch_func(symbol=prefixed_symbol, **kwargs)
            if not result.empty:
                return result
        except Exception:
            continue

    return pd.DataFrame()


def try_multiple_sources(fetch_functions: list, delay: float = 1.0) -> pd.DataFrame:
    """
    依次尝试多个数据源，返回第一个成功且非空的结果。
    用于东方财富连接被断开时自动切换新浪等备用源。
    """
    last_error = None
    for i, fetch_func in enumerate(fetch_functions):
        try:
            if i > 0:
                time.sleep(delay)
            result = fetch_func()
            if result is not None and not result.empty:
                return result
        except Exception as e:
            last_error = e
            continue
    if last_error is not None:
        raise last_error
    return pd.DataFrame()


@retry_on_network_error(max_retries=2, base_delay=1.0, silent=True)
def _fetch_stock_us_spot_em() -> pd.DataFrame:
    """获取美股实时行情 - 东方财富"""
    return ak.stock_us_spot_em()


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_stock_us_spot_sina() -> pd.DataFrame:
    """获取美股实时行情 - 新浪备用"""
    df = ak.stock_us_spot()
    return _normalize_sina_us_spot(df)


def _fetch_stock_us_spot() -> pd.DataFrame:
    """
    获取美股实时行情（多数据源：新浪 -> 东方财富）

    说明：
    - 新浪接口通常更轻、更稳定，适合作为优先数据源以提升搜索/首次加载速度
    - 东方财富作为备用源，在新浪不可用时补充更完整的数据
    """
    return try_multiple_sources(
        [
            _fetch_stock_us_spot_sina,  # 优先新浪
            _fetch_stock_us_spot_em,    # 东方财富备用
        ],
        delay=1.0,
    )


def _get_stock_us_spot_cached() -> pd.DataFrame:
    """
    带进程内缓存的美股全市场行情。

    - 成功获取一次后，在 TTL 内后续调用都会直接复用缓存，避免频繁拉取大表导致超时
    - 全部数据源都失败时不写入缓存，方便下次重试
    """
    global _US_SPOT_CACHE_DF, _US_SPOT_CACHE_TS

    now = time.time()
    if _US_SPOT_CACHE_DF is not None and _US_SPOT_CACHE_TS is not None:
        if now - _US_SPOT_CACHE_TS < US_SPOT_CACHE_TTL:
            return _US_SPOT_CACHE_DF

    df = _fetch_stock_us_spot()
    if df is not None and not df.empty:
        _US_SPOT_CACHE_DF = df
        _US_SPOT_CACHE_TS = now
    return df


@retry_on_network_error(max_retries=3, base_delay=1.5)
def _fetch_stock_us_history(**kwargs) -> pd.DataFrame:
    """获取美股历史数据 - 东方财富"""
    return ak.stock_us_hist(**kwargs)


def _fetch_stock_us_history_sina(symbol: str, **kwargs) -> pd.DataFrame:
    """获取美股历史数据 - 新浪备用（symbol 为纯代码如 AAPL）"""
    try:
        adjust = kwargs.get("adjust", "")
        return ak.stock_us_daily(symbol=symbol, adjust=adjust)
    except Exception:
        return pd.DataFrame()


@tool
def get_stock_us_realtime(symbol: str) -> str:
    """
    获取美股实时行情。

    Args:
        symbol: 美股代码，如 "AAPL"(苹果), "TSLA"(特斯拉), "NVDA"(英伟达)

    Returns:
        美股的实时行情信息，包括最新价、涨跌幅、成交量等
    """
    try:
        # 标准化代码
        symbol_normalized = _normalize_us_symbol(symbol)

        # 获取全市场数据（多源 + 进程内缓存，失败不抛错）
        df = _get_stock_us_spot_cached()

        if df.empty:
            return f"未找到美股代码 {symbol} 的数据"

        # 查找指定股票（尝试多种可能的列名）
        code_col = None
        for col in ['代码', '股票代码', 'symbol']:
            if col in df.columns:
                code_col = col
                break

        if not code_col:
            return "数据格式错误：找不到代码列"

        # 尝试原始输入和标准化代码
        stock_data = df[df[code_col] == symbol_normalized]
        if stock_data.empty:
            # 尝试去掉前缀
            stock_data = df[df[code_col].str.contains(symbol.upper(), na=False)]

        if stock_data.empty:
            # 东方财富/新浪列表都没有时，尝试用新浪单只历史取最近收盘作为“最新”参考
            try:
                hist = ak.stock_us_daily(symbol=symbol.strip().upper(), adjust="")
                if not hist.empty:
                    last = hist.iloc[-1]
                    return (
                        f"美股 {symbol.upper()} 最近行情（新浪，非实时）:\n"
                        f"  股票代码: {symbol.upper()}\n"
                        f"  最新收盘: ${last.get('close', 'N/A')}\n"
                        f"  开盘: ${last.get('open', 'N/A')}\n"
                        f"  最高: ${last.get('high', 'N/A')}\n"
                        f"  最低: ${last.get('low', 'N/A')}\n"
                        f"  成交量: {last.get('volume', 'N/A')}\n"
                        f"  日期: {hist.index[-1]}\n"
                        f"（数据来源：新浪，非实时；若需实时请稍后重试东方财富接口）"
                    )
            except Exception:
                pass
            return f"未找到美股代码 {symbol} 的数据\n\n提示：请使用美股代码，如 AAPL(苹果)"

        row = stock_data.iloc[0]
        latest = row.get("最新价", row.get("最新价", "N/A"))
        if pd.isna(latest) or (isinstance(latest, float) and latest != latest):
            try:
                hist = ak.stock_us_daily(symbol=symbol.strip().upper(), adjust="")
                if not hist.empty:
                    last = hist.iloc[-1]
                    latest = f"{last.get('close', 'N/A')}（新浪最近收盘，非实时）"
            except Exception:
                latest = "N/A（暂无实时价，请查历史）"

        output = f"美股 {symbol.upper()} 实时行情:\n"
        output += f"  股票代码: {row.get('代码', row.get(code_col, symbol))}\n"
        output += f"  股票名称: {row.get('名称', row.get('股票名称', 'N/A'))}\n"
        output += f"  最新价: ${latest}\n"
        output += f"  涨跌额: ${row.get('涨跌额', 'N/A')}\n"
        output += f"  涨跌幅: {row.get('涨跌幅', 'N/A')}%\n"
        output += f"  开盘价: ${row.get('开盘价', row.get('开盘', 'N/A'))}\n"
        output += f"  最高价: ${row.get('最高价', row.get('最高', 'N/A'))}\n"
        output += f"  最低价: ${row.get('最低价', row.get('最低', 'N/A'))}\n"
        output += f"  昨收价: ${row.get('昨收价', row.get('昨收', 'N/A'))}\n"
        output += f"  成交量: {row.get('成交量', 'N/A')}\n"

        return output
    except Exception as e:
        return f"获取美股实时行情失败: {str(e)[:200]}"


@tool
def get_stock_us_history(
    symbol: str,
    start_date: str = "20240101",
    end_date: str = "",
    period: str = "daily",
) -> str:
    """
    获取美股历史行情数据。

    Args:
        symbol: 美股代码，如 "AAPL"
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD
        period: 周期，默认 "daily"(日)

    Returns:
        美股历史K线数据
    """
    try:
        # 标准化代码
        symbol_upper = symbol.strip().upper()

        kwargs = {
            "period": period,
            "start_date": start_date.replace("-", ""),
            "adjust": "",
        }

        if end_date:
            kwargs["end_date"] = end_date.replace("-", "")

        # 尝试多个交易所前缀（东方财富）
        df = try_multiple_exchanges(symbol_upper, _fetch_stock_us_history, **kwargs)

        # 东方财富失败时用新浪历史接口（纯代码如 AAPL）
        if df.empty:
            df = _fetch_stock_us_history_sina(symbol_upper, **kwargs)
            if not df.empty:
                # 新浪返回的列名可能是 date/open/high/low/close/volume，直接展示
                return f"美股 {symbol.upper()} 历史行情 ({period}，新浪):\n\n{format_dataframe(df)}"

        if df.empty:
            return f"未找到美股 {symbol} 的历史数据\n\n提示：请确认代码正确，如 AAPL(苹果)、TSLA(特斯拉)"

        return f"美股 {symbol.upper()} 历史行情 ({period}):\n\n{format_dataframe(df)}"
    except Exception as e:
        return f"获取美股历史行情失败: {str(e)[:200]}"


@tool
def search_stock_us(keyword: str) -> str:
    """
    搜索美股股票。

    Args:
        keyword: 搜索关键词，可以是公司名称或代码的一部分

    Returns:
        匹配的美股列表
    """
    try:
        kw = (keyword or "").strip()
        if not kw:
            return "请输入美股搜索关键词，例如公司名称的一部分或代码，如 AAPL、MSFT、TSLA 等。"

        # 使用实时行情接口获取全市场股票列表（带总超时保护 + 进程内缓存）
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_get_stock_us_spot_cached)
            try:
                df = future.result(timeout=US_STOCK_SEARCH_TIMEOUT)
            except FutureTimeoutError:
                return (
                    "搜索美股超时，数据源响应过慢或网络不稳定。\n\n"
                    "建议：\n"
                    "- 直接使用美股代码查询，例如 AAPL(苹果)、MSFT(微软)、TSLA(特斯拉)；\n"
                    "- 或稍后重试。"
                )

        if df.empty:
            return (
                f"无法获取美股列表数据。\n\n"
                f"提示: 请使用美股代码直接查询，例如:\n"
                f"  - 苹果: AAPL\n"
                f"  - 微软: MSFT\n"
                f"  - 特斯拉: TSLA"
            )

        # 确定代码和名称列
        code_col = None
        name_col = None

        for col in ['代码', '股票代码', 'symbol']:
            if col in df.columns:
                code_col = col
                break

        for col in ['名称', '股票名称', 'name']:
            if col in df.columns:
                name_col = col
                break

        if not code_col or not name_col:
            return (
                f"数据格式错误。\n\n"
                f"提示: 请使用美股代码直接查询，例如:\n"
                f"  - 苹果: AAPL\n"
                f"  - 微软: MSFT\n"
                f"  - 特斯拉: TSLA"
            )

        # 将用于搜索的列统一转为字符串，避免数值列 .str 报错
        codes = df[code_col].astype(str)
        names = df[name_col].astype(str)

        # 搜索匹配（支持代码和名称）
        mask = codes.str.contains(kw, case=False, na=False) | names.str.contains(kw, case=False, na=False)

        # 选择要显示的列
        display_cols = [code_col, name_col]
        if "最新价" in df.columns:
            display_cols.append("最新价")
        if "涨跌幅" in df.columns:
            display_cols.append("涨跌幅")

        result_df = df[mask][display_cols].head(20)

        if not result_df.empty:
            return f"搜索 '{keyword}' 的美股结果（前20个）:\n\n{format_dataframe(result_df)}"
        else:
            return (
                f"未找到与 '{keyword}' 相关的美股。\n\n"
                f"提示: 请使用美股代码查询，例如:\n"
                f"  - 苹果: AAPL\n"
                f"  - 微软: MSFT\n"
                f"  - 特斯拉: TSLA"
            )

    except Exception as e:
        return f"搜索美股失败: {str(e)[:200]}\n\n建议直接使用美股代码查询"
