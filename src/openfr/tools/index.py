"""
Index data tools based on AKShare.
"""

import akshare as ak
import pandas as pd
from langchain_core.tools import tool
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

from openfr.tools.base import format_dataframe, retry_on_network_error

# 单次请求超时（秒），避免「卡很久后失败」；偏小以快速切换数据源
INDEX_FETCH_TIMEOUT = 5
INDEX_SPOT_TOTAL_TIMEOUT = 10


def _run_with_timeout(func, timeout: float, default: pd.DataFrame) -> pd.DataFrame:
    """在子线程中执行 func()，超时则返回 default，避免卡在「获取指数实时行情」"""
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(func)
            return fut.result(timeout=timeout)
    except (FuturesTimeoutError, Exception):
        return default


def try_multiple_sources_silent(
    fetch_functions: list, delay: float = 1.0, per_call_timeout: float = 0
) -> pd.DataFrame:
    """
    静默尝试多个数据源接口。per_call_timeout>0 时对每次调用做超时限制，避免卡死。
    """
    for i, fetch_func in enumerate(fetch_functions):
        try:
            if i > 0:
                time.sleep(min(delay, 0.8))  # 源间延迟上限 0.8s，加快切换
            if per_call_timeout > 0:
                result = _run_with_timeout(fetch_func, per_call_timeout, pd.DataFrame())
            else:
                result = fetch_func()
            if not result.empty:
                return result
        except Exception:
            continue
    return pd.DataFrame()


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_index_hist_for_symbol(symbol: str, days: int = 5) -> pd.DataFrame:
    """拉取单只指数近期日线（带重试），供实时/降级使用"""
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=max(1, days))).strftime("%Y%m%d")
    return ak.index_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start_date,
        end_date=end_date,
    )


def _fetch_index_history_as_realtime(symbol: str) -> pd.DataFrame:
    """
    用历史日线拼「最新」当实时；仅用新浪/腾讯，避免东财接口触发 PyMiniRacer 崩溃。
    """
    df = _fetch_index_hist_sina_or_tx(symbol, days=5)
    if not df.empty:
        latest = df.tail(1).copy()
        latest["代码"] = symbol
        latest["名称"] = _get_index_name(symbol)
        if "收盘" in latest.columns and "最新价" not in latest.columns:
            latest["最新价"] = latest["收盘"]
        return latest
    # 不再调用东财 index_zh_a_hist，避免部分环境下 libmini_racer 崩溃
    return pd.DataFrame()


def _get_index_name(symbol: str) -> str:
    """获取指数名称"""
    index_names = {
        "000001": "上证指数",
        "399001": "深证成指",
        "399006": "创业板指",
        "000688": "科创50",
        "000300": "沪深300",
        "000905": "中证500",
        "000852": "中证1000",
    }
    return index_names.get(symbol, f"指数{symbol}")


def _index_symbol_to_sina_tx(symbol: str) -> str:
    """6 位指数代码转为新浪/腾讯格式：000001->sh000001, 399001->sz399001"""
    s = symbol.strip()
    if s.startswith("399") or s.startswith("2") or s.startswith("1"):
        return f"sz{s}" if len(s) >= 5 else s
    return f"sh{s}" if len(s) >= 5 else s


def _fetch_index_hist_sina_or_tx(symbol: str, days: int = 10) -> pd.DataFrame:
    """指数日线备用：新浪或腾讯（东财 index_zh_a_hist 失败时用）"""
    sym = _index_symbol_to_sina_tx(symbol)
    for fetch in (ak.stock_zh_index_daily, ak.stock_zh_index_daily_tx):
        try:
            df = fetch(symbol=sym)
            if df is not None and not df.empty:
                # 统一列名：新浪/腾讯可能用 date, open, close, high, low, volume
                df = df.tail(days).copy()
                if "date" in df.columns:
                    df["日期"] = df["date"]
                if "close" in df.columns:
                    df["收盘"] = df["close"]
                if "open" in df.columns:
                    df["开盘"] = df["open"]
                if "high" in df.columns:
                    df["最高"] = df["high"]
                if "low" in df.columns:
                    df["最低"] = df["low"]
                if "volume" in df.columns:
                    df["成交量"] = df["volume"]
                df["代码"] = symbol
                df["名称"] = _get_index_name(symbol)
                df["最新价"] = df["收盘"]
                df["涨跌幅"] = df.get("涨跌幅", float("nan"))
                df["涨跌额"] = df.get("涨跌额", float("nan"))
                return df
        except Exception:
            continue
    return pd.DataFrame()


@retry_on_network_error(max_retries=2, base_delay=1.0, silent=True)
def _fetch_index_spot_em() -> pd.DataFrame:
    """获取指数实时行情 - 东方财富接口"""
    return ak.stock_zh_index_spot_em()


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_index_spot_sina() -> pd.DataFrame:
    """获取指数实时行情 - 新浪接口（AKShare 导出名为 stock_zh_index_spot_sina）"""
    return ak.stock_zh_index_spot_sina()


def _fetch_major_indices_fallback() -> pd.DataFrame:
    """
    降级方案：使用历史数据接口逐只查询主要指数（带重试，多试几只）
    """
    major_indices = [
        ("000001", "上证指数"),
        ("399001", "深证成指"),
        ("399006", "创业板指"),
        ("000300", "沪深300"),
        ("000688", "科创50"),
    ]
    all_data = []
    for symbol, _ in major_indices:
        try:
            df = _fetch_index_history_as_realtime(symbol)
            if not df.empty:
                all_data.append(df)
        except Exception:
            continue
        time.sleep(0.2)
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def _format_index_spot_df_as_realtime(df: pd.DataFrame) -> str:
    """将全市场指数 spot 或降级历史表格式化为「主要指数行情」文案"""
    major_codes = {"000001", "399001", "399006", "000300", "000688"}
    code_col = "代码" if "代码" in df.columns else None
    if code_col:
        raw = df[code_col].astype(str)
        code_clean = raw.str.replace(r"\D", "", regex=True)
        mask = code_clean.isin(major_codes) | code_clean.str[-6:].isin(major_codes)
        subset = df.loc[mask]
        if not subset.empty:
            df = subset
    if df.empty:
        return ""
    out = "主要指数行情:\n\n"
    name_col = "名称" if "名称" in df.columns else None
    price_col = "最新价" if "最新价" in df.columns else "收盘"
    pct_col = "涨跌幅" if "涨跌幅" in df.columns else None
    high_col = "最高" if "最高" in df.columns else None
    low_col = "最低" if "最低" in df.columns else None
    vol_col = "成交量" if "成交量" in df.columns else None
    date_col = "日期" if "日期" in df.columns else None
    for _, row in df.head(10).iterrows():
        name = row.get("名称", row.get(name_col, row.get("代码", "—")))
        price = row.get(price_col, row.get("收盘", "—"))
        pct = row.get(pct_col, "")
        if pct is not None and pct != "" and pct != "—":
            try:
                pct = f"{float(pct):.2f}%"
            except (TypeError, ValueError):
                pct = str(pct)
        else:
            pct = "—"
        out += f"【{name}】\n"
        out += f"  最新/收盘: {price}  涨跌幅: {pct}\n"
        if date_col and date_col in row:
            out += f"  日期: {row[date_col]}\n"
        if high_col and high_col in row and pd.notna(row.get(high_col)):
            out += f"  最高/最低: {row.get(high_col)} / {row.get(low_col, '—')}\n"
        if vol_col and vol_col in row and pd.notna(row.get(vol_col)):
            out += f"  成交量: {row.get(vol_col)}\n"
        out += "\n"
    out += "💡 数据来自全市场接口或历史日线\n"
    return out


def _fetch_index_spot() -> pd.DataFrame:
    """
    获取指数实时行情（串行 + 降级策略）

    策略：
    1. 直接调用新浪接口 stock_zh_index_spot_sina（不再额外开线程）
    2. 若失败或为空，降级为使用历史数据逐只拼“最新”

    说明：
    - 不再在此处使用 ThreadPoolExecutor，以减少与 AKShare 内部可能使用的
      libmini_racer/py_mini_racer 的线程交互，避免 address_pool_manager 崩溃。
    """
    try:
        result = _fetch_index_spot_sina()
        if result is not None and not result.empty:
            return result
    except Exception:
        result = pd.DataFrame()

    # 降级：逐只指数拼最新
    try:
        return _fetch_major_indices_fallback()
    except Exception:
        return pd.DataFrame()


@retry_on_network_error(max_retries=3, base_delay=1.0)
def _fetch_index_history(**kwargs) -> pd.DataFrame:
    """获取指数历史行情（带重试）"""
    return ak.index_zh_a_hist(**kwargs)


def _fetch_single_index_latest(symbol: str) -> pd.DataFrame:
    """获取单个指数最新一条（先新浪/腾讯日线再东财，减少卡顿）"""
    return _fetch_index_history_as_realtime(symbol)


def _msg_index_fallback_help() -> str:
    return (
        "❌ 无法获取指数行情数据\n\n"
        "📊 主要指数代码参考：\n"
        "  • 上证指数: 000001\n"
        "  • 深证成指: 399001\n"
        "  • 创业板指: 399006\n"
        "  • 科创50: 000688\n"
        "  • 沪深300: 000300\n\n"
        "💡 建议：使用 get_index_history 查询具体指数\n"
        "⏰ 交易时间：工作日 9:30-15:00"
    )


@tool
def get_index_realtime() -> str:
    """
    获取主要指数实时行情。

    Returns:
        主要指数（上证指数、深证成指、创业板指等）的实时行情
    """
    try:
        major_indices = [
            ("000001", "上证指数"),
            ("399001", "深证成指"),
            ("399006", "创业板指"),
        ]

        def _fetch_one(symbol: str, name: str):
            df = _fetch_single_index_latest(symbol)
            if df is None or df.empty:
                return None
            latest = df.tail(1).iloc[0]
            return {
                "代码": symbol,
                "名称": name,
                "日期": latest.get("日期", ""),
                "收盘价": latest.get("收盘", ""),
                "涨跌幅": f"{latest.get('涨跌幅', 0):.2f}%",
                "最高": latest.get("最高", ""),
                "最低": latest.get("最低", ""),
                "成交量": latest.get("成交量", ""),
            }

        # 三只指数串行拉取，避免额外线程与 JS 运行环境交叉
        all_data = []
        for sym, name in major_indices:
            try:
                info = _fetch_one(sym, name)
                if info:
                    all_data.append(info)
            except Exception:
                continue
        # 按固定顺序排列
        order = {t[0]: i for i, t in enumerate(major_indices)}
        all_data.sort(key=lambda x: order.get(x["代码"], 99))

        if all_data:
            output = "主要指数行情（最新交易日数据）:\n\n"
            for info in all_data:
                output += f"【{info['名称']}】({info['代码']})\n"
                output += f"  日期: {info['日期']}\n"
                output += f"  收盘: {info['收盘价']}\n"
                output += f"  涨跌幅: {info['涨跌幅']}\n"
                output += f"  最高/最低: {info['最高']} / {info['最低']}\n"
                output += f"  成交量: {info['成交量']}\n\n"
            output += "💡 提示: 这是最新交易日的收盘数据\n"
            output += "⏰ 交易时间: 工作日 9:30-15:00"
            return output

        # 逐只历史仍全部失败时：尝试全市场 spot（东方财富→新浪）或历史降级（带总超时）
        spot_df = _run_with_timeout(
            _fetch_index_spot,
            INDEX_SPOT_TOTAL_TIMEOUT,
            pd.DataFrame(),
        )
        if not spot_df.empty:
            formatted = _format_index_spot_df_as_realtime(spot_df)
            if formatted:
                return formatted

        # 最后兜底：仅拉取上证指数 000001（仅新浪/腾讯，避免东财触发 PyMiniRacer 崩溃）
        def _last_resort_000001():
            return _fetch_index_hist_sina_or_tx("000001", days=10)

        try:
            df = _run_with_timeout(_last_resort_000001, 6, pd.DataFrame())
            if not df.empty:
                latest = df.tail(1).iloc[0]
                pct = latest.get("涨跌幅", float("nan"))
                pct_str = f"{float(pct):.2f}%" if pd.notna(pct) else "—"
                out = "主要指数行情（上证指数，最新交易日）:\n\n"
                out += "【上证指数】(000001)\n"
                out += f"  日期: {latest.get('日期', '')}\n"
                out += f"  收盘: {latest.get('收盘', '')}\n"
                out += f"  涨跌幅: {pct_str}\n"
                out += f"  最高/最低: {latest.get('最高', '')} / {latest.get('最低', '')}\n"
                out += f"  成交量: {latest.get('成交量', '')}\n\n"
                out += "💡 仅获取到上证指数，其他指数请用 get_index_history 查询\n"
                out += "⏰ 交易时间: 工作日 9:30-15:00"
                return out
        except Exception:
            pass

        return _msg_index_fallback_help()
    except Exception:
        return _msg_index_fallback_help()


@tool
def get_index_history(
    symbol: str = "000001", start_date: str = "", end_date: str = "", period: str = "daily"
) -> str:
    """
    获取指数历史行情。

    Args:
        symbol: 指数代码，如 "000001"(上证指数), "399001"(深证成指), "399006"(创业板指)
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD
        period: 周期，可选 "daily", "weekly", "monthly"

    Returns:
        指数历史K线数据
    """
    try:
        kwargs = {"symbol": symbol, "period": period}

        if start_date:
            kwargs["start_date"] = start_date.replace("-", "")
        if end_date:
            kwargs["end_date"] = end_date.replace("-", "")

        df = _fetch_index_history(**kwargs)

        if df.empty:
            return f"未找到指数 {symbol} 的历史数据"

        return f"指数 {symbol} 历史行情 ({period}):\n\n{format_dataframe(df)}"
    except Exception as e:
        return f"获取指数历史数据失败: {str(e)[:200]}"
