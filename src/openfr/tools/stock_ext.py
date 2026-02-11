"""
A 股扩展数据工具：五档买卖、资金流向、龙虎榜、业绩预告/快报、盈利预测等（AKShare）。
"""

from datetime import datetime, timedelta

import akshare as ak
import pandas as pd
from langchain_core.tools import tool

from openfr.tools.base import format_dataframe, validate_stock_code, validate_date, retry_on_network_error


def _market_for_code(code: str) -> str:
    """6 位 A 股代码 -> 东财市场标识 sh/sz/bj。"""
    if not code or len(code) < 6:
        return "sz"
    if code.startswith("8") or code.startswith("4"):
        return "bj"
    if code.startswith("6") or code.startswith("5") or code.startswith("9"):
        return "sh"
    return "sz"


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _bid_ask(symbol: str) -> pd.DataFrame:
    """东方财富五档买卖盘。symbol 为 6 位代码。"""
    return ak.stock_bid_ask_em(symbol=symbol)


@tool
def get_stock_bid_ask(symbol: str) -> str:
    """
    获取 A 股五档买卖盘、涨跌停价、量比等行情报价。

    Args:
        symbol: 股票代码，如 "000001" 或 "600519"

    Returns:
        买五到卖五档位、最新价、涨跌停、量比、换手等
    """
    try:
        code = validate_stock_code(symbol)
        df = _bid_ask(code)
        if df is None or df.empty:
            return f"股票 {code} 暂无五档行情数据"
        # 转为可读键值对
        rows = df.set_index("item")["value"].to_dict()
        out = f"股票 {code} 五档行情:\n"
        out += f"  最新: {rows.get('最新', 'N/A')}  均价: {rows.get('均价', 'N/A')}  涨跌: {rows.get('涨跌', 'N/A')}  涨幅: {rows.get('涨幅', 'N/A')}\n"
        out += f"  今开: {rows.get('今开', 'N/A')}  昨收: {rows.get('昨收', 'N/A')}  最高: {rows.get('最高', 'N/A')}  最低: {rows.get('最低', 'N/A')}\n"
        out += f"  涨停: {rows.get('涨停', 'N/A')}  跌停: {rows.get('跌停', 'N/A')}  量比: {rows.get('量比', 'N/A')}  换手: {rows.get('换手', 'N/A')}\n"
        out += "  买五 ~ 买一:\n"
        for i in range(5, 0, -1):
            p, v = rows.get(f"buy_{i}", "N/A"), rows.get(f"buy_{i}_vol", "N/A")
            out += f"    买{i}: {p}  量 {v}\n"
        out += "  卖一 ~ 卖五:\n"
        for i in range(1, 6):
            p, v = rows.get(f"sell_{i}", "N/A"), rows.get(f"sell_{i}_vol", "N/A")
            out += f"    卖{i}: {p}  量 {v}\n"
        out += f"  总手: {rows.get('总手', 'N/A')}  金额: {rows.get('金额', 'N/A')}  外盘: {rows.get('外盘', 'N/A')}  内盘: {rows.get('内盘', 'N/A')}\n"
        return out
    except Exception as e:
        return f"获取五档行情失败: {str(e)[:200]}"


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _individual_fund_flow(stock: str, market: str) -> pd.DataFrame:
    """东方财富个股资金流向（日频）。"""
    return ak.stock_individual_fund_flow(stock=stock, market=market)


@tool
def get_stock_fund_flow(symbol: str, limit: int = 10) -> str:
    """
    获取 A 股个股近期资金流向（主力/大单/中单/小单净流入等）。

    Args:
        symbol: 股票代码，如 "600519"
        limit: 返回最近交易日条数，默认 10

    Returns:
        日期、收盘价、涨跌幅、主力/超大单/大单/中单/小单净额与净占比
    """
    try:
        code = validate_stock_code(symbol)
        market = _market_for_code(code)
        df = _individual_fund_flow(code, market)
        if df is None or df.empty:
            return f"股票 {code} 暂无资金流向数据"
        df = df.head(int(limit))
        return f"股票 {code} 近期资金流向（最近 {len(df)} 日）:\n" + format_dataframe(df, max_rows=20)
    except Exception as e:
        return f"获取资金流向失败: {str(e)[:200]}"


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _lhb_detail(start_date: str, end_date: str) -> pd.DataFrame:
    return ak.stock_lhb_detail_em(start_date=start_date, end_date=end_date)


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _lhb_stock_dates(symbol: str) -> pd.DataFrame:
    return ak.stock_lhb_stock_detail_date_em(symbol=symbol)


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _lhb_rank(period: str) -> pd.DataFrame:
    return ak.stock_lhb_stock_statistic_em(symbol=period)


@tool
def get_stock_lhb_detail(start_date: str = "", end_date: str = "", max_rows: int = 30) -> str:
    """
    按日期范围获取龙虎榜明细（上榜股票、净买额、涨跌幅、上榜原因等）。

    Args:
        start_date: 开始日期 YYYYMMDD，默认约 7 天前
        end_date: 结束日期 YYYYMMDD，默认今天
        max_rows: 最多返回行数，默认 30

    Returns:
        龙虎榜明细表
    """
    try:
        end = datetime.now()
        start = end - timedelta(days=7)
        if end_date:
            end = datetime.strptime(validate_date(end_date), "%Y%m%d")
        if start_date:
            start = datetime.strptime(validate_date(start_date), "%Y%m%d")
        if start > end:
            start, end = end, start
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        df = _lhb_detail(start_str, end_str)
        if df is None or df.empty:
            return f"龙虎榜 {start_str}~{end_str} 暂无数据"
        return f"龙虎榜明细 {start_str}~{end_str}:\n" + format_dataframe(df, max_rows=max_rows)
    except Exception as e:
        return f"获取龙虎榜明细失败: {str(e)[:200]}"


@tool
def get_stock_lhb_dates(symbol: str) -> str:
    """
    获取指定股票曾登上龙虎榜的交易日列表。

    Args:
        symbol: 股票代码，如 "600519"

    Returns:
        该股龙虎榜上榜日期列表
    """
    try:
        code = validate_stock_code(symbol)
        df = _lhb_stock_dates(code)
        if df is None or df.empty:
            return f"股票 {code} 近期无龙虎榜上榜记录"
        return f"股票 {code} 龙虎榜上榜日期:\n" + format_dataframe(df, max_rows=50)
    except Exception as e:
        return f"获取龙虎榜日期失败: {str(e)[:200]}"


@tool
def get_stock_lhb_rank(period: str = "近一月", max_rows: int = 25) -> str:
    """
    获取龙虎榜个股上榜统计排行（按净买入额等）。

    Args:
        period: 统计周期，可选 "近一月"、"近三月"、"近六月"、"近一年"
        max_rows: 最多返回条数，默认 25

    Returns:
        龙虎榜统计排行表
    """
    try:
        if period not in ("近一月", "近三月", "近六月", "近一年"):
            period = "近一月"
        df = _lhb_rank(period)
        if df is None or df.empty:
            return f"龙虎榜 {period} 统计暂无数据"
        return f"龙虎榜上榜统计（{period}）:\n" + format_dataframe(df, max_rows=max_rows)
    except Exception as e:
        return f"获取龙虎榜排行失败: {str(e)[:200]}"


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _yjyg(date: str) -> pd.DataFrame:
    return ak.stock_yjyg_em(date=date)


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _yjbb(date: str) -> pd.DataFrame:
    return ak.stock_yjbb_em(date=date)


@tool
def get_stock_yjyg(report_date: str = "", max_rows: int = 30) -> str:
    """
    获取指定报告期的 A 股业绩预告列表（如 20201231 年报、20210930 三季报）。

    Args:
        report_date: 报告期 YYYYMMDD，如 20201231、20230930；默认最近年报期
        max_rows: 最多返回条数，默认 30

    Returns:
        业绩预告列表：代码、简称、预测指标、业绩变动、变动幅度等
    """
    try:
        if report_date:
            date_str = validate_date(report_date)
        else:
            y = datetime.now().year
            if datetime.now().month >= 10:
                date_str = f"{y}0930"
            elif datetime.now().month >= 7:
                date_str = f"{y}0630"
            elif datetime.now().month >= 4:
                date_str = f"{y}0331"
            else:
                date_str = f"{y - 1}1231"
        df = _yjyg(date_str)
        if df is None or df.empty:
            return f"业绩预告（报告期 {date_str}）暂无数据"
        return f"业绩预告（报告期 {date_str}）:\n" + format_dataframe(df, max_rows=max_rows)
    except Exception as e:
        return f"获取业绩预告失败: {str(e)[:200]}"


@tool
def get_stock_yjbb(report_date: str = "", max_rows: int = 30) -> str:
    """
    获取指定报告期的 A 股业绩快报列表（营收、净利润、同比等）。

    Args:
        report_date: 报告期 YYYYMMDD，如 20201231、20230930；默认最近季报期
        max_rows: 最多返回条数，默认 30

    Returns:
        业绩快报表：代码、简称、每股收益、营收、净利润、同比等
    """
    try:
        if report_date:
            date_str = validate_date(report_date)
        else:
            y = datetime.now().year
            if datetime.now().month >= 10:
                date_str = f"{y}0930"
            elif datetime.now().month >= 7:
                date_str = f"{y}0630"
            elif datetime.now().month >= 4:
                date_str = f"{y}0331"
            else:
                date_str = f"{y - 1}1231"
        df = _yjbb(date_str)
        if df is None or df.empty:
            return f"业绩快报（报告期 {date_str}）暂无数据"
        return f"业绩快报（报告期 {date_str}）:\n" + format_dataframe(df, max_rows=max_rows)
    except Exception as e:
        return f"获取业绩快报失败: {str(e)[:200]}"


def _profit_forecast_by_symbol(symbol: str) -> pd.DataFrame | None:
    """单次请求盈利预测，不重试。部分行业（如 白酒）会触发 akshare 内部 NoneType 报错。"""
    try:
        df = ak.stock_profit_forecast_em(symbol=symbol)
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            return None
        return df
    except (TypeError, KeyError, AttributeError, IndexError):
        return None
    except Exception:
        return None


@retry_on_network_error(max_retries=2, base_delay=1.0, silent=True)
def _fetch_profit_forecast_em(symbol: str) -> pd.DataFrame | None:
    """带重试的盈利预测请求（建议 symbol='' 取全部）。"""
    return _profit_forecast_by_symbol(symbol)


@tool
def get_stock_profit_forecast(industry: str = "", max_rows: int = 25) -> str:
    """
    获取机构盈利预测（一致预期）。可按行业筛选或全部。

    Args:
        industry: 行业名称，如 "银行"、"船舶制造"；留空为全部。部分行业（如 白酒）接口可能无数据，将自动回退为全部。
        max_rows: 最多返回条数，默认 25

    Returns:
        盈利预测表：代码、名称、研报数、评级、预测每股收益等
    """
    try:
        symbol = (industry or "").strip()
        df = None
        used_fallback = False
        if symbol:
            df = _profit_forecast_by_symbol(symbol)
            if df is None or df.empty:
                df = _fetch_profit_forecast_em("")
                used_fallback = bool(df is not None and not df.empty)
        else:
            df = _fetch_profit_forecast_em("")
        if df is None or df.empty:
            return (
                "盈利预测数据源暂不可用（接口无数据或返回异常），请稍后再试或使用「获取核心财务指标」等工具查看估值与业绩。"
            )
        if used_fallback:
            title = f"盈利预测（按行业「{industry}」筛选暂不可用，显示全部）"
        else:
            title = f"盈利预测（{'行业: ' + industry if industry else '全部'}）"
        return f"{title}:\n" + format_dataframe(df, max_rows=max_rows)
    except Exception:
        return (
            "盈利预测数据源暂不可用（接口返回异常），请稍后再试或使用其他工具查看估值与业绩。"
        )
