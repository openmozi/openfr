"""
A 股财务与估值：PE/PB/ROE、财务分析指标、营收/利润增速等。
"""

from datetime import datetime, timedelta

import akshare as ak
import pandas as pd

from openfr.tools.base import retry_on_network_error
from openfr.tools.stock_common import (
    _norm_code,
    _to_em_symbol,
    _to_em_symbol_dot,
    _call_ak_with_symbol_or_stock,
)
from openfr.tools.stock_spot import (
    _fetch_stock_spot,
    _fetch_stock_spot_sina,
    _fetch_stock_info,
    _fetch_stock_history,
)

# 东财财务分析接口返回英文字段名
_EM_FINANCE_ROW_MAP = {
    "roe": ["ROEJQ", "ROEKCJQ", "ROE_AVG"],
    "rev_g": ["TOTALOPERATEREVETZ", "YYSRTB"],
    "prof_g": ["PARENTNETPROFITTZ", "JLRTB"],
}


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_stock_financial_analysis_indicator(symbol: str) -> pd.DataFrame | None:
    """获取 A股财务分析指标原始数据。兼容多种 akshare 接口与参数名。"""
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
    try:
        for sym in (symbol, _to_em_symbol(symbol)):
            ab = _call_ak_with_symbol_or_stock(ak.stock_financial_abstract, sym)
            if ab is None or ab.empty or "指标" not in ab.columns:
                continue
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
    em_sym = _to_em_symbol(symbol).upper()
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
            code_col = next(
                (c for c in ("代码", "code", "symbol", "Symbol") if c in spot_df.columns),
                spot_df.columns[0] if len(spot_df.columns) else None,
            )
            if code_col is not None:
                mask = spot_df[code_col].astype(str).apply(lambda x: _norm_code(x) == target)
                if mask.any():
                    row = spot_df.loc[mask].iloc[0]
        if row is None:
            try:
                sina_df = _fetch_stock_spot_sina()
                if not sina_df.empty:
                    code_col_s = next(
                        (c for c in ("代码", "code", "symbol") if c in sina_df.columns),
                        sina_df.columns[0] if len(sina_df.columns) else None,
                    )
                    if code_col_s is not None:
                        mask = sina_df[code_col_s].astype(str).apply(lambda x: _norm_code(x) == target)
                        if mask.any():
                            row = sina_df.loc[mask].iloc[0]
            except Exception:
                pass
        if row is None:
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
        try:
            sina_df = _fetch_stock_spot_sina()
            if not sina_df.empty:
                code_col_sina = next(
                    (c for c in ("代码", "code", "symbol") if c in sina_df.columns),
                    sina_df.columns[0] if len(sina_df.columns) else None,
                )
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
        pe2, pb2 = _get_pe_pb_from_lg(symbol)
        if pe2 != "N/A" or pb2 != "N/A":
            return pe2, pb2
        pe2, pb2 = _get_pe_pb_from_stock_info(symbol)
        if pe2 != "N/A" or pb2 != "N/A":
            return pe2, pb2
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
    """用 最新价/每股收益、最新价/每股净资产 估算 PE/PB（兜底）。"""

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
        if hasattr(ak, "stock_financial_report_sina"):
            for sym in (symbol, _to_em_symbol(symbol)):
                try:
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
        pe_est = (price / eps) if (eps is not None and not pd.isna(eps) and eps > 0) else None
        pb_est = (price / bps) if (bps is not None and not pd.isna(bps) and bps > 0) else None
        return _v(pe_est) if pe_est is not None else "N/A", _v(pb_est) if pb_est is not None else "N/A"
    except Exception:
        return "N/A", "N/A"


def _get_pe_pb_from_stock_info(symbol: str) -> tuple[str, str]:
    """从东财个股详情 item/value 中取市盈率、市净率。"""

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
    """从乐咕乐股 stock_a_lg_indicator 取单股 PE/PB；失败时试 symbol=all 再按代码筛选。"""

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
        for sym in (symbol, _to_em_symbol(symbol)):
            try:
                df = _call_ak_with_symbol_or_stock(ak.stock_a_lg_indicator, sym)
                if df is not None and not df.empty:
                    return _row_to_pe_pb(df.iloc[-1])
            except Exception:
                continue
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
    """从新浪财报摘要（宽表）中提取营业收入和净利润的同比增长率。"""
    if df is None or df.empty or len(df.columns) < 4:
        return None, None
    indicator_col = None
    for col in ["指标", "项目", "名称"]:
        if col in df.columns:
            indicator_col = col
            break
    if indicator_col is None:
        indicator_col = df.columns[1] if len(df.columns) >= 2 else None
    if indicator_col is None:
        return None, None
    period_cols = [c for c in df.columns if c not in ["选项", "指标", "项目", "名称"]]
    if len(period_cols) < 2:
        return None, None
    try:
        period_cols_sorted = sorted(period_cols, key=lambda x: str(x), reverse=True)
    except Exception:
        period_cols_sorted = period_cols
    rev_growth = None
    prof_growth = None
    for _, row in df.iterrows():
        indicator_name = str(row.get(indicator_col, ""))
        if rev_growth is None and any(k in indicator_name for k in ["营业收入增长率", "营业收入同比增长", "营收增长率", "收入增长率"]):
            val = row.get(period_cols_sorted[0])
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                try:
                    rev_growth = float(val)
                except Exception:
                    pass
        if prof_growth is None and any(k in indicator_name for k in ["净利润增长率", "净利润同比增长", "归母净利润增长率"]):
            val = row.get(period_cols_sorted[0])
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                try:
                    prof_growth = float(val)
                except Exception:
                    pass
    if rev_growth is not None and prof_growth is not None:
        return rev_growth, prof_growth
    year_periods = [p for p in period_cols_sorted if str(p).endswith("1231")]
    if len(year_periods) >= 2:
        latest_year, prev_year = year_periods[0], year_periods[1]
        if rev_growth is None:
            for _, row in df.iterrows():
                indicator_name = str(row.get(indicator_col, ""))
                if any(k in indicator_name for k in ["营业总收入", "营业收入"]) and "增长" not in indicator_name:
                    current, previous = row.get(latest_year), row.get(prev_year)
                    if current is not None and previous is not None:
                        try:
                            c, p = float(current), float(previous)
                            if p != 0:
                                rev_growth = ((c - p) / abs(p)) * 100
                                break
                        except Exception:
                            pass
        if prof_growth is None:
            for _, row in df.iterrows():
                indicator_name = str(row.get(indicator_col, ""))
                if any(k in indicator_name for k in ["归母净利润", "净利润"]) and "增长" not in indicator_name:
                    current, previous = row.get(latest_year), row.get(prev_year)
                    if current is not None and previous is not None:
                        try:
                            c, p = float(current), float(previous)
                            if p != 0:
                                prof_growth = ((c - p) / abs(p)) * 100
                                break
                        except Exception:
                            pass
    return rev_growth, prof_growth


def _fmt_finance_val(val, as_pct: bool = False) -> str:
    """格式化财务指标：as_pct 时按百分比显示。"""
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
