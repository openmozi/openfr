"""
估值相关工具函数（PE/PB 获取逻辑）。

将 stock.py 中重复的 PE/PB 获取逻辑提取到独立模块。
"""

import pandas as pd
import akshare as ak
from datetime import datetime, timedelta

from openfr.tools.base import retry_on_network_error
from openfr.tools.constants import STOCK_INFO_TIMEOUT


def _norm_code(s: str) -> str:
    """将代码规范为 6 位数字便于比较。"""
    import re
    s = str(s).strip()
    s = re.sub(r"\D", "", s)
    return s.zfill(6)[-6:] if len(s) >= 6 else s.zfill(6)


def _to_em_symbol(symbol: str) -> str:
    """6 位代码转东方财富格式：600519 -> sh600519, 000001 -> sz000001"""
    import re
    s = re.sub(r"\D", "", str(symbol).strip())[-6:].zfill(6)
    if s.startswith("6") or s.startswith("5") or s.startswith("9"):
        return f"sh{s}"
    return f"sz{s}"


def _fmt_val(v) -> str:
    """格式化估值数据"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "N/A"
    return str(v)


@retry_on_network_error(max_retries=1, base_delay=0.5, silent=True)
def get_pe_pb_from_lg(symbol: str) -> tuple[str, str]:
    """从乐咕乐股 stock_a_lg_indicator 取单股 PE/PB"""
    if not hasattr(ak, "stock_a_lg_indicator"):
        return "N/A", "N/A"

    def _row_to_pe_pb(row: pd.Series) -> tuple[str, str]:
        pe = row.get("pe", row.get("pe_ttm", row.get("市盈率", None)))
        pb = row.get("pb", row.get("市净率", None))
        return _fmt_val(pe), _fmt_val(pb)

    try:
        # 先试单股
        for sym in (symbol, _to_em_symbol(symbol)):
            try:
                for kw in ("symbol", "stock"):
                    try:
                        df = ak.stock_a_lg_indicator(**{kw: sym})
                        if df is not None and not df.empty:
                            return _row_to_pe_pb(df.iloc[-1])
                    except TypeError:
                        continue
            except Exception:
                continue

        # 单股失败时，试拉全量再按代码筛
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

    return "N/A", "N/A"


def get_pe_pb_from_stock_info(symbol: str, fetch_func) -> tuple[str, str]:
    """从东财个股详情中取市盈率、市净率"""
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

        return _fmt_val(pe), _fmt_val(pb)

    try:
        for sym in (symbol, _to_em_symbol(symbol)):
            df = fetch_func(sym)
            pe, pb = _parse(df)
            if pe != "N/A" or pb != "N/A":
                return pe, pb
    except Exception:
        pass

    return "N/A", "N/A"


def get_pe_pb_from_eps_bps(symbol: str, spot_row: pd.Series | None, fetch_spot_func, fetch_history_func) -> tuple[str, str]:
    """用最新价/每股收益、最新价/每股净资产估算 PE/PB（兜底方案）"""
    try:
        price = None

        # 从行情行获取价格
        if spot_row is not None:
            price_col = next((c for c in spot_row.index if "最新" in str(c) or str(c).strip() in ("最新价", "close", "price")), None)
            if price_col is not None:
                price = pd.to_numeric(spot_row.get(price_col), errors="coerce")

        # 从新浪行情获取价格
        if price is None or pd.isna(price) or price <= 0:
            try:
                sina = fetch_spot_func()
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

        # 从历史数据获取最近收盘价
        if price is None or pd.isna(price) or price <= 0:
            try:
                end_d = datetime.now().strftime("%Y%m%d")
                start_d = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
                hist = fetch_history_func(symbol=symbol, period="daily", start_date=start_d, end_date=end_d)
                if hist is not None and not hist.empty and "收盘" in hist.columns:
                    price = pd.to_numeric(hist["收盘"].iloc[-1], errors="coerce")
            except Exception:
                pass

        if price is None or pd.isna(price) or price <= 0:
            return "N/A", "N/A"

        # 获取 EPS 和 BPS
        eps, bps = None, None
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

        # 计算 PE/PB
        pe_est = price / eps if eps is not None and not pd.isna(eps) and eps > 0 else None
        pb_est = price / bps if bps is not None and not pd.isna(bps) and bps > 0 else None

        return (_fmt_val(pe_est) if pe_est is not None else "N/A",
                _fmt_val(pb_est) if pb_est is not None else "N/A")
    except Exception:
        return "N/A", "N/A"
