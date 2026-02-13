"""
A 股行业板块与概念板块列表（不含概念成分股详情）。
"""

import akshare as ak
import pandas as pd

from openfr.tools.base import retry_on_network_error
from openfr.tools.stock_common import try_multiple_sources


def _normalize_change_pct(df: pd.DataFrame) -> pd.DataFrame:
    """
    东财接口返回的涨跌幅有时为十万分比（如 8.81% 返回 881121），
    若绝对值超过阈值则除以 100000 转为正常百分比；同花顺等已是百分比，不受影响。
    """
    if df is None or df.empty:
        return df
    df = df.copy()
    for col in ["涨跌幅", "领涨股票-涨跌幅", "领涨股-涨跌幅"]:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        if s.notna().any() and (s.abs() > 100).any():
            df[col] = s / 100000
    return df


@retry_on_network_error(max_retries=3, base_delay=1.2, silent=True)
def _fetch_industry_boards_em() -> pd.DataFrame:
    """获取行业板块 - 东方财富接口"""
    return ak.stock_board_industry_name_em()


@retry_on_network_error(max_retries=2, base_delay=1.0, silent=True)
def _fetch_industry_boards_ths() -> pd.DataFrame:
    """获取行业板块 - 同花顺 summary，列名统一为东方财富风格"""
    df = ak.stock_board_industry_summary_ths()
    if df is None or df.empty:
        return pd.DataFrame()
    rename = {"板块": "板块名称", "领涨股": "领涨股票"}
    if "领涨股-涨跌幅" in df.columns and "领涨股票-涨跌幅" not in df.columns:
        rename["领涨股-涨跌幅"] = "领涨股票-涨跌幅"
    if "领涨股-最新价" in df.columns and "领涨股票-最新价" not in df.columns:
        rename["领涨股-最新价"] = "领涨股票-最新价"
    return df.rename(columns=rename)


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
    """获取行业板块（串行三重备用，同花顺相关强制串行避免 libmini_racer 崩溃）"""
    df = try_multiple_sources(
        [_fetch_industry_boards_em, _fetch_industry_boards_ths, _fetch_industry_boards_name_ths],
        delay=1.0,
    )
    return _normalize_change_pct(df)


@retry_on_network_error(max_retries=2, base_delay=1.0, silent=True)
def _fetch_concept_boards_em() -> pd.DataFrame:
    """获取概念板块 - 东方财富接口"""
    return ak.stock_board_concept_name_em()


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_concept_boards_ths() -> pd.DataFrame:
    """获取概念板块 - 同花顺备用；仅名称+代码，无涨跌幅时填 NaN"""
    df = ak.stock_board_concept_name_ths()
    if df.empty:
        return df
    df = df.rename(columns={"name": "板块名称", "code": "代码"})
    if "涨跌幅" not in df.columns:
        df["涨跌幅"] = float("nan")
    return df


def _fetch_concept_boards() -> pd.DataFrame:
    """获取概念板块（东方财富 -> 同花顺备用）"""
    df = try_multiple_sources(
        [_fetch_concept_boards_em, _fetch_concept_boards_ths],
        delay=1.0,
    )
    return _normalize_change_pct(df)


@retry_on_network_error(max_retries=3, base_delay=1.2, silent=True)
def _fetch_industry_cons_em(symbol: str) -> pd.DataFrame:
    """获取指定行业板块成分股（东方财富）。symbol 为板块名称，如 酿酒行业、小金属。"""
    return ak.stock_board_industry_cons_em(symbol=symbol)
