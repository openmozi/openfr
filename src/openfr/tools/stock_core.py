"""
A 股数据工具聚合层：从子模块 re-export，保持 from openfr.tools.stock_core import ... 的 API 不变。
"""

from openfr.tools.stock_common import (
    _call_ak_with_symbol_or_stock,
    _invoke_sub_tool,
    _norm_code,
    _to_em_symbol,
    _to_em_symbol_dot,
)
from openfr.tools.stock_spot import (
    _fetch_stock_history,
    _fetch_stock_info,
    _fetch_stock_news,
    _fetch_stock_spot,
    _fetch_stock_spot_sina,
    _fetch_hot_stocks,
    _get_stock_list_code_name_cached,
    _realtime_from_spot_row,
)
from openfr.tools.stock_boards import (
    _fetch_concept_boards,
    _fetch_industry_boards,
    _fetch_industry_cons_em,
)
from openfr.tools.stock_finance import (
    _extract_growth_from_abstract,
    _fetch_roe_revg_profg_fallback,
    _fetch_stock_financial_analysis_indicator,
    _fmt_finance_val,
    _get_pe_pb_from_spot,
    _parse_em_finance_row,
)
from openfr.tools.stock_concept import _get_concept_stocks_impl

__all__ = [
    "_call_ak_with_symbol_or_stock",
    "_extract_growth_from_abstract",
    "_fetch_concept_boards",
    "_fetch_industry_boards",
    "_fetch_industry_cons_em",
    "_fetch_roe_revg_profg_fallback",
    "_fetch_stock_financial_analysis_indicator",
    "_fetch_stock_history",
    "_fetch_stock_info",
    "_fetch_stock_news",
    "_fetch_stock_spot",
    "_fetch_stock_spot_sina",
    "_fetch_hot_stocks",
    "_fmt_finance_val",
    "_get_concept_stocks_impl",
    "_get_pe_pb_from_spot",
    "_get_stock_list_code_name_cached",
    "_invoke_sub_tool",
    "_norm_code",
    "_parse_em_finance_row",
    "_realtime_from_spot_row",
    "_to_em_symbol",
    "_to_em_symbol_dot",
]
