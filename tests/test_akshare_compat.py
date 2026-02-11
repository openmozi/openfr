"""
测试项目中使用的 akshare 接口参数是否与当前 akshare 版本兼容。
仅检查参数名/参数数量导致的 TypeError，不依赖网络数据返回内容。
"""
import inspect
import pytest
import akshare as ak


def _sig_params(name: str):
    """获取 akshare 中某函数的参数名列表，若不存在或不可调用则返回 None。"""
    func = getattr(ak, name, None)
    if func is None or not callable(func):
        return None
    try:
        return list(inspect.signature(func).parameters.keys())
    except Exception:
        return None


# 项目中实际调用方式：(接口名, 我们传入的关键字参数集合，必须为 set)
AK_CALLS = [
    ("stock_zh_a_spot_em", set()),
    ("stock_zh_a_spot", set()),
    ("stock_info_a_code_name", set()),
    ("stock_zh_a_hist", {"symbol", "period", "start_date", "end_date", "timeout"}),
    ("stock_individual_info_em", {"symbol", "timeout"}),
    ("stock_news_em", {"symbol"}),
    ("stock_hot_rank_em", set()),
    ("stock_board_industry_name_em", set()),
    ("stock_board_industry_summary_ths", set()),
    ("stock_board_concept_name_em", set()),
    ("stock_board_concept_name_ths", set()),
    ("stock_financial_analysis_indicator", {"symbol"}),
    ("stock_financial_analysis_indicator_em", {"symbol"}),
    ("stock_financial_abstract", {"symbol"}),
    ("stock_financial_report_sina", {"stock", "symbol"}),
    ("stock_board_industry_cons_em", {"symbol"}),
    ("stock_board_concept_cons_em", {"symbol"}),
    ("index_zh_a_hist", {"symbol", "period", "start_date", "end_date"}),
    ("stock_zh_index_daily", {"symbol"}),
    ("stock_zh_index_daily_tx", {"symbol"}),
    ("stock_zh_index_spot_em", set()),
    ("stock_zh_index_spot_sina", set()),
    ("stock_hk_spot_em", set()),
    ("stock_hk_spot", set()),
    ("stock_hk_hot_rank_em", set()),
    ("stock_hk_main_board_spot_em", set()),
    ("stock_hk_hist", {"symbol", "period", "start_date", "end_date", "adjust"}),
    ("fund_etf_spot_em", set()),
    ("fund_etf_spot_ths", {"date"}),
    ("fund_lof_spot_em", set()),
    ("fund_name_em", set()),
    ("fund_etf_hist_em", {"symbol", "period", "start_date", "end_date", "adjust"}),
    ("fund_etf_hist_sina", {"symbol"}),
    ("fund_open_fund_rank_em", {"symbol"}),
    ("futures_zh_spot", set()),
    ("futures_zh_daily_sina", {"symbol"}),
    ("futures_inventory_em", {"symbol"}),
    ("macro_china_cpi", set()),
    ("macro_china_ppi", set()),
    ("macro_china_pmi", set()),
    ("macro_china_gdp", set()),
    ("macro_china_money_supply", set()),
]


@pytest.mark.parametrize("name,our_kwargs", AK_CALLS)
def test_akshare_interface_accepts_our_params(name, our_kwargs):
    """我们传入的参数名必须是 akshare 接口支持的参数名（或为接口的 **kwargs）。"""
    params = _sig_params(name)
    if params is None:
        pytest.skip(f"akshare 中无 {name} 或不可调用")
    # 我们传的每个关键字必须在接口参数里存在
    extra = our_kwargs - set(params)
    assert not extra, (
        f"{name}: 我们传了接口不支持的参数: {extra}. "
        f"当前 akshare 参数: {params}"
    )
