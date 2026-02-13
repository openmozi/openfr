"""
A 股概念板块成分股：东财/同花顺拉取、名称解析、结果归一化与对外实现。
"""

import os
import re
from functools import lru_cache

import akshare as ak
import pandas as pd
import requests

from openfr.tools.base import format_dataframe, retry_on_network_error
from openfr.tools.stock_boards import _fetch_concept_boards_em


@retry_on_network_error(max_retries=2, base_delay=1.0, silent=True)
def _fetch_concept_stocks_em(concept_name: str) -> pd.DataFrame:
    """获取概念板块成分股 - 东方财富，symbol 为板块名称或板块代码(BKxxxx)"""
    return ak.stock_board_concept_cons_em(symbol=concept_name)


@retry_on_network_error(max_retries=2, base_delay=0.8, silent=True)
def _fetch_concept_stocks_em_direct(board_code: str) -> pd.DataFrame:
    """
    东方财富概念板块成分股（直连接口 + 显式 timeout + 分页）
    避免 akshare 内部 fetch_paginated_data 在特定网络下卡住/返回空。
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
            for pn in range(1, 11):
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
    生成同花顺访问所需的 v Cookie。默认禁用 py_mini_racer，设置 OPENFR_ENABLE_THS_JS=1 启用。
    """
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
        return str(js_code.call("v"))
    except Exception:
        return ""


def _resolve_em_concept_board_code(concept_name: str) -> str | None:
    """从东方财富概念板块列表解析板块代码(BKxxxx)，支持精确与包含匹配。"""
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
        contains = df.loc[s.str.contains(re.escape(name), na=False), "板块代码"]
        if not contains.empty:
            return str(contains.values[0]).strip()
    except Exception:
        return None
    return None


def _resolve_ths_concept_code(concept_name: str) -> str | None:
    """从同花顺概念列表解析概念 code(数字)。"""
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
    获取概念板块成分股 - 同花顺网页解析兜底。
    东财概念成分股接口在部分网络下可能断连/返回空时使用。
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
    获取指定概念板块的成分股列表及行情。可先调用 get_concept_boards 查看板块名称。
    """
    concept_name = (concept_name or "").strip()
    if not concept_name:
        return "请传入概念板块名称，如：人工智能、ChatGPT概念。可先调用 get_concept_boards 查看可选板块。"

    if re.match(r"^BK\d+", concept_name.upper()):
        df0 = _fetch_concept_stocks_em_direct(concept_name.upper())
        if not df0.empty:
            if "涨跌幅" in df0.columns:
                df0 = df0.sort_values("涨跌幅", ascending=False)
            out_cols = [c for c in ["代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交额"] if c in df0.columns]
            result_df0 = df0[out_cols].head(30) if out_cols else df0.head(30)
            return f"概念「{concept_name.upper()}」成分股（按涨跌幅）:\n\n{format_dataframe(result_df0)}"

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
