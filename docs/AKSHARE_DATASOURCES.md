# AKShare 数据源与备用接口说明

本文档梳理 **指数实时行情**、**行业板块**、**A 股实时行情** 在 AKShare 中的多数据源，便于在东方财富/新浪等接口失败时切换备用。

---

## 1. 指数行情

### 1.1 实时行情（全市场）

| 数据源 | AKShare 接口 | 说明 |
|--------|--------------|------|
| 东方财富 | `stock_zh_index_spot_em(symbol)` | symbol 可选：`"沪深重要指数"`、`"上证系列指数"`、`"深证系列指数"`、`"指数成份"`、`"中证系列指数"`，无参默认上证系列。易 RemoteDisconnected。 |
| 新浪 | `stock_zh_index_spot_sina()` | 新浪财经-行情中心-所有指数。注意：导出名为 `stock_zh_index_spot_sina`，不是 `stock_zh_index_spot`。 |

**当前 OpenFR 使用**：东财 → 新浪；若仍失败则用「指数日线」拼最新一条当实时。

### 1.2 指数日线（单只 / 用于拼“最新”）

| 数据源 | AKShare 接口 | 代码格式 | 说明 |
|--------|--------------|----------|------|
| 东方财富 | `index_zh_a_hist(symbol, period, start_date, end_date)` | `"000001"`、`"399001"` | 最常用，易断开。 |
| 新浪 | `stock_zh_index_daily(symbol)` | `"sh000001"`、`"sz399001"` | 需把 000001→sh000001，399xxx→sz399xxx。 |
| 腾讯 | `stock_zh_index_daily_tx(symbol)` | `"sh000001"`、`"sz399001"` | 补充新浪缺失指数，前复权。 |
| 东方财富(另一) | `stock_zh_index_daily_em(symbol, start_date, end_date)` | `"sh000001"`、`"sz399001"`、`"csi000905"` | 东财指数日线另一接口。 |

**可做备用**：东财 `index_zh_a_hist` 失败时，用新浪 `stock_zh_index_daily("sh000001")` 或腾讯 `stock_zh_index_daily_tx("sh000001")` 取最近一天当「上证指数最新」。

---

## 2. 行业板块

| 数据源 | AKShare 接口 | 说明 |
|--------|--------------|------|
| 东方财富 | `stock_board_industry_name_em()` | 行业板块名称+涨跌幅等，易 RemoteDisconnected。 |
| 同花顺 | `stock_board_industry_summary_ths()` | 同花顺行业一览表，含板块、涨跌幅、领涨股等，列名与东财略有不同，需做列名映射。 |

**当前 OpenFR 使用**：东财 → 同花顺 summary；无更多可直接替代「行业板块列表+涨跌幅」的第三方源。

申万：`sw_index_first_info()` 等为乐咕乐股/申万一级分类，无涨跌幅，不适合做板块排行替代。

---

## 3. A 股实时行情

### 3.1 全市场列表（用于搜索 / 按代码筛单只）

| 数据源 | AKShare 接口 | 说明 |
|--------|--------------|------|
| 东方财富 | `stock_zh_a_spot_em()` | 易 RemoteDisconnected。 |
| 新浪 | `stock_zh_a_spot()` | 备用。 |

**当前 OpenFR 使用**：东财 → 新浪；个股信息失败时用全市场列表按代码筛一行做降级。

### 3.2 单只个股信息/实时

| 数据源 | AKShare 接口 | 说明 |
|--------|--------------|------|
| 东方财富 | `stock_individual_info_em(symbol)` | 单只详情，含最新价、涨跌幅等，易断开。 |

**当前 OpenFR 使用**：先个股信息，失败则用全市场 `stock_zh_a_spot_em` / `stock_zh_a_spot` 按代码取一行。

同模块还有 `stock_sh_a_spot_em`、`stock_sz_a_spot_em` 等，均为东财，无独立备用源。

---

## 4. 概念板块

| 数据源 | AKShare 接口 | 说明 |
|--------|--------------|------|
| 东方财富 | `stock_board_concept_name_em()` | 概念板块名称+涨跌幅等。 |
| 同花顺 | `stock_board_concept_name_ths()` | 仅名称+代码，无涨跌幅；用作东财失败时的备用，展示时涨跌幅为 NaN。 |

**当前 OpenFR 使用**：东财 → 同花顺 name 列表备用。

---

## 5. 基金 ETF

| 数据类型 | 主源 | 备用 | 说明 |
|----------|------|------|------|
| ETF 实时 | 东财 `fund_etf_spot_em()` | 同花顺 `fund_etf_spot_ths(date="")` | 同花顺列名映射为代码、名称、涨跌幅、最新价。 |
| ETF 历史 | 东财 `fund_etf_hist_em(symbol, ...)` | 新浪 `fund_etf_hist_sina(symbol="sh510050")` | 东财失败时用新浪，symbol 转为 sh/sz 前缀，并按 start_date/end_date 过滤。 |

LOF、基金列表、基金排行等仍为东财单源，无备用。

---

## 6. 期货与宏观

- **期货**：实时 `futures_zh_spot`、库存 `futures_inventory_em` 为单源；历史已用新浪 `futures_zh_daily_sina`。已加强重试（base_delay=1.2，silent=True），未删接口。
- **宏观**：CPI/PPI/PMI/GDP/货币供应量等均为单源，已加静默重试，未删接口。

---

## 7. 核心财务指标（PE/PB/ROE/营收与利润增速）

| 数据源 | AKShare 接口 | 说明 |
|--------|--------------|------|
| 东方财富-主要指标 | `stock_financial_analysis_indicator_em(symbol, indicator="按报告期")` | 返回英文字段：REPORT_DATE、ROEJQ、TOTALOPERATEREVETZ、PARENTNETPROFITTZ 等；symbol 建议用 `600519.SH` 格式。优先取年报（REPORT_DATE 末四位 1231）与东财页面一致。 |
| 新浪-关键指标 | `stock_financial_abstract(symbol)` | 列：选项、指标、各报告期（20241231 等），指标行为中文名。 |
| 东财-同行比较 | `stock_zh_dupont_comparison_em(symbol="SH600519")`、`stock_zh_growth_comparison_em`、`stock_zh_valuation_comparison_em` | 杜邦取 ROE-24A；成长性取营收/净利润增长率-24A；估值取市净率-MRQ/市净率-24A。 |

**当前 OpenFR 使用**：东财主要指标（英文字段解析）→ 新浪摘要 + 东财同行比较补数；PE/PB 行情兜底 + 估值比较补市净率；增速统一按百分比显示，报告期标注年报/报告期。

---

## 8. OpenFR 已接入的 AKShare 扩展数据

以下数据已通过 `openfr.tools.stock_ext` 提供，对话中可直接调用对应工具：

| 类别 | OpenFR 工具 | AKShare 接口 | 说明 |
|------|-------------|--------------|------|
| 行情报价 | `get_stock_bid_ask` | `stock_bid_ask_em(symbol)` | 五档买卖盘、涨跌停价、量比、换手等。 |
| 资金流向 | `get_stock_fund_flow` | `stock_individual_fund_flow(stock, market)` | 个股近期主力/大单/中单/小单净流入。 |
| 龙虎榜 | `get_stock_lhb_detail` | `stock_lhb_detail_em(start_date, end_date)` | 按日期范围龙虎榜明细。 |
| 龙虎榜 | `get_stock_lhb_dates` | `stock_lhb_stock_detail_date_em(symbol)` | 某股龙虎榜上榜日期列表。 |
| 龙虎榜 | `get_stock_lhb_rank` | `stock_lhb_stock_statistic_em(symbol)` | 近一月/三月/六月/一年上榜统计排行。 |
| 业绩预告 | `get_stock_yjyg` | `stock_yjyg_em(date)` | 指定报告期业绩预告列表。 |
| 业绩快报 | `get_stock_yjbb` | `stock_yjbb_em(date)` | 指定报告期业绩快报列表。 |
| 盈利预测 | `get_stock_profit_forecast` | `stock_profit_forecast_em(symbol)` | 机构一致预期（可按行业或全部）。 |

尚未接入、可在后续版本扩展的接口示例：股东与高管（`stock_hold_control_cninfo`、`stock_hold_change_cninfo`）、板块资金流（`stock_sector_fund_flow_rank`）等。接入时需注意 symbol 格式（6 位 / sh/sz / 600519.SH）与列名兼容。

---

## 9. 小结与建议

- **指数**：实时用东财 + 新浪（接口名 `stock_zh_index_spot_sina`）；日线用东财 `index_zh_a_hist`，失败可接新浪 `stock_zh_index_daily` 或腾讯 `stock_zh_index_daily_tx`（代码格式 sh/sz+6 位）。
- **行业板块**：东财 + 同花顺 `stock_board_industry_summary_ths`，已用；无更多等价备用。
- **A 股实时**：东财个股 + 东财/新浪全市场列表降级，已用；无更多独立数据源。
- **概念板块**：东财 + 同花顺 `stock_board_concept_name_ths` 备用（无涨跌幅）。
- **基金 ETF**：东财 + 同花顺实时、新浪历史备用；LOF/基金列表/排行仍单源。
- **期货/宏观**：单源，已加强重试与静默，未删接口。
- **核心财务指标**：东财主要指标（英文字段）+ 新浪摘要 + 东财同行比较；PE/PB 行情 + 估值比较兜底；优先年报、增速按%显示。

若所有上述接口在同一网络下均不可用，多为网络/防火墙限制，可考虑更换网络或使用代理。
