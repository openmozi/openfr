"""
Output formatting utilities for beautiful CLI display.
"""

from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
import re


_TOOL_DISPLAY_NAMES: dict[str, str] = {
    "get_stock_realtime": "获取股票实时行情",
    "get_stock_history": "获取股票历史数据",
    "get_stock_info": "获取股票基本信息",
    "get_stock_financials": "获取核心财务指标",
    "search_stock": "搜索股票（A股）",
    "search_stock_any": "智能搜索股票（A股/港股）",
    "get_stock_news": "获取股票新闻",
    "get_hot_stocks": "获取热门股票",
    "get_industry_boards": "获取行业板块",
    "get_industry_board_detail": "获取行业板块详情（涨跌幅+估值）",
    "get_stock_bid_ask": "获取五档买卖盘与涨跌停",
    "get_stock_fund_flow": "获取个股资金流向",
    "get_stock_lhb_detail": "获取龙虎榜明细（按日期）",
    "get_stock_lhb_dates": "获取某股龙虎榜上榜日期",
    "get_stock_lhb_rank": "获取龙虎榜上榜统计排行",
    "get_stock_yjyg": "获取业绩预告",
    "get_stock_yjbb": "获取业绩快报",
    "get_stock_profit_forecast": "获取机构盈利预测",
    "get_stock_hk_realtime": "获取港股实时行情",
    "get_stock_hk_history": "获取港股历史数据",
    "search_stock_hk": "搜索港股",
    "get_fund_list": "获取基金列表",
    "get_etf_realtime": "获取ETF实时行情",
    "get_etf_history": "获取ETF历史数据",
    "get_fund_rank": "获取基金排行",
    "get_futures_realtime": "获取期货实时行情",
    "get_futures_history": "获取期货历史数据",
    "get_futures_inventory": "获取期货库存",
    "get_index_realtime": "获取指数实时行情",
    "get_index_history": "获取指数历史数据",
    "get_macro_cpi": "获取CPI数据",
    "get_macro_ppi": "获取PPI数据",
    "get_macro_pmi": "获取PMI数据",
    "get_macro_gdp": "获取GDP数据",
    "get_money_supply": "获取货币供应量",
}


def _display_name(tool_name: str) -> str:
    if not tool_name or tool_name == "unknown":
        return ""
    return _TOOL_DISPLAY_NAMES.get(tool_name, tool_name)


def format_stock_info(text: str) -> Panel:
    """格式化股票信息为美观的面板"""
    if "股票" in text and ("实时行情" in text or "基本信息" in text):
        # 提取关键信息
        lines = text.strip().split('\n')

        # 创建表格
        table = Table(show_header=False, box=box.SIMPLE, border_style="cyan", padding=(0, 2))
        table.add_column("项目", style="cyan bold", width=14)
        table.add_column("数值", style="white")

        # 提取股票代码和名称
        stock_code = ""
        stock_name = ""

        for line in lines[1:]:  # 跳过标题行
            if ':' not in line:
                continue

            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()

            # 跳过 N/A 值
            if value == 'N/A' or value == '':
                continue

            # 记录代码和名称
            if '股票代码' in key:
                stock_code = value
            if '股票简称' in key or '名称' in key:
                stock_name = value

            # 高亮显示涨跌幅
            if '涨跌幅' in key:
                try:
                    val = float(value.replace('%', ''))
                    if val > 0:
                        value = f"[bold green]↑ +{value}[/bold green]"
                    elif val < 0:
                        value = f"[bold red]↓ {value}[/bold red]"
                    else:
                        value = f"[dim]{value}[/dim]"
                except (ValueError, TypeError):
                    pass

            # 高亮显示价格
            elif '最新价' in key or '最新' in key:
                value = f"[bold yellow]¥ {value}[/bold yellow]"

            # 高亮显示市值
            elif '市值' in key:
                try:
                    val = float(value)
                    if val > 1000000000000:  # 万亿
                        value = f"{val/1000000000000:.2f}万亿"
                    elif val > 100000000:  # 亿
                        value = f"{val/100000000:.2f}亿"
                    value = f"[bold magenta]{value}[/bold magenta]"
                except (ValueError, TypeError):
                    pass

            table.add_row(key, value)

        # 生成标题
        title_parts = []
        if stock_code:
            title_parts.append(f"[cyan]{stock_code}[/cyan]")
        if stock_name:
            title_parts.append(f"[bold]{stock_name}[/bold]")

        if title_parts:
            title = f"📈 {' · '.join(title_parts)}"
        else:
            title = lines[0] if lines else "📈 股票信息"

        return Panel(
            table,
            title=f"[bold blue]{title}[/bold blue]",
            border_style="blue",
            box=box.DOUBLE,
            padding=(1, 2)
        )

    return Panel(text, border_style="cyan")


def format_search_results(text: str) -> Panel:
    """格式化搜索结果为表格"""
    if "搜索" in text and "结果" in text:
        lines = text.strip().split('\n')

        # 解析表格头和数据
        table = Table(box=box.DOUBLE, border_style="green", show_header=True, header_style="bold cyan")
        table.add_column("代码", style="cyan", width=10, no_wrap=True)
        table.add_column("名称", style="bold white", width=14)
        table.add_column("最新价", style="yellow", justify="right", width=12)
        table.add_column("涨跌幅", justify="right", width=12)

        # 查找数据部分
        in_data = False
        row_count = 0

        for line in lines:
            line = line.strip()

            # 跳过空行和提示行
            if not line or "搜索" in line or "提示" in line or "建议" in line or "==" in line:
                continue

            # 检测表格头
            if "代码" in line and "名称" in line:
                in_data = True
                continue

            # 解析数据行
            if in_data or re.search(r'\d{6}', line):
                # 尝试按空格分割
                parts = line.split()

                if len(parts) >= 2:
                    code = parts[0]
                    name = parts[1]
                    price = parts[2] if len(parts) > 2 else "-"
                    change = parts[3] if len(parts) > 3 else "-"

                    # 涨跌幅着色
                    if change != "-" and change != 'N/A':
                        try:
                            # 移除百分号
                            val_str = change.replace('%', '')
                            val = float(val_str)

                            if val > 0:
                                change = f"[bold green]↑ +{abs(val):.2f}%[/bold green]"
                            elif val < 0:
                                change = f"[bold red]↓ {val:.2f}%[/bold red]"
                            else:
                                change = f"[dim]{val:.2f}%[/dim]"
                        except (ValueError, TypeError):
                            pass

                    # 价格格式化
                    if price != "-":
                        try:
                            price = f"¥{float(price):.2f}"
                        except (ValueError, TypeError):
                            pass

                    table.add_row(code, name, price, change)
                    row_count += 1

                    if row_count >= 10:  # 最多显示10条
                        break

        if row_count > 0:
            # 提取搜索关键词
            keyword = ""
            for line in lines:
                if "搜索" in line and "'" in line:
                    parts = line.split("'")
                    if len(parts) >= 2:
                        keyword = parts[1]
                        break

            title = f"🔍 搜索结果"
            if keyword:
                title += f": [yellow]{keyword}[/yellow]"

            return Panel(
                table,
                title=f"[bold green]{title}[/bold green]",
                border_style="green",
                box=box.DOUBLE,
                padding=(1, 2)
            )

    return Panel(text, border_style="green")


def format_board_data(text: str) -> Panel:
    """格式化板块数据"""
    if "板块" in text or "排行" in text:
        # 解析表格数据
        lines = text.strip().split('\n')

        # 查找数据行
        data_lines = []
        for line in lines:
            line = line.strip()
            if not line or any(x in line for x in ["板块", "排行", "=="]):
                continue
            if len(line.split()) >= 2:
                data_lines.append(line)

        if data_lines:
            # 创建表格
            table = Table(box=box.ROUNDED, border_style="yellow", show_header=True)

            # 根据列数判断格式
            first_parts = data_lines[0].split()
            if len(first_parts) >= 3:
                table.add_column("名称", style="bold cyan", width=15)
                table.add_column("涨跌幅", justify="right", width=10)
                table.add_column("领涨股", style="dim", width=12)

                for line in data_lines[:15]:
                    parts = line.split()
                    if len(parts) >= 2:
                        name = parts[0]
                        change = parts[1] if len(parts) > 1 else "-"
                        leader = parts[2] if len(parts) > 2 else "-"

                        # 涨跌幅着色
                        if change != "-":
                            try:
                                val = float(change.replace('%', ''))
                                if val > 0:
                                    change = f"[bold green]+{change}%[/bold green]"
                                elif val < 0:
                                    change = f"[bold red]{change}%[/bold red]"
                            except (ValueError, TypeError):
                                pass

                        table.add_row(name, change, leader)

            title = lines[0] if lines else "板块数据"
            return Panel(table, title=f"[bold blue]📊 {title}[/bold blue]", border_style="blue")

    return Panel(text, border_style="yellow")


def format_industry_board_detail(text: str) -> Panel:
    """格式化行业板块详情（get_industry_board_detail 的键值对输出）"""
    if "行业板块：" not in text or "板块整体涨跌幅" not in text:
        return Panel(text, border_style="yellow", title="[yellow]板块详情[/yellow]")

    lines = text.strip().split("\n")
    table = Table(show_header=False, box=box.SIMPLE, border_style="yellow", padding=(0, 2))
    table.add_column("项目", style="cyan bold", width=22)
    table.add_column("数值", style="white")

    title_line = "板块详情"
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if ":" in line or "：" in line:
            # 支持中英文冒号
            sep = "：" if "：" in line else ":"
            parts = line.split(sep, 1)
            if len(parts) != 2:
                continue
            key, value = parts[0].strip(), parts[1].strip()
            if value == "":
                continue
            if "行业板块" in key:
                title_line = value
                continue
            # 涨跌幅着色
            if "涨跌幅" in key or "涨跌" in key:
                try:
                    val_str = value.replace("%", "").strip()
                    val = float(val_str)
                    if val > 0:
                        value = f"[bold green]↑ +{value}[/bold green]"
                    elif val < 0:
                        value = f"[bold red]↓ {value}[/bold red]"
                    else:
                        value = f"[dim]{value}[/dim]"
                except (ValueError, TypeError):
                    pass
            elif "最新价" in key or "市盈" in key or "市净" in key:
                value = f"[bold yellow]{value}[/bold yellow]"
            table.add_row(key, value)

    return Panel(
        table,
        title=f"[bold blue]📊 行业板块：{title_line}[/bold blue]",
        border_style="blue",
        box=box.ROUNDED,
        padding=(1, 2),
    )


def format_tool_result(tool_name: str, result: str) -> Panel:
    """根据工具类型格式化结果"""
    display_name = _display_name(tool_name)
    # 股票实时行情
    if "realtime" in tool_name or "实时" in result:
        return format_stock_info(result)

    # 搜索结果
    if "search" in tool_name or "搜索" in result:
        return format_search_results(result)

    # 行业板块详情（键值对格式，优先于通用板块表格）
    if tool_name == "get_industry_board_detail" or (
        "行业板块：" in result and "板块整体涨跌幅" in result
    ):
        return format_industry_board_detail(result)

    # 板块列表 / 概念成分股（表格格式）
    if "board" in tool_name or "concept_stocks" in tool_name or "板块" in result or "概念" in result:
        return format_board_data(result)

    # 历史数据或其他 - 使用简洁面板，并在标题中标明具体工具
    if len(result) > 300:
        # 长文本显示前200字符
        preview = result[:200] + "...\n\n[dim]（结果已截断，完整内容将在最终答案中显示）[/dim]"
        if display_name:
            title = f"[dim]工具结果预览：{display_name}[/dim]"
        elif tool_name and tool_name != "unknown":
            title = f"[dim]工具结果预览：{tool_name}[/dim]"
        else:
            title = "[dim]工具结果预览[/dim]"
        return Panel(preview, border_style="dim cyan", title=title)

    if display_name:
        title = f"[cyan]工具结果：{display_name}[/cyan]"
    elif tool_name and tool_name != "unknown":
        title = f"[cyan]工具结果：{tool_name}[/cyan]"
    else:
        title = "[cyan]工具结果[/cyan]"
    return Panel(result, border_style="cyan", title=title)


def format_final_answer(content: str) -> Panel:
    """格式化最终答案"""
    # 使用 Markdown 渲染
    from rich.markdown import Markdown

    md = Markdown(content)

    return Panel(
        md,
        title="[bold green]💡 分析结果[/bold green]",
        border_style="green",
        box=box.DOUBLE,
        padding=(1, 2)
    )


def create_progress_text(iteration: int, tool_name: str | None = None) -> Text:
    """创建美观的进度文本"""
    text = Text()

    if tool_name:
        # 工具调用进度
        text.append("🔧 ", style="bold yellow")
        text.append(f"第 {iteration} 轮 ", style="cyan")
        text.append("· ", style="dim")
        text.append(tool_name, style="bold yellow")
    else:
        # 思考进度（每轮会调用一次大模型，可能需几秒）
        text.append("🤔 ", style="bold cyan")
        text.append(f"第 {iteration} 步 · 调用大模型决策", style="bold cyan")
        text.append("（可能需几秒）", style="dim")

    return text
