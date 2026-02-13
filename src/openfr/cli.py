"""
Command Line Interface for OpenFR.
"""

import os
import time

# 在导入任何其他模块之前禁用 tqdm 进度条
os.environ["TQDM_DISABLE"] = "1"

# 尝试 monkey patch tqdm 以完全禁用
try:
    import tqdm
    # 将 tqdm 替换为无操作版本
    class DummyTqdm:
        def __init__(self, *args, **kwargs):
            self.iterable = kwargs.get('iterable', args[0] if args else None)
        def __iter__(self):
            return iter(self.iterable) if self.iterable else iter([])
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def update(self, *args, **kwargs):
            pass
        def close(self):
            pass

    tqdm.tqdm = DummyTqdm
    tqdm.std.tqdm = DummyTqdm
except ImportError:
    pass

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box
from prompt_toolkit import PromptSession
from prompt_toolkit.history import InMemoryHistory

from openfr import __version__
from openfr.agent import FinancialResearchAgent
from openfr.config import Config, PROVIDER_CONFIG
from openfr.formatter import (
    format_tool_result,
    format_final_answer,
    create_progress_text
)

# 需要先加载 .env
from dotenv import load_dotenv
load_dotenv()

from openfr.tools import get_tool_descriptions


def process_agent_events(
    agent: FinancialResearchAgent,
    question: str,
    messages: list | None = None,
    verbose: bool = True,
    show_plan: bool = True,
) -> str:
    """处理 agent 事件并返回最终答案的公共函数"""
    current_tool = None
    current_step = None
    current_step_goal = None
    total_steps = None

    with console.status("[bold green]🤔 正在思考...") as status:
        for event in agent.run(question, messages=messages):
            if event["type"] == "thinking":
                iteration = event.get("iteration", 1)
                phase = event.get("phase")
                step_goal = event.get("step_goal")
                if phase == "planning":
                    status.update("[bold magenta]🧠 正在拆解任务...[/]")
                elif step_goal is not None:
                    step_num = event.get("step", iteration)
                    if total_steps is not None and current_step != step_num:
                        console.print(f"\n[bold cyan]第 {step_num}/{total_steps} 步[/] [dim]·[/] [cyan]{step_goal}[/]")
                    current_step = step_num
                    current_step_goal = step_goal
                    status.update(f"[bold cyan]📌 第 {step_num}/{total_steps or '?'} 步: {step_goal[:30]}{'…' if len(step_goal) > 30 else ''}[/]")
                else:
                    current_step = None
                    current_step_goal = None
                    status.update(create_progress_text(iteration))

            elif event["type"] == "plan":
                steps = event.get("steps") or []
                total_steps = event.get("n_steps") or (len(steps) if steps else None)
                if steps and show_plan:
                    plan_text = "\n".join(f"  {i+1}. {s}" for i, s in enumerate(steps))
                    console.print(Panel(
                        plan_text,
                        title="[bold magenta]📋 任务规划[/bold magenta]",
                        border_style="magenta",
                        box=box.ROUNDED,
                    ))
                    status.update("[bold green]✓ 规划完成，开始执行[/]")

            elif event["type"] == "tool_start":
                tool_name = event["tool"]
                tool_desc = get_tool_display_name(tool_name)
                current_tool = tool_name
                step_num = event.get("step", current_step)
                step_goal = event.get("step_goal", current_step_goal)
                if step_num is not None and step_goal is not None:
                    n = f"/{total_steps}" if total_steps is not None else ""
                    status.update(f"[bold cyan]📌 第 {step_num}{n} 步: {step_goal[:25]}{'…' if len(step_goal) > 25 else ''} · {tool_desc}[/]")
                else:
                    status.update(create_progress_text(event.get("iteration", 1), tool_desc))

                if verbose:
                    console.print(f"\n[bold cyan]▶[/bold cyan] [bold]{tool_desc}[/bold]")

            elif event["type"] == "tool_end":
                if verbose:
                    result = event["result"]
                    tool_name = current_tool or "unknown"
                    formatted_result = format_tool_result(tool_name, result)
                    console.print(formatted_result)

            elif event["type"] == "tool_warning":
                console.print(Panel(
                    f"⚠️  {event['message']}",
                    border_style="yellow",
                    title="[yellow]提示[/yellow]"
                ))

            elif event["type"] == "answer":
                console.print()
                final_panel = format_final_answer(event["content"])
                console.print(final_panel)
                return event["content"]

    return ""


app = typer.Typer(
    name="openfr",
    help="OpenFR - 基于 AKShare 的金融研究 Agent",
    add_completion=False,
)
console = Console()


# 所有支持的提供商列表
PROVIDER_CHOICES = list(PROVIDER_CONFIG.keys())


def get_default_provider() -> str:
    """从环境变量获取默认提供商"""
    return os.getenv("OPENFR_PROVIDER", "zhipu")


def get_default_model() -> str:
    """从环境变量获取默认模型"""
    return os.getenv("OPENFR_MODEL", "")


def format_status_message(event: dict) -> Text:
    """格式化状态消息，显示详细信息"""
    text = Text()

    if event["type"] == "thinking":
        iteration = event["iteration"]
        text.append("🤔 ", style="bold")
        phase = event.get("phase")
        step_goal = event.get("step_goal")
        if phase == "planning":
            text.append("规划任务", style="bold cyan")
            text.append(" - 正在拆解研究步骤...", style="dim")
        elif phase == "final_answer":
            text.append("整理最终回答", style="bold cyan")
            text.append(" - 正在综合所有步骤的结果...", style="dim")
        elif step_goal:
            # 按步骤执行阶段
            text.append(f"第 {event.get('step', iteration)} 步思考", style="bold cyan")
            text.append(f" - {step_goal}", style="dim")
        else:
            text.append(f"第 {iteration} 轮思考", style="bold cyan")
            text.append(" - 正在分析问题并决定下一步操作...", style="dim")

    elif event["type"] == "tool_start":
        tool_name = event["tool"]
        tool_desc = get_tool_display_name(tool_name)
        text.append("🔧 ", style="bold")
        text.append(f"调用工具: {tool_desc}", style="bold yellow")
        # 显示参数
        if event.get("args"):
            args_str = ", ".join(f"{k}={v}" for k, v in event["args"].items())
            if len(args_str) > 50:
                args_str = args_str[:50] + "..."
            text.append(f" ({args_str})", style="dim")

    elif event["type"] == "tool_end":
        text.append("✓ ", style="bold green")
        text.append("工具执行完成", style="green")

    elif event["type"] == "tool_warning":
        text.append("⚠ ", style="bold yellow")
        text.append(f"警告: {event['message']}", style="yellow")

    elif event["type"] == "plan":
        text.append("🧠 ", style="bold magenta")
        text.append("任务规划完成：", style="bold magenta")
        steps = event.get("steps") or []
        if steps:
            for i, s in enumerate(steps, 1):
                text.append(f"\n  {i}. {s}", style="magenta")

    return text


def get_tool_display_name(tool_name: str) -> str:
    """获取工具的中文显示名称"""
    tool_names = {
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
    return tool_names.get(tool_name, tool_name)


@app.command()
def query(
    question: str = typer.Argument(..., help="要研究的问题"),
    model: str = typer.Option(None, "--model", "-m", help="使用的模型 (留空使用环境变量或默认)"),
    provider: str = typer.Option(None, "--provider", "-p", help="模型提供商 (留空使用环境变量或默认)"),
    verbose: bool = typer.Option(True, "--verbose/--quiet", "-v/-q", help="是否显示详细过程"),
):
    """
    向金融研究 Agent 提问。

    示例:
        openfr query "贵州茅台今天股价多少?"
        openfr query "分析今天的热门板块" -p deepseek
        openfr query "查询沪深300指数" -p dashscope -m qwen-max
    """
    # 使用环境变量默认值
    if provider is None:
        provider = get_default_provider()
    if model is None:
        model = get_default_model()

    # 验证提供商
    if provider not in PROVIDER_CONFIG:
        console.print(f"[red]错误: 不支持的提供商 '{provider}'[/]")
        console.print(f"支持的提供商: {', '.join(PROVIDER_CHOICES)}")
        raise typer.Exit(1)

    config = Config(
        provider=provider,  # type: ignore
        model=model,
        verbose=verbose,
    )

    # 美化的问题显示
    query_text = Text()
    query_text.append("❓ ", style="bold blue")
    query_text.append(question, style="bold white")
    query_text.append("\n\n")
    query_text.append("🤖 模型: ", style="dim")
    query_text.append(f"{provider} / {config.get_model_name()}", style="cyan")

    console.print(Panel(
        query_text,
        title="[bold blue]OpenFR 查询[/bold blue]",
        border_style="blue",
        box=box.ROUNDED,
        padding=(1, 2)
    ))

    # 检查 API Key
    api_key = config.get_api_key()
    if not api_key and provider != "ollama":
        env_key = PROVIDER_CONFIG[provider]["env_key"]
        console.print(f"[yellow]警告: 未设置 {env_key} 环境变量[/]")

    agent = FinancialResearchAgent(config)
    process_agent_events(agent, question, verbose=verbose)


@app.command()
def chat(
    model: str = typer.Option(None, "--model", "-m", help="使用的模型 (留空使用环境变量或默认)"),
    provider: str = typer.Option(None, "--provider", "-p", help="模型提供商 (留空使用环境变量或默认)"),
):
    """
    进入交互式对话模式。

    示例:
        openfr chat
        openfr chat -p dashscope
        openfr chat -p zhipu -m glm-4-plus
    """
    # 使用环境变量默认值
    if provider is None:
        provider = get_default_provider()
    if model is None:
        model = get_default_model()

    if provider not in PROVIDER_CONFIG:
        console.print(f"[red]错误: 不支持的提供商 '{provider}'[/]")
        raise typer.Exit(1)

    config = Config(
        provider=provider,  # type: ignore
        model=model,
        verbose=True,
    )

    # 美化的欢迎界面
    welcome_text = Text()
    welcome_text.append("欢迎使用 ", style="bold")
    welcome_text.append("OpenFR", style="bold cyan")
    welcome_text.append(" 金融研究助手！", style="bold")
    welcome_text.append("\n\n")
    welcome_text.append("💹 当前配置: ", style="cyan")
    welcome_text.append(f"{provider} / {config.get_model_name()}", style="yellow")
    welcome_text.append("\n\n")
    welcome_text.append("📝 输入您的问题开始分析", style="dim")
    welcome_text.append("\n")
    welcome_text.append("🚪 输入 ", style="dim")
    welcome_text.append("exit", style="bold dim")
    welcome_text.append(" 或 ", style="dim")
    welcome_text.append("quit", style="bold dim")
    welcome_text.append(" 退出", style="dim")

    console.print(Panel(
        welcome_text,
        title="[bold blue]💡 OpenFR Chat[/bold blue]",
        border_style="blue",
        box=box.DOUBLE,
        padding=(1, 2)
    ))

    agent = FinancialResearchAgent(config)
    # 多轮对话上下文（仅保存用户/助手消息，避免工具结果过长）
    chat_history = []

    # 创建带历史记录的输入会话
    session = PromptSession(history=InMemoryHistory())

    while True:
        try:
            console.print()
            try:
                question = session.prompt("你: ")
            except (EOFError, KeyboardInterrupt):
                break

            if question.lower() in ("exit", "quit", "q"):
                console.print("[dim]👋 再见！[/]")
                break

            if not question.strip():
                continue

            console.print()
            start_time = time.time()
            process_agent_events(agent, question, messages=chat_history, verbose=True, show_plan=True)
            elapsed = time.time() - start_time
            console.print(f"[dim]⏱ 本轮用时 {elapsed:.1f} 秒[/]")

        except KeyboardInterrupt:
            console.print("\n[dim]已取消当前操作[/]")
            continue


@app.command()
def tools():
    """
    列出所有可用的金融数据工具。
    """
    console.print(Panel(get_tool_descriptions(), title="🔧 可用工具"))


@app.command()
def providers():
    """
    列出所有支持的模型提供商。
    """
    table = Table(title="支持的模型提供商")
    table.add_column("提供商", style="cyan")
    table.add_column("环境变量", style="green")
    table.add_column("默认模型", style="yellow")
    table.add_column("说明")
    table.add_column("状态", style="bold")

    providers_list = Config.list_providers()
    provider_by_name = {p["name"]: p for p in providers_list}

    # 国产模型
    table.add_row("[bold]--- 国产模型 ---", "", "", "", "")
    for name in ["deepseek", "doubao", "dashscope", "zhipu", "modelscope", "kimi", "stepfun", "minimax"]:
        cfg = PROVIDER_CONFIG[name]
        provider_info = provider_by_name.get(name)
        status = "[green]✓ 已配置[/]" if provider_info and provider_info["configured"] else "[dim]未配置[/]"
        table.add_row(name, cfg["env_key"], cfg["default_model"], cfg["description"], status)

    # 海外模型
    table.add_row("[bold]--- 海外模型 ---", "", "", "", "")
    for name in ["openai", "anthropic", "openrouter", "together", "groq"]:
        cfg = PROVIDER_CONFIG[name]
        provider_info = provider_by_name.get(name)
        status = "[green]✓ 已配置[/]" if provider_info and provider_info["configured"] else "[dim]未配置[/]"
        table.add_row(name, cfg["env_key"], cfg["default_model"], cfg["description"], status)

    # 代理/聚合
    table.add_row("[bold]--- 代理/聚合 ---", "", "", "", "")
    cfg = PROVIDER_CONFIG["modelsproxy"]
    provider_info = provider_by_name.get("modelsproxy")
    status = "[green]✓ 已配置[/]" if provider_info and provider_info["configured"] else "[dim]未配置[/]"
    table.add_row("modelsproxy", cfg["env_key"], cfg["default_model"], cfg["description"], status)

    # 本地部署
    table.add_row("[bold]--- 本地部署 ---", "", "", "", "")
    cfg = PROVIDER_CONFIG["ollama"]
    table.add_row("ollama", cfg["env_key"], cfg["default_model"], cfg["description"], "[dim]本地[/]")

    # 自定义
    table.add_row("[bold]--- 自定义 ---", "", "", "", "")
    cfg = PROVIDER_CONFIG["custom"]
    table.add_row("custom", "CUSTOM_API_KEY + CUSTOM_BASE_URL", "(需指定)", cfg["description"], "[dim]自定义[/]")

    console.print(table)

    # 显示当前默认配置
    current_provider = get_default_provider()
    current_model = get_default_model() or PROVIDER_CONFIG.get(current_provider, {}).get("default_model", "")
    console.print(f"\n[bold]当前默认:[/] {current_provider} / {current_model}")
    console.print("[dim]提示: 在 .env 文件中设置 OPENFR_PROVIDER 和 OPENFR_MODEL 可修改默认值[/]")


@app.command()
def version():
    """
    显示版本信息。
    """
    console.print(f"OpenFR v{__version__}")


if __name__ == "__main__":
    app()
