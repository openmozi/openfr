"""
Financial Research Agent implementation.
"""

from typing import Iterator
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langchain_core.language_models import BaseChatModel
from langchain_openai import ChatOpenAI

from openfr.config import Config, PROVIDER_CONFIG
from openfr.prompts import (
    get_system_prompt,
    FINAL_ANSWER_PROMPT,
    SELF_VALIDATION_PROMPT,
    LOOP_DETECTED_PROMPT,
    PLANNING_SYSTEM_PROMPT,
    parse_plan,
)
from openfr.scratchpad import Scratchpad
from openfr.tools import get_all_tools
from openfr.tools.parallel import can_parallelize, execute_tools_parallel


class FinancialResearchAgent:
    """
    A financial research agent that uses LLM and AKShare tools
    to answer questions about Chinese financial markets.

    支持的模型提供商:
    - 国产: deepseek, doubao, dashscope, zhipu, modelscope, kimi, stepfun, minimax
    - 海外: openai, anthropic, openrouter, together, groq
    - 本地: ollama
    """

    def __init__(self, config: Config | None = None):
        """
        Initialize the agent.

        Args:
            config: Configuration object. If None, loads from environment.
        """
        self.config = config or Config.from_env()
        self.llm = self._create_llm()
        self.tools = get_all_tools(
            include_stock=self.config.enable_stock_tools,
            include_stock_hk=self.config.enable_stock_hk_tools,
            include_fund=self.config.enable_fund_tools,
            include_futures=self.config.enable_futures_tools,
            include_index=self.config.enable_index_tools,
            include_macro=self.config.enable_macro_tools,
        )
        self.llm_with_tools = self.llm.bind_tools(self.tools)

    def _create_llm(self) -> BaseChatModel:
        """Create the LLM based on configuration."""
        provider = self.config.provider
        model = self.config.get_model_name()
        api_key = self.config.get_api_key()
        base_url = self.config.get_base_url()

        # Anthropic 使用专门的 SDK
        if provider == "anthropic":
            from langchain_anthropic import ChatAnthropic

            return ChatAnthropic(
                model=model,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
                api_key=api_key,
            )

        # Ollama 使用专门的 SDK
        if provider == "ollama":
            from langchain_ollama import ChatOllama

            return ChatOllama(
                model=model,
                temperature=self.config.temperature,
                base_url=base_url,
            )

        # 其他所有提供商都使用 OpenAI 兼容接口
        # 包括: openai, deepseek, doubao, dashscope, zhipu, modelscope,
        #       kimi, stepfun, minimax, openrouter, together, groq
        kwargs = {
            "model": model,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
            "api_key": api_key,
        }

        if base_url:
            kwargs["base_url"] = base_url

        return ChatOpenAI(**kwargs)

    def _get_tool_by_name(self, name: str):
        """Get a tool function by its name."""
        for tool in self.tools:
            if tool.name == name:
                return tool
        return None

    def _trim_chat_history(self, history: list[BaseMessage], max_messages: int = 20) -> None:
        """
        Trim chat history in-place to keep context bounded.

        We only store Human/AI messages in history for multi-turn chat,
        avoiding ToolMessage bloat across turns.
        """
        if len(history) <= max_messages:
            return
        # keep the most recent messages
        del history[:-max_messages]

    def _run_plan_execute(
        self,
        query: str,
        ctx_messages: list[BaseMessage],
        scratchpad: Scratchpad,
        log_path: str | None,
        remember: bool,
        messages: list[BaseMessage] | None,
    ) -> Iterator[dict]:
        """Dexter 风格：先规划任务（输出步骤列表），再按步骤依次执行工具并最后综合回答。"""
        # ---------- 阶段一：规划 ----------
        yield {"type": "thinking", "iteration": 1, "phase": "planning"}
        planning_messages: list[BaseMessage] = [
            SystemMessage(content=PLANNING_SYSTEM_PROMPT),
            HumanMessage(content=query),
        ]
        plan_response = self.llm.invoke(planning_messages)
        steps = parse_plan(plan_response.content or "")
        if not steps:
            # 解析失败或空计划时，退化为单一步骤：直接围绕原始问题执行
            fallback_goal = (query or "").strip() or "回答用户问题"
            steps = [{"goal": fallback_goal}]

        step_goals = [s["goal"] for s in steps]
        n_steps = len(steps)
        yield {"type": "plan", "steps": step_goals, "n_steps": n_steps}

        # 执行前注入一次「研究计划」摘要，便于模型知道全局；每步再强调「仅完成当前步」
        plan_summary = "研究计划（共 {} 步）：\n".format(n_steps) + "\n".join(f"  {i+1}. {g}" for i, g in enumerate(step_goals))
        ctx_messages.append(HumanMessage(content=plan_summary + "\n\n请按上述步骤依次执行。下面将只给出当前需要完成的那一步。"))

        # ---------- 阶段二：按步骤执行 ----------
        for step_index, step in enumerate(steps):
            goal = step["goal"]
            k = step_index + 1
            current_step_msg = (
                f"【仅完成第 {k}/{n_steps} 步】{goal}\n\n"
                "请只为本步骤调用所需工具，不要为后续步骤调用工具。完成本步后回复简要说明即可。"
            )
            ctx_messages.append(HumanMessage(content=current_step_msg))

            step_iteration = 0
            while step_iteration < self.config.max_iterations:
                step_iteration += 1
                yield {"type": "thinking", "iteration": step_index + 1, "step": step_index + 1, "step_goal": goal}

                response = self.llm_with_tools.invoke(ctx_messages)
                ctx_messages.append(response)

                if not response.tool_calls:
                    break

                tool_results = []
                current_step_num = step_index + 1
                tool_calls = response.tool_calls

                # 可并行且均为允许的调用时，并行执行
                use_parallel = (
                    self.config.enable_parallel_tools
                    and can_parallelize(tool_calls)
                    and all(
                        scratchpad.can_call_tool(tc.get("name"), tc.get("args", {}))[0]
                        for tc in tool_calls
                    )
                )

                if use_parallel:
                    for tc in tool_calls:
                        yield {"type": "tool_start", "tool": tc["name"], "args": tc.get("args", {}), "step": current_step_num, "step_goal": goal}
                    parallel_results = execute_tools_parallel(
                        tool_calls,
                        get_tool_func=self._get_tool_by_name,
                        max_workers=min(2 + len(tool_calls), 8),
                        timeout=45.0,
                    )
                    for tool_call, pres in zip(tool_calls, parallel_results):
                        tool_name = tool_call["name"]
                        tool_args = tool_call.get("args", {})
                        if pres.get("error"):
                            result_str = f"工具执行失败: {pres['error']}"
                            scratchpad.add_call(tool_name, tool_args, "", error=pres["error"])
                        else:
                            result_str = pres["result"] if isinstance(pres["result"], str) else str(pres["result"])
                            scratchpad.add_call(tool_name, tool_args, result_str)
                        result_preview = result_str[:500] + "..." if len(result_str) > 500 else result_str
                        yield {"type": "tool_end", "tool": tool_name, "result": result_preview, "step": current_step_num, "step_goal": goal}
                        tool_results.append(ToolMessage(content=result_str, tool_call_id=tool_call["id"]))
                else:
                    for tool_call in tool_calls:
                        tool_name = tool_call["name"]
                        tool_args = tool_call.get("args", {})
                        yield {"type": "tool_start", "tool": tool_name, "args": tool_args, "step": current_step_num, "step_goal": goal}
                        can_call, warning = scratchpad.can_call_tool(tool_name, tool_args)
                        if not can_call:
                            yield {"type": "tool_warning", "tool": tool_name, "message": warning}
                            result = f"跳过: {warning}"
                        else:
                            tool = self._get_tool_by_name(tool_name)
                            if tool:
                                try:
                                    result = tool.invoke(tool_args)
                                    scratchpad.add_call(tool_name, tool_args, result)
                                except Exception as e:
                                    result = f"工具执行失败: {str(e)}"
                                    scratchpad.add_call(tool_name, tool_args, "", error=str(e))
                            else:
                                result = f"未找到工具: {tool_name}"
                        result_str = result if isinstance(result, str) else str(result)
                        result_preview = result_str[:500] + "..." if len(result_str) > 500 else result_str
                        yield {"type": "tool_end", "tool": tool_name, "result": result_preview, "step": current_step_num, "step_goal": goal}
                        tool_results.append(ToolMessage(content=result_str, tool_call_id=tool_call["id"]))
                ctx_messages.extend(tool_results)

                if self.config.enable_loop_detection and (
                    scratchpad.is_loop_no_progress() or len(scratchpad.tool_calls) >= self.config.max_total_tool_calls
                ):
                    yield {"type": "tool_warning", "tool": "", "message": "检测到无进展或调用次数已达上限，将基于已有信息收尾"}
                    break

        # ---------- 阶段三：综合回答 ----------
        ctx_messages.append(HumanMessage(content=FINAL_ANSWER_PROMPT))
        yield {"type": "thinking", "iteration": n_steps + 1, "phase": "final_answer"}
        final_response = self.llm.invoke(ctx_messages)
        final_answer = final_response.content or ""

        # 可选自校验：在最终回答前，让模型基于已获取的数据自检是否充分
        if self.config.enable_self_validation:
            ctx_messages.append(final_response)
            ctx_messages.append(HumanMessage(content=SELF_VALIDATION_PROMPT))
            yield {"type": "thinking", "iteration": n_steps + 1, "phase": "self_validation"}
            final_response2 = self.llm.invoke(ctx_messages)
            final_answer = final_response2.content or final_answer

        yield {"type": "answer", "content": final_answer}
        if remember and messages is not None:
            messages.append(HumanMessage(content=query))
            messages.append(AIMessage(content=final_answer))
            self._trim_chat_history(messages)
        if log_path is not None:
            scratchpad.write_jsonl(log_path, final_answer=final_answer)

    def run(self, query: str, messages: list[BaseMessage] | None = None) -> Iterator[dict]:
        """
        Run the agent to answer a query.

        Args:
            query: The user's question
            messages: Optional chat history (in-place). When provided, the agent
                will use it as multi-turn context and append (user, assistant)
                messages back to it.

        Yields:
            Event dictionaries with type and data
        """
        scratchpad = Scratchpad(query)

        # 生成本次会话的 Scratchpad 日志路径（若启用）
        log_path: str | None = None
        if self.config.log_scratchpad:
            base_dir = (
                self.config.log_dir
                or str(Path.home() / ".openfr" / "scratchpad")
            )
            run_id = datetime.now().strftime("%Y-%m-%d-%H%M%S") + "_" + uuid4().hex[:8]
            log_path = str(Path(base_dir) / f"{run_id}.jsonl")

        # Build per-turn context messages
        if messages is None:
            ctx_messages: list[BaseMessage] = [
                SystemMessage(content=get_system_prompt()),
                HumanMessage(content=query),
            ]
            remember = False
        else:
            # multi-turn chat: include history but keep it lean (no tool traces)
            ctx_messages = [SystemMessage(content=get_system_prompt()), *messages, HumanMessage(content=query)]
            remember = True
            # trim before running to avoid unbounded growth
            self._trim_chat_history(messages)

        # 统一走「先规划再执行」流程
        yield from self._run_plan_execute(
            query=query,
            ctx_messages=ctx_messages,
            scratchpad=scratchpad,
            log_path=log_path,
            remember=remember,
            messages=messages,
        )

    def query(self, query: str, verbose: bool | None = None) -> str:
        """
        Simple interface to query the agent and get a string response.

        Args:
            query: The user's question
            verbose: Whether to print progress. If None, uses config.verbose

        Returns:
            The agent's answer as a string
        """
        verbose = verbose if verbose is not None else self.config.verbose
        answer = ""

        for event in self.run(query):
            if verbose:
                if event["type"] == "thinking":
                    print(f"\n[迭代 {event['iteration']}] 思考中...")
                elif event["type"] == "tool_start":
                    print(f"  调用工具: {event['tool']}")
                elif event["type"] == "tool_end":
                    result_preview = (
                        event["result"][:100] + "..."
                        if len(event["result"]) > 100
                        else event["result"]
                    )
                    print(f"  结果: {result_preview}")
                elif event["type"] == "tool_warning":
                    print(f"  警告: {event['message']}")

            if event["type"] == "answer":
                answer = event["content"]

        return answer
