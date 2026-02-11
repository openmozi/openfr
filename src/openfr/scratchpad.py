"""
Scratchpad for tracking tool calls and managing context.

受 Dexter 启发，本模块不仅在内存中保存工具调用记录，
还支持将一次会话的调用轨迹持久化为 JSONL 方便调试与回放。
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
import json
from pathlib import Path


@dataclass
class ToolCall:
    """Record of a single tool call"""

    tool_name: str
    args: dict[str, Any]
    result: str
    timestamp: datetime = field(default_factory=datetime.now)
    error: str | None = None


@dataclass
class Scratchpad:
    """
    Scratchpad for tracking agent's tool calls and managing context.

    Similar to dexter's implementation, this helps:
    - Track all tool calls during a research session
    - Prevent duplicate/redundant calls
    - Manage context window by clearing old results when needed
    """

    query: str
    tool_calls: list[ToolCall] = field(default_factory=list)
    max_calls_per_tool: int = 3

    def add_call(
        self, tool_name: str, args: dict[str, Any], result: str, error: str | None = None
    ) -> None:
        """Record a tool call"""
        self.tool_calls.append(
            ToolCall(tool_name=tool_name, args=args, result=result, error=error)
        )

    def get_tool_call_count(self, tool_name: str) -> int:
        """Get number of times a tool has been called"""
        return sum(1 for tc in self.tool_calls if tc.tool_name == tool_name)

    def can_call_tool(self, tool_name: str, args: dict[str, Any] | None = None) -> tuple[bool, str]:
        """
        Check if a tool can be called (soft limit).

        Returns:
            (allowed, warning_message)
        """
        count = self.get_tool_call_count(tool_name)
        if count >= self.max_calls_per_tool:
            return False, f"已达到工具 {tool_name} 的调用上限 ({self.max_calls_per_tool} 次)"

        # Check for similar previous calls (exact same tool+args)
        if args:
            for tc in self.tool_calls:
                if tc.tool_name == tool_name and tc.args == args:
                    return False, f"工具 {tool_name} 已使用相同参数调用过"

        return True, ""

    def recent_failures_count(self, window: int = 4) -> int:
        """
        统计最近 window 次工具调用中「失败或无效」的次数。
        用于 loop detection：连续多次无进展则触发收尾。
        """
        failure_keywords = ("未找到", "失败", "超时", "无法获取", "跳过", "错误", "异常")
        recent = self.tool_calls[-window:] if len(self.tool_calls) >= window else self.tool_calls
        count = 0
        for tc in recent:
            if tc.error:
                count += 1
            else:
                text = (tc.result or "").strip()
                if not text or any(k in text for k in failure_keywords):
                    count += 1
        return count

    def is_loop_no_progress(
        self, window: int = 4, failure_threshold: int = 3
    ) -> bool:
        """
        判断是否疑似「无进展循环」：最近 window 次调用中至少有 failure_threshold 次失败/无效。
        """
        if len(self.tool_calls) < failure_threshold:
            return False
        return self.recent_failures_count(window=window) >= failure_threshold

    def get_context(self, max_results: int = 5) -> str:
        """Get formatted context from recent tool calls"""
        if not self.tool_calls:
            return ""

        recent_calls = self.tool_calls[-max_results:]
        context_parts = []

        for tc in recent_calls:
            if tc.error:
                context_parts.append(f"[{tc.tool_name}] 错误: {tc.error}")
            else:
                # Truncate very long results
                result = tc.result[:2000] + "..." if len(tc.result) > 2000 else tc.result
                context_parts.append(f"[{tc.tool_name}] {result}")

        return "\n\n".join(context_parts)

    def clear_old_results(self, keep_count: int = 3) -> int:
        """Clear old tool results to manage context, return number cleared"""
        if len(self.tool_calls) <= keep_count:
            return 0

        cleared = len(self.tool_calls) - keep_count
        self.tool_calls = self.tool_calls[-keep_count:]
        return cleared

    def get_summary(self) -> str:
        """Get a summary of all tool calls made"""
        if not self.tool_calls:
            return "尚未调用任何工具"

        summary_parts = [f"原始查询: {self.query}", f"工具调用次数: {len(self.tool_calls)}", ""]

        tool_counts: dict[str, int] = {}
        for tc in self.tool_calls:
            tool_counts[tc.tool_name] = tool_counts.get(tc.tool_name, 0) + 1

        summary_parts.append("工具使用统计:")
        for tool, count in tool_counts.items():
            summary_parts.append(f"  - {tool}: {count} 次")

        return "\n".join(summary_parts)

    def write_jsonl(self, file_path: str, final_answer: str | None = None) -> None:
        """
        将本次会话的工具调用轨迹写入 JSONL 文件。

        结构与 Dexter 类似，包含：
        - init: 原始查询
        - tool_result: 每次工具调用的入参、结果与错误
        - answer: 最终回答（如有）

        写盘失败时静默忽略，不影响正常对话流程。
        """
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            with path.open("w", encoding="utf-8") as f:
                # init 记录
                init_entry = {
                    "type": "init",
                    "timestamp": datetime.now().isoformat(),
                    "query": self.query,
                }
                f.write(json.dumps(init_entry, ensure_ascii=False) + "\n")

                # 工具调用记录
                for tc in self.tool_calls:
                    entry = {
                        "type": "tool_result",
                        "timestamp": tc.timestamp.isoformat(),
                        "toolName": tc.tool_name,
                        "args": tc.args,
                        "result": tc.result,
                        "error": tc.error,
                    }
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")

                # 最终回答（可选）
                if final_answer is not None:
                    answer_entry = {
                        "type": "answer",
                        "timestamp": datetime.now().isoformat(),
                        "content": final_answer,
                    }
                    f.write(json.dumps(answer_entry, ensure_ascii=False) + "\n")
        except Exception:
            # 日志写入失败不应影响正常流程
            return
