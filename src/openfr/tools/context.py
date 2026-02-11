"""
上下文管理和压缩。

智能管理对话上下文，避免 token 浪费。
"""

from langchain_core.messages import BaseMessage, ToolMessage, AIMessage
from typing import List


def compress_tool_results(messages: List[BaseMessage], max_length: int = 1000) -> List[BaseMessage]:
    """
    压缩工具结果消息，保留关键信息。

    Args:
        messages: 消息列表
        max_length: 单个工具结果的最大长度

    Returns:
        压缩后的消息列表
    """
    compressed = []

    for msg in messages:
        if isinstance(msg, ToolMessage):
            content = msg.content
            if len(content) > max_length:
                # 截断并添加省略标记
                compressed_content = content[:max_length] + f"\n\n... (已截断，原长度: {len(content)} 字符)"
                compressed.append(ToolMessage(
                    content=compressed_content,
                    tool_call_id=msg.tool_call_id
                ))
            else:
                compressed.append(msg)
        else:
            compressed.append(msg)

    return compressed


def summarize_tool_results(messages: List[BaseMessage]) -> str:
    """
    总结工具调用结果，生成简洁摘要。

    Args:
        messages: 消息列表

    Returns:
        工具调用摘要
    """
    tool_calls = []

    for msg in messages:
        if isinstance(msg, ToolMessage):
            # 提取工具名称（从 tool_call_id 或内容推断）
            content_preview = msg.content[:100] if len(msg.content) > 100 else msg.content
            tool_calls.append(f"- {content_preview}")

    if not tool_calls:
        return "无工具调用"

    return "已调用工具:\n" + "\n".join(tool_calls[:5])  # 最多显示 5 个


def remove_redundant_messages(messages: List[BaseMessage]) -> List[BaseMessage]:
    """
    移除冗余消息，保留关键信息。

    策略：
    1. 保留最近的 N 条消息
    2. 移除中间的工具调用详情，只保留摘要
    3. 保留所有用户消息和最终 AI 回复

    Args:
        messages: 消息列表

    Returns:
        精简后的消息列表
    """
    if len(messages) <= 10:
        return messages

    # 保留前 2 条（系统提示 + 用户问题）和最后 5 条
    keep_start = 2
    keep_end = 5

    result = messages[:keep_start]

    # 中间部分：只保留摘要
    middle = messages[keep_start:-keep_end]
    if middle:
        summary = summarize_tool_results(middle)
        result.append(AIMessage(content=f"[历史工具调用摘要]\n{summary}"))

    result.extend(messages[-keep_end:])

    return result


def estimate_token_count(messages: List[BaseMessage]) -> int:
    """
    估算消息列表的 token 数量。

    简单估算：中文 1 字符 ≈ 1.5 token，英文 1 词 ≈ 1.3 token

    Args:
        messages: 消息列表

    Returns:
        估算的 token 数量
    """
    total_chars = sum(len(msg.content) for msg in messages if hasattr(msg, 'content'))
    # 粗略估算：平均每字符 1.5 token
    return int(total_chars * 1.5)


def should_compress_context(messages: List[BaseMessage], max_tokens: int = 8000) -> bool:
    """
    判断是否需要压缩上下文。

    Args:
        messages: 消息列表
        max_tokens: 最大 token 数

    Returns:
        是否需要压缩
    """
    estimated = estimate_token_count(messages)
    return estimated > max_tokens
