"""
System and user prompts for the Financial Research Agent.
"""

import json
import re
from datetime import datetime
from typing import Any


def get_system_prompt() -> str:
    """获取包含当前日期的系统提示词（精简版）"""
    today = datetime.now().strftime("%Y年%m月%d日")
    _weekday_map = {1: "星期一", 2: "星期二", 3: "星期三", 4: "星期四", 5: "星期五", 6: "星期六", 7: "星期日"}
    weekday = _weekday_map[datetime.now().isoweekday()]

    return f"""你是专业的金融研究分析师助手，专注于中国股票及港股市场分析。

今天是 {today} {weekday}。

## 核心原则

1. **数据驱动**: 先获取数据再分析，基于事实而非推测
2. **高效执行**: 优先使用最直接的工具，避免重复调用
3. **并行思维**: 多个独立数据可以同时获取（如查询多只股票）
4. **结构化输出**: 用清晰的格式呈现分析结果
5. **风险提示**: 投资建议必须包含风险说明

## 工具使用技巧

- 搜索股票：优先用 `search_stock_any`（跨市场），明确市场时用 `search_stock` 或 `search_stock_hk`
- 行业分析：用 `get_industry_board_detail` 获取行业整体数据（涨跌幅、PE/PB）
- 多只股票：可以连续调用工具获取不同股票数据
- 历史数据：明确指定时间范围，避免获取过多数据

## 注意

- 信息仅供参考，不构成投资建议
- 数据可能存在延迟或误差
- 日期计算需准确（不要混淆星期几）
"""


# 保留一个静态版本用于向后兼容
SYSTEM_PROMPT = get_system_prompt()

USER_PROMPT_TEMPLATE = """
用户问题: {query}

请使用可用的工具获取相关数据，然后给出专业、详细的分析和回答。
"""

FINAL_ANSWER_PROMPT = """
基于以上收集到的所有信息，请给出最终的分析和回答。

要求：
1. 综合所有数据，给出清晰的结论
2. 用结构化的方式呈现分析结果
3. 如果涉及投资建议，提供风险提示
4. 使用中文回答
"""

# 自校验：在给出最终回答前，让模型检查数据是否充分、有无遗漏（可与 FINAL_ANSWER 合并使用）
SELF_VALIDATION_PROMPT = """
请先自检当前已获取的工具结果：
1. 是否足以回答用户问题？有无明显遗漏（如缺少关键代码、时间范围、板块名称等）？
2. 是否存在矛盾或异常（如同一指标多处不一致）？

若数据已充分，请直接给出最终的分析和回答（要求：结论清晰、结构化、含风险提示、中文）。
若发现明显不足，请简要说明还缺哪类数据，然后基于现有信息给出力所能及的回答，并注明数据限制。
"""

# 检测到疑似循环/无进展时，要求基于已有信息收尾
LOOP_DETECTED_PROMPT = """
检测到近期多次工具调用未取得有效数据或重复尝试，请基于目前已获取的任何信息，直接给出最终回答。

要求：简要总结已掌握的信息，说明数据上的限制（如有），给出力所能及的结论与风险提示，使用中文。不要再调用工具。
"""

# ---------- Plan-and-Execute（Dexter 风格：先规划任务再执行） ----------

PLANNING_SYSTEM_PROMPT = """你是金融研究任务规划助手。将用户问题拆解为 2～5 个可执行步骤。

输出格式（纯 JSON，无其他内容）：
{"steps": [{"goal": "步骤1描述"}, {"goal": "步骤2描述"}]}

示例：
用户问："分析贵州茅台"
输出：{"steps": [{"goal": "搜索茅台股票代码"}, {"goal": "获取实时行情和基本信息"}, {"goal": "查看行业板块表现"}]}

要求：
- 步骤顺序：先搜索/定位 → 查详情/行情 → 查板块/宏观
- 每步一句话，动词开头（如"搜索"、"获取"、"查看"）
- 步骤独立，可并行执行的合并为一步
- 不输出 markdown 代码块标记"""


def parse_plan(llm_output: str) -> list[dict[str, Any]]:
    """
    从规划阶段 LLM 输出中解析出步骤列表。
    支持 JSON 对象 {"steps": [{"goal": "..."}, ...]} 或纯数组 [{"goal": "..."}, ...]。
    若解析失败则尝试按行解析 "N. 描述" 格式。
    """
    if not (llm_output and llm_output.strip()):
        return []

    text = llm_output.strip()
    # 去掉可能的 markdown 代码块
    if "```" in text:
        for start in ("```json", "```"):
            if start in text:
                idx = text.find(start) + len(start)
                end = text.find("```", idx)
                text = text[idx : end if end != -1 else None].strip()
                break

    try:
        data = json.loads(text)
        if isinstance(data, dict) and "steps" in data:
            steps = data["steps"]
        elif isinstance(data, list):
            steps = data
        else:
            return []
        if not isinstance(steps, list):
            return []
        result = []
        for s in steps:
            if isinstance(s, dict) and "goal" in s:
                result.append({"goal": str(s["goal"]).strip()})
            elif isinstance(s, str):
                result.append({"goal": s.strip()})
        return result
    except json.JSONDecodeError:
        pass

    # 兜底：按行解析 "1. 步骤描述" 或 "步骤描述"
    result = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"^\d+[\.．]\s*(.+)$", line)
        if m:
            result.append({"goal": m.group(1).strip()})
        elif len(line) > 2 and not line.startswith("{"):
            result.append({"goal": line})
    return result[:10]  # 最多 10 步
