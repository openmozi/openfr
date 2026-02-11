"""
性能监控和分析工具。

用于跟踪工具调用性能、识别瓶颈。
"""

import time
from dataclasses import dataclass, field
from typing import Any
from collections import defaultdict


@dataclass
class PerformanceMetrics:
    """性能指标"""

    tool_call_times: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    tool_call_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    cache_hits: int = 0
    cache_misses: int = 0

    def record_call(self, tool_name: str, duration: float) -> None:
        """记录工具调用时间"""
        self.tool_call_times[tool_name].append(duration)
        self.tool_call_counts[tool_name] += 1

    def record_cache_hit(self) -> None:
        """记录缓存命中"""
        self.cache_hits += 1

    def record_cache_miss(self) -> None:
        """记录缓存未命中"""
        self.cache_misses += 1

    def get_average_time(self, tool_name: str) -> float:
        """获取工具平均调用时间"""
        times = self.tool_call_times.get(tool_name, [])
        return sum(times) / len(times) if times else 0.0

    def get_total_time(self, tool_name: str) -> float:
        """获取工具总调用时间"""
        return sum(self.tool_call_times.get(tool_name, []))

    def get_cache_hit_rate(self) -> float:
        """获取缓存命中率"""
        total = self.cache_hits + self.cache_misses
        return self.cache_hits / total if total > 0 else 0.0

    def get_summary(self) -> str:
        """获取性能摘要"""
        lines = ["性能统计:", ""]

        # 工具调用统计
        if self.tool_call_counts:
            lines.append("工具调用次数:")
            for tool, count in sorted(self.tool_call_counts.items(), key=lambda x: -x[1]):
                avg_time = self.get_average_time(tool)
                total_time = self.get_total_time(tool)
                lines.append(f"  {tool}: {count}次, 平均{avg_time:.2f}s, 总计{total_time:.2f}s")
            lines.append("")

        # 缓存统计
        if self.cache_hits + self.cache_misses > 0:
            hit_rate = self.get_cache_hit_rate()
            lines.append(f"缓存命中率: {hit_rate:.1%} ({self.cache_hits}/{self.cache_hits + self.cache_misses})")

        return "\n".join(lines)


# 全局性能指标实例
_global_metrics = PerformanceMetrics()


def get_metrics() -> PerformanceMetrics:
    """获取全局性能指标"""
    return _global_metrics


def reset_metrics() -> None:
    """重置性能指标"""
    global _global_metrics
    _global_metrics = PerformanceMetrics()


def timed_call(func, *args, **kwargs) -> tuple[Any, float]:
    """
    执行函数并计时。

    Returns:
        (result, duration) 元组
    """
    start = time.time()
    result = func(*args, **kwargs)
    duration = time.time() - start
    return result, duration
