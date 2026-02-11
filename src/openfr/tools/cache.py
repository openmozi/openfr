"""
统一的缓存管理模块。

提供全局缓存机制，避免重复的网络请求，提升性能。
"""

import time
from typing import Any, Callable, TypeVar
from functools import wraps
import pandas as pd

T = TypeVar('T')


class CacheEntry:
    """缓存条目"""
    def __init__(self, value: Any, ttl: float):
        self.value = value
        self.timestamp = time.time()
        self.ttl = ttl

    def is_expired(self) -> bool:
        """检查是否过期"""
        return time.time() - self.timestamp > self.ttl


class SimpleCache:
    """简单的内存缓存"""

    def __init__(self):
        self._cache: dict[str, CacheEntry] = {}

    def get(self, key: str) -> Any | None:
        """获取缓存值"""
        entry = self._cache.get(key)
        if entry is None:
            return None

        if entry.is_expired():
            del self._cache[key]
            return None

        return entry.value

    def set(self, key: str, value: Any, ttl: float) -> None:
        """设置缓存值"""
        self._cache[key] = CacheEntry(value, ttl)

    def clear(self) -> None:
        """清空缓存"""
        self._cache.clear()

    def remove_expired(self) -> int:
        """移除过期条目，返回移除数量"""
        expired_keys = [
            key for key, entry in self._cache.items()
            if entry.is_expired()
        ]
        for key in expired_keys:
            del self._cache[key]
        return len(expired_keys)


# 全局缓存实例
_global_cache = SimpleCache()


def cached(ttl: float = 300.0, key_func: Callable[..., str] | None = None):
    """
    缓存装饰器。

    Args:
        ttl: 缓存有效期（秒）
        key_func: 自定义缓存键生成函数
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # 生成缓存键
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                # 默认使用函数名和参数生成键
                args_str = "_".join(str(arg) for arg in args)
                kwargs_str = "_".join(f"{k}={v}" for k, v in sorted(kwargs.items()))
                cache_key = f"{func.__name__}_{args_str}_{kwargs_str}"

            # 尝试从缓存获取
            cached_value = _global_cache.get(cache_key)
            if cached_value is not None:
                return cached_value

            # 执行函数并缓存结果
            result = func(*args, **kwargs)
            _global_cache.set(cache_key, result, ttl)
            return result

        return wrapper
    return decorator


def get_cache() -> SimpleCache:
    """获取全局缓存实例"""
    return _global_cache


def clear_cache() -> None:
    """清空全局缓存"""
    _global_cache.clear()
