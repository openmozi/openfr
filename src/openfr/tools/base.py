"""
Base utilities for tools.
"""

import time
import pandas as pd
from datetime import datetime
from functools import wraps
from typing import Callable, Any
import logging

logger = logging.getLogger(__name__)


def retry_on_network_error(max_retries: int = 3, base_delay: float = 1.0, silent: bool = False):
    """
    装饰器：在网络错误时自动重试。

    Args:
        max_retries: 最大重试次数
        base_delay: 基础延迟时间（秒），使用指数退避
        silent: 是否静默模式（不输出日志）
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries):
                try:
                    # 添加小延迟避免请求过快
                    if attempt > 0:
                        delay = base_delay * (2 ** (attempt - 1))  # 指数退避
                        if not silent:
                            logger.info(f"重试 {func.__name__} (尝试 {attempt + 1}/{max_retries})，等待 {delay:.1f}s...")
                        time.sleep(delay)

                    return func(*args, **kwargs)

                except (ConnectionError, TimeoutError) as e:
                    # 明确的网络错误类型
                    last_exception = e
                    if attempt < max_retries - 1:
                        if not silent:
                            logger.warning(f"网络错误: {str(e)[:100]}，准备重试...")
                        continue
                    break
                except Exception as e:
                    last_exception = e
                    error_str = str(e).lower()

                    # 检查是否是网络相关错误
                    network_errors = [
                        'connection', 'remote', 'timeout', 'timed out',
                        'disconnect', 'network', 'unreachable', 'refused'
                    ]

                    is_network_error = any(err in error_str for err in network_errors)

                    if is_network_error and attempt < max_retries - 1:
                        if not silent:
                            logger.warning(f"网络错误: {str(e)[:100]}，准备重试...")
                        continue
                    else:
                        # 如果不是网络错误或已达到最大重试次数，直接抛出
                        break

            # 所有重试都失败，抛出最后一个异常
            if last_exception:
                raise last_exception

        return wrapper
    return decorator


def format_dataframe(df: pd.DataFrame, max_rows: int = 30) -> str:
    """
    Format a DataFrame for display in text output.

    Does not modify the input DataFrame.

    Args:
        df: The DataFrame to format
        max_rows: Maximum number of rows to display

    Returns:
        Formatted string representation
    """
    if df.empty:
        return "(无数据)"

    total_rows = len(df)
    display_df = df.head(max_rows) if total_rows > max_rows else df
    truncated = total_rows > max_rows

    result = display_df.to_string(index=False, max_colwidth=50)

    if truncated:
        result += f"\n\n... (显示前 {max_rows} 条，共 {total_rows} 条)"

    return result


def validate_stock_code(code: str) -> str:
    """
    Validate and normalize stock code.

    Args:
        code: Stock code (e.g., "000001", "SH600519", "600519.SH")

    Returns:
        Normalized 6-digit stock code
    """
    # Remove common prefixes/suffixes
    code = code.upper().strip()
    code = code.replace("SH", "").replace("SZ", "").replace("BJ", "")
    code = code.replace(".", "").replace("-", "")

    # Ensure 6 digits
    if len(code) < 6:
        code = code.zfill(6)

    return code[:6]


def validate_date(date_str: str) -> str:
    """
    Validate and normalize date string.

    Args:
        date_str: Date string in various formats

    Returns:
        Date in YYYYMMDD format
    """
    # Remove common separators
    date_str = date_str.replace("-", "").replace("/", "").replace(".", "")

    # Validate format
    if len(date_str) == 8:
        try:
            datetime.strptime(date_str, "%Y%m%d")
            return date_str
        except ValueError:
            pass

    raise ValueError(f"Invalid date format: {date_str}. Expected YYYYMMDD")
