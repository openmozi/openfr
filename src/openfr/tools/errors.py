"""
自定义异常类型。

提供更精确的错误处理和错误信息。
"""


class OpenFRError(Exception):
    """OpenFR 基础异常类"""
    pass


class DataFetchError(OpenFRError):
    """数据获取失败异常"""
    def __init__(self, source: str, reason: str):
        self.source = source
        self.reason = reason
        super().__init__(f"从 {source} 获取数据失败: {reason}")


class InvalidParameterError(OpenFRError):
    """参数验证失败异常"""
    def __init__(self, param_name: str, param_value: str, reason: str):
        self.param_name = param_name
        self.param_value = param_value
        self.reason = reason
        super().__init__(f"参数 {param_name}={param_value} 无效: {reason}")


class StockNotFoundError(OpenFRError):
    """股票未找到异常"""
    def __init__(self, query: str):
        self.query = query
        super().__init__(f"未找到股票: {query}")


class TimeoutError(OpenFRError):
    """操作超时异常"""
    def __init__(self, operation: str, timeout: float):
        self.operation = operation
        self.timeout = timeout
        super().__init__(f"操作 {operation} 超时 ({timeout}s)")
