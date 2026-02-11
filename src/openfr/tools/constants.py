"""
工具模块的常量配置。

集中管理超时时间、缓存配置、重试参数等。
"""

# ==================== 缓存配置 ====================
# 股票列表缓存时间（秒）
STOCK_LIST_CACHE_TTL = 6 * 60 * 60  # 6小时

# 行情数据缓存时间（秒）
STOCK_SPOT_CACHE_TTL = 60  # 1分钟

# 板块数据缓存时间（秒）
BOARD_CACHE_TTL = 5 * 60  # 5分钟


# ==================== 超时配置 ====================
# 个股详情接口超时（秒）
STOCK_INFO_TIMEOUT = 6

# 概念成分股总超时（秒）
CONCEPT_STOCKS_TOTAL_TIMEOUT = 8.0

# 通用网络请求超时（秒）
DEFAULT_REQUEST_TIMEOUT = 8


# ==================== 重试配置 ====================
# 默认最大重试次数
DEFAULT_MAX_RETRIES = 2

# 东方财富接口重试次数（易断连）
EM_MAX_RETRIES = 3

# 重试基础延迟（秒）
DEFAULT_RETRY_DELAY = 1.0


# ==================== 数据展示配置 ====================
# DataFrame 最大显示行数
MAX_DISPLAY_ROWS = 30

# 搜索结果最大返回数
MAX_SEARCH_RESULTS = 20

# 板块列表最大返回数
MAX_BOARD_RESULTS = 50


# ==================== 网络错误关键词 ====================
NETWORK_ERROR_KEYWORDS = [
    'connection', 'remote', 'timeout', 'timed out',
    'disconnect', 'network', 'unreachable', 'refused'
]


# ==================== 东方财富字段映射 ====================
# 财务分析指标字段映射
EM_FINANCE_FIELD_MAP = {
    "roe": ["ROEJQ", "ROEKCJQ", "ROE_AVG"],  # 净资产收益率
    "rev_g": ["TOTALOPERATEREVETZ", "YYSRTB"],  # 营收同比增长
    "prof_g": ["PARENTNETPROFITTZ", "JLRTB"],   # 净利润同比增长
}


# ==================== 行业别名映射 ====================
INDUSTRY_ALIAS_MAP = {
    "白酒": "酿酒",
    "锂电": "能源金属",
    "光伏": "光伏设备",
    "芯片": "半导体",
}
