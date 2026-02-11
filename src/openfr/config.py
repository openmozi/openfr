"""
Configuration management for OpenFR

支持的模型提供商:

国产模型:
- deepseek: DeepSeek，推理能力强、性价比高
- doubao: 豆包(字节跳动火山引擎)，Seed 深度思考系列，256k 上下文
- dashscope: DashScope(阿里云灵积)，通义千问商业版，稳定高并发
- zhipu: 智谱 AI，GLM-Z1/GLM-4 系列，清华技术团队
- modelscope: ModelScope(阿里云魔搭)，Qwen 开源版，有免费额度
- kimi: Kimi/Moonshot，K2.5 系列，长上下文支持
- stepfun: 阶跃星辰，Step-2/Step-1 系列，推理与多模态
- minimax: MiniMax，M2.1 系列，推理能力强

海外模型:
- openai: OpenAI，GPT-4o、GPT-4、GPT-3.5
- anthropic: Anthropic，Claude 系列
- openrouter: OpenRouter，聚合多家模型
- together: Together AI，开源模型托管
- groq: Groq，超快推理速度

本地部署:
- ollama: Ollama，本地运行开源模型

自定义:
- custom: 自定义 OpenAI 兼容接口
"""

import os
from dataclasses import dataclass, field
from typing import Literal

from dotenv import load_dotenv

load_dotenv()


# 所有支持的提供商
ModelProvider = Literal[
    # 国产模型
    "deepseek",
    "doubao",
    "dashscope",
    "zhipu",
    "modelscope",
    "kimi",
    "stepfun",
    "minimax",
    # 海外模型
    "openai",
    "anthropic",
    "openrouter",
    "together",
    "groq",
    # 本地部署
    "ollama",
    # 自定义
    "custom",
]


# 提供商配置信息
PROVIDER_CONFIG = {
    # 国产模型
    "deepseek": {
        "env_key": "DEEPSEEK_API_KEY",
        "base_url": "https://api.deepseek.com/v1",
        "default_model": "deepseek-chat",
        "description": "DeepSeek，推理能力强、性价比高",
    },
    "doubao": {
        "env_key": "DOUBAO_API_KEY",
        "base_url": "https://ark.cn-beijing.volces.com/api/v3",
        "default_model": "doubao-1-5-pro-256k",
        "description": "豆包(火山引擎)，Seed 深度思考系列，256k 上下文",
    },
    "dashscope": {
        "env_key": "DASHSCOPE_API_KEY",
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "default_model": "qwen-max",
        "description": "DashScope(阿里云灵积)，通义千问商业版",
    },
    "zhipu": {
        "env_key": "ZHIPU_API_KEY",
        "base_url": "https://open.bigmodel.cn/api/paas/v4",
        "default_model": "glm-4.7",
        "description": "智谱 AI，GLM-Z1/GLM-4 系列",
    },
    "modelscope": {
        "env_key": "MODELSCOPE_API_KEY",
        "base_url": "https://api-inference.modelscope.cn/v1",
        "default_model": "qwen2.5-72b-instruct",
        "description": "ModelScope(魔搭社区)，Qwen 开源版",
    },
    "kimi": {
        "env_key": "KIMI_API_KEY",
        "base_url": "https://api.moonshot.cn/v1",
        "default_model": "moonshot-v1-128k",
        "description": "Kimi/Moonshot，长上下文支持",
    },
    "stepfun": {
        "env_key": "STEPFUN_API_KEY",
        "base_url": "https://api.stepfun.com/v1",
        "default_model": "step-2-16k",
        "description": "阶跃星辰，Step-2/Step-1 系列",
    },
    "minimax": {
        "env_key": "MINIMAX_API_KEY",
        "base_url": "https://api.minimax.chat/v1",
        "default_model": "MiniMax-Text-01",
        "description": "MiniMax，M2.1 系列",
    },
    # 海外模型
    "openai": {
        "env_key": "OPENAI_API_KEY",
        "base_url": None,  # 使用默认
        "default_model": "gpt-4o",
        "description": "OpenAI，GPT-4o、GPT-4、GPT-3.5",
    },
    "anthropic": {
        "env_key": "ANTHROPIC_API_KEY",
        "base_url": None,
        "default_model": "claude-sonnet-4-20250514",
        "description": "Anthropic，Claude 系列",
    },
    "openrouter": {
        "env_key": "OPENROUTER_API_KEY",
        "base_url": "https://openrouter.ai/api/v1",
        "default_model": "anthropic/claude-sonnet-4",
        "description": "OpenRouter，聚合多家模型",
    },
    "together": {
        "env_key": "TOGETHER_API_KEY",
        "base_url": "https://api.together.xyz/v1",
        "default_model": "meta-llama/Llama-3.3-70B-Instruct-Turbo",
        "description": "Together AI，开源模型托管",
    },
    "groq": {
        "env_key": "GROQ_API_KEY",
        "base_url": "https://api.groq.com/openai/v1",
        "default_model": "llama-3.3-70b-versatile",
        "description": "Groq，超快推理速度",
    },
    # 本地部署
    "ollama": {
        "env_key": "OLLAMA_BASE_URL",
        "base_url": "http://localhost:11434",
        "default_model": "qwen2.5:14b",
        "description": "Ollama，本地运行开源模型",
    },
    # 自定义
    "custom": {
        "env_key": "CUSTOM_API_KEY",
        "base_url": None,  # 需要通过 CUSTOM_BASE_URL 设置
        "default_model": "",
        "description": "自定义 OpenAI 兼容接口",
    },
}


@dataclass
class Config:
    """Configuration for the Financial Research Agent"""

    # LLM settings
    provider: ModelProvider = "zhipu"
    model: str = ""  # 留空则使用提供商默认模型
    temperature: float = 0.0
    max_tokens: int = 4096

    # 自定义提供商设置 (当 provider="custom" 时使用)
    custom_base_url: str = ""
    custom_api_key: str = ""

    # Agent settings
    max_iterations: int = 10
    verbose: bool = True
    # 先规划任务再执行（Dexter 风格）：先拆解为步骤列表，再按步骤依次调用工具
    enable_plan_execute: bool = True
    # 自校验：在给出最终回答前先检查数据是否充分（Dexter 风格）
    enable_self_validation: bool = True
    # 无进展循环检测：近期多次失败/无效则强制收尾
    enable_loop_detection: bool = True
    # 单次会话最大工具调用总数，超过则强制收尾
    max_total_tool_calls: int = 14

    # Performance settings
    # 是否允许并行执行同一轮中的多个工具调用（线程并行）。
    # 注意：部分数据源（如同花顺）可能通过 libmini_racer 执行 JS，线程并行可能导致崩溃。
    enable_parallel_tools: bool = True
    # 是否允许在工具内部并行尝试多个数据源（线程并行）。
    # 默认关闭以避免 libmini_racer 相关的非线程安全问题。
    enable_parallel_sources: bool = True

    # Tool settings
    enable_stock_tools: bool = True
    enable_stock_hk_tools: bool = True
    enable_fund_tools: bool = True
    enable_futures_tools: bool = True
    enable_macro_tools: bool = True
    enable_index_tools: bool = True

    # Logging / debug settings
    # 是否将每次会话的 Scratchpad 记录落盘为 JSONL（类似 Dexter）
    log_scratchpad: bool = False
    # 自定义日志目录；为空则默认使用 ~/.openfr/scratchpad
    log_dir: str = ""

    def __post_init__(self):
        """设置默认模型"""
        if not self.model and self.provider in PROVIDER_CONFIG:
            self.model = PROVIDER_CONFIG[self.provider]["default_model"]

    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables"""
        raw_provider = os.getenv("OPENFR_PROVIDER", "zhipu")
        provider = raw_provider if raw_provider in PROVIDER_CONFIG else "zhipu"
        model = os.getenv("OPENFR_MODEL", "")

        return cls(
            provider=provider,  # type: ignore
            model=model,
            temperature=float(os.getenv("OPENFR_TEMPERATURE", "0.0")),
            max_tokens=int(os.getenv("OPENFR_MAX_TOKENS", "4096")),
            max_iterations=int(os.getenv("OPENFR_MAX_ITERATIONS", "10")),
            verbose=os.getenv("OPENFR_VERBOSE", "true").lower() == "true",
            enable_plan_execute=os.getenv("OPENFR_ENABLE_PLAN_EXECUTE", "true").lower() == "true",
            enable_self_validation=os.getenv("OPENFR_ENABLE_SELF_VALIDATION", "true").lower() == "true",
            enable_loop_detection=os.getenv("OPENFR_ENABLE_LOOP_DETECTION", "true").lower() == "true",
            max_total_tool_calls=int(os.getenv("OPENFR_MAX_TOTAL_TOOL_CALLS", "14")),
            enable_parallel_tools=os.getenv("OPENFR_ENABLE_PARALLEL_TOOLS", "true").lower() == "true",
            enable_parallel_sources=os.getenv("OPENFR_ENABLE_PARALLEL_SOURCES", "true").lower() == "true",
            enable_stock_tools=os.getenv("OPENFR_ENABLE_STOCK_TOOLS", "true").lower() == "true",
            enable_stock_hk_tools=os.getenv("OPENFR_ENABLE_STOCK_HK_TOOLS", "true").lower() == "true",
            enable_fund_tools=os.getenv("OPENFR_ENABLE_FUND_TOOLS", "true").lower() == "true",
            enable_futures_tools=os.getenv("OPENFR_ENABLE_FUTURES_TOOLS", "true").lower() == "true",
            enable_macro_tools=os.getenv("OPENFR_ENABLE_MACRO_TOOLS", "true").lower() == "true",
            enable_index_tools=os.getenv("OPENFR_ENABLE_INDEX_TOOLS", "true").lower() == "true",
            log_scratchpad=os.getenv("OPENFR_LOG_SCRATCHPAD", "false").lower() == "true",
            log_dir=os.getenv("OPENFR_SCRATCHPAD_DIR", ""),
            custom_base_url=os.getenv("CUSTOM_BASE_URL", ""),
            custom_api_key=os.getenv("CUSTOM_API_KEY", ""),
        )

    @classmethod
    def custom(cls, base_url: str, api_key: str, model: str, **kwargs) -> "Config":
        """创建自定义提供商配置

        Args:
            base_url: API 基础 URL
            api_key: API Key
            model: 模型名称
            **kwargs: 其他配置参数

        Example:
            config = Config.custom(
                base_url="https://models-proxy.stepfun-inc.com/v1",
                api_key="your-api-key",
                model="minimax-m2.1"
            )
        """
        return cls(
            provider="custom",
            model=model,
            custom_base_url=base_url,
            custom_api_key=api_key,
            **kwargs,
        )

    def get_api_key(self) -> str:
        """获取当前提供商的 API Key"""
        # 自定义提供商
        if self.provider == "custom":
            return self.custom_api_key or os.getenv("CUSTOM_API_KEY", "")

        if self.provider not in PROVIDER_CONFIG:
            return ""
        env_key = PROVIDER_CONFIG[self.provider]["env_key"]
        return os.getenv(env_key, "")

    def get_base_url(self) -> str | None:
        """获取当前提供商的 Base URL"""
        # 自定义提供商
        if self.provider == "custom":
            return self.custom_base_url or os.getenv("CUSTOM_BASE_URL", "")

        if self.provider not in PROVIDER_CONFIG:
            return None

        base_url = PROVIDER_CONFIG[self.provider]["base_url"]

        # Ollama 支持自定义 URL
        if self.provider == "ollama":
            return os.getenv("OLLAMA_BASE_URL", base_url)

        return base_url

    def get_model_name(self) -> str:
        """获取完整的模型名称"""
        return self.model or PROVIDER_CONFIG.get(self.provider, {}).get("default_model", "")

    @staticmethod
    def list_providers() -> list[dict]:
        """列出所有支持的提供商"""
        providers = []
        for name, config in PROVIDER_CONFIG.items():
            providers.append({
                "name": name,
                "env_key": config["env_key"],
                "default_model": config["default_model"],
                "description": config["description"],
                "configured": bool(os.getenv(config["env_key"], "")),
            })
        return providers
