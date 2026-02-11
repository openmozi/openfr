"""
Integration tests for OpenFR.

使用 .env 配置的真实 LLM 与真实工具执行，不 mock 数据和 LLM。
需配置 .env 与有效 API Key，未配置时相关用例自动 skip。
"""

from pathlib import Path

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from openfr import FinancialResearchAgent, Config
from openfr.tools import get_all_tools

# 项目根目录（tests 的上级）
_REPO_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _REPO_ROOT / ".env"

# 真实 LLM 单次 run 最多收集的事件数，避免无限等待
_MAX_EVENTS_REAL_RUN = 120


def _load_env_and_skip_if_no_key():
    """加载 .env，若未配置有效 API Key 则 pytest.skip。返回 Config。"""
    if load_dotenv is None:
        pytest.skip("python-dotenv 未安装")
    if _ENV_FILE.exists():
        load_dotenv(_ENV_FILE, override=True)
    config = Config.from_env()
    api_key = config.get_api_key()
    if not api_key or api_key.startswith("your_") or "here" in api_key.lower():
        pytest.skip("未在 .env 中配置有效 API Key（当前提供商: " + config.provider + "）")
    return config


class TestToolIntegration:
    """Integration tests for tools."""

    @pytest.mark.integration
    def test_stock_tools_return_valid_format(self):
        """Test that stock tools return properly formatted strings."""
        from openfr.tools.stock import get_hot_stocks

        with patch("openfr.tools.stock.ak") as mock_ak:
            mock_ak.stock_hot_rank_em.return_value = pd.DataFrame({
                "代码": ["000001"],
                "股票名称": ["平安银行"],
                "最新价": [10.5],
                "涨跌幅": [1.23],
            })

            result = get_hot_stocks.invoke({})
            assert isinstance(result, str)
            assert len(result) > 0

    @pytest.mark.integration
    def test_index_tools_return_valid_format(self):
        """Test that index tools return properly formatted strings."""
        from openfr.tools.index import get_index_realtime

        with patch("openfr.tools.index.ak") as mock_ak:
            mock_ak.stock_zh_index_spot_em.return_value = pd.DataFrame({
                "代码": ["000001"],
                "名称": ["上证指数"],
                "最新价": [3200.0],
                "涨跌幅": [0.5],
            })

            result = get_index_realtime.invoke({})
            assert isinstance(result, str)
            assert len(result) > 0

    @pytest.mark.integration
    def test_futures_realtime_returns_data(self):
        """期货实时行情：新浪接口若列数异常则走 fallback，应返回有效行情字符串。"""
        from openfr.tools.futures import get_futures_realtime, _fetch_futures_spot

        df = _fetch_futures_spot()
        assert not df.empty, "期货实时数据不应为空"
        assert "symbol" in df.columns and "name" in df.columns, "应包含 symbol/name 列"
        result = get_futures_realtime.invoke({})
        assert isinstance(result, str)
        assert "期货" in result
        assert "行情" in result or "symbol" in result


class TestAgentIntegration:
    """使用 .env 配置的真实 LLM 与真实工具，无 mock。"""

    @pytest.mark.integration
    @pytest.mark.slow
    @pytest.mark.timeout(180)
    def test_agent_processes_events_correctly(self):
        """真实执行：.env 的 LLM + 工具，断言事件顺序为 thinking → plan → … → answer。"""
        config = _load_env_and_skip_if_no_key()
        agent = FinancialResearchAgent(config)
        events = []
        for ev in agent.run("上证指数当前多少？"):
            events.append(ev)
            if ev.get("type") == "answer":
                break
            if len(events) >= _MAX_EVENTS_REAL_RUN:
                break

        types = [e["type"] for e in events]
        assert "thinking" in types, "应有 thinking 事件"
        assert "plan" in types, "应有 plan 事件"
        assert "answer" in types, f"应在 {_MAX_EVENTS_REAL_RUN} 步内得到 answer，当前事件数: {len(events)}"
        assert events[-1]["type"] == "answer"
        assert len(events[-1].get("content", "")) > 0

    @pytest.mark.integration
    @pytest.mark.slow
    @pytest.mark.timeout(180)
    def test_agent_handles_tool_errors_gracefully(self):
        """真实执行：询问可能触发工具失败或未找到的问题，Agent 应正常收尾并返回 answer。"""
        config = _load_env_and_skip_if_no_key()
        agent = FinancialResearchAgent(config)
        events = []
        for ev in agent.run("查询股票代码 000000 的实时行情"):
            events.append(ev)
            if ev.get("type") == "answer":
                break
            if len(events) >= _MAX_EVENTS_REAL_RUN:
                break

        assert events[-1]["type"] == "answer", f"应以 answer 收尾，当前最后事件: {events[-1].get('type')}"
        tool_ends = [e for e in events if e.get("type") == "tool_end"]
        if tool_ends:
            result = tool_ends[0].get("result", "")
            assert (
                "未找到" in result or "失败" in result or "数据" in result or len(result) > 0
            ), f"工具结果应含未找到/失败/数据或非空，得到: {result[:100]}"


class TestConfigIntegration:
    """Integration tests for configuration."""

    @pytest.mark.integration
    def test_config_with_all_providers(self):
        """Test that config works with all providers."""
        providers = [
            "deepseek", "doubao", "dashscope", "zhipu", "modelscope",
            "kimi", "stepfun", "minimax", "openai", "anthropic",
            "openrouter", "together", "groq", "ollama", "modelsproxy",
        ]

        for provider in providers:
            config = Config(provider=provider)
            assert config.provider == provider
            assert config.get_model_name() != ""  # Should have default model

    @pytest.mark.integration
    def test_custom_config_integration(self):
        """Test custom provider configuration."""
        config = Config.custom(
            base_url="https://test-api.com/v1",
            api_key="test-key",
            model="test-model",
        )

        assert config.provider == "custom"
        assert config.get_base_url() == "https://test-api.com/v1"
        assert config.get_api_key() == "test-key"
        assert config.get_model_name() == "test-model"


class TestEndToEnd:
    """端到端：.env 配置的真实 LLM + 真实工具，无 mock。"""

    @pytest.mark.integration
    @pytest.mark.slow
    @pytest.mark.timeout(180)
    def test_full_query_flow(self):
        """真实执行：用 .env 的 LLM 与工具跑完「今天上证指数怎么样？」并得到回答与工具调用结果。"""
        config = _load_env_and_skip_if_no_key()
        agent = FinancialResearchAgent(config)
        events = []
        for ev in agent.run("今天上证指数怎么样？"):
            events.append(ev)
            if ev.get("type") == "answer":
                break
            if len(events) >= _MAX_EVENTS_REAL_RUN:
                break

        answer_ev = next((e for e in events if e.get("type") == "answer"), None)
        assert answer_ev is not None, (
            f"应在 {_MAX_EVENTS_REAL_RUN} 步内得到 answer，当前事件数: {len(events)}"
        )
        assert len(answer_ev.get("content", "")) > 0

        tool_ends = [e for e in events if e.get("type") == "tool_end"]
        assert len(tool_ends) >= 1, "应至少有一次真实工具调用"
        assert len(tool_ends[0].get("result", "")) > 0, "工具应返回非空结果"


class TestEnvIntegration:
    """使用 .env 配置的集成测试：加载项目根目录 .env，用 Config.from_env() 构建 Agent 并验证流程。"""

    @pytest.fixture(autouse=False)
    def load_dotenv_from_repo_root(self):
        """确保从项目根目录加载 .env，不依赖 conftest 的 mock 覆盖。"""
        if load_dotenv is None:
            pytest.skip("python-dotenv 未安装，跳过 .env 集成测试")
        if _ENV_FILE.exists():
            load_dotenv(_ENV_FILE, override=True)
        yield

    @pytest.mark.integration
    def test_config_from_env_builds(self, load_dotenv_from_repo_root):
        """使用 .env 构建 Config：Config.from_env() 应能正确读取 OPENFR_* 等变量。"""
        config = Config.from_env()
        assert config.provider is not None
        assert config.get_model_name() != ""
        # 常见 .env 中会配置的项
        assert config.max_iterations >= 1
        assert config.max_total_tool_calls >= 1

    @pytest.mark.integration
    def test_agent_from_env_config_has_tools(self, load_dotenv_from_repo_root):
        """使用 .env 配置创建 Agent，应能正确挂载工具。"""
        config = Config.from_env()
        agent = FinancialResearchAgent(config)
        assert len(agent.tools) > 0
        assert agent._get_tool_by_name("get_stock_realtime") is not None
        assert agent._get_tool_by_name("search_stock_any") is not None

    @pytest.mark.integration
    @pytest.mark.slow
    @pytest.mark.timeout(180)
    def test_full_flow_with_env_config(self):
        """使用 .env 的 LLM 与工具跑「贵州茅台适合买入吗」完整流程（规划 → 执行 → 回答）。"""
        config = _load_env_and_skip_if_no_key()
        agent = FinancialResearchAgent(config)
        event_list = []
        for ev in agent.run("贵州茅台适合买入吗"):
            event_list.append(ev)
            if ev.get("type") == "answer":
                break
            if len(event_list) >= _MAX_EVENTS_REAL_RUN:
                break

        types = [e["type"] for e in event_list]
        assert "thinking" in types
        assert "plan" in types
        assert "answer" in types, f"应在 {_MAX_EVENTS_REAL_RUN} 步内得到 answer，事件数: {len(event_list)}"
        plan_events = [e for e in event_list if e.get("type") == "plan"]
        assert len(plan_events) >= 1
        assert "steps" in plan_events[0]
        assert plan_events[0].get("n_steps", 0) >= 1
