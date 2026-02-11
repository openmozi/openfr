"""
Tests for financial data tools.
"""

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

from openfr.tools import get_all_tools, get_tool_descriptions
from openfr.tools.base import format_dataframe, validate_stock_code, validate_date
from openfr.tools.stock import (
    get_stock_realtime,
    get_stock_history,
    get_stock_info,
    search_stock,
    get_hot_stocks,
    get_industry_boards,
)
from openfr.tools.fund import get_fund_list, get_etf_realtime, get_fund_rank
from openfr.tools.futures import get_futures_realtime, get_futures_history
from openfr.tools.index import get_index_realtime, get_index_history
from openfr.tools.macro import get_macro_cpi, get_macro_ppi, get_macro_pmi, get_macro_gdp


class TestToolRegistry:
    """Tests for tool registry."""

    def test_get_all_tools(self):
        """Test getting all tools."""
        tools = get_all_tools()
        # A股18 (含扩展：五档/资金流/龙虎榜/业绩预告与快报/盈利预测等) + 港股3 + 基金4 + 期货3 + 指数2 + 宏观5 = 35
        assert len(tools) == 35

    def test_get_tools_with_filters(self):
        """Test getting tools with category filters."""
        # 仅股票（A股+港股）
        tools = get_all_tools(
            include_stock=True,
            include_stock_hk=True,
            include_fund=False,
            include_futures=False,
            include_index=False,
            include_macro=False,
        )
        assert len(tools) == 21  # 18 A股 + 3 港股

        # 仅基金
        tools = get_all_tools(
            include_stock=False,
            include_stock_hk=False,
            include_fund=True,
            include_futures=False,
            include_index=False,
            include_macro=False,
        )
        assert len(tools) == 4  # 4 fund tools

    def test_get_tool_descriptions(self):
        """Test getting tool descriptions."""
        descriptions = get_tool_descriptions()
        assert "股票数据" in descriptions
        assert "基金数据" in descriptions
        assert "期货数据" in descriptions
        assert "指数数据" in descriptions
        assert "宏观数据" in descriptions

    def test_all_tools_have_names(self):
        """Test that all tools have names."""
        tools = get_all_tools()
        for tool in tools:
            assert hasattr(tool, "name")
            assert tool.name is not None

    def test_all_tools_have_descriptions(self):
        """Test that all tools have descriptions."""
        tools = get_all_tools()
        for tool in tools:
            assert hasattr(tool, "description")
            assert tool.description is not None
            assert len(tool.description) > 0


class TestBaseUtils:
    """Tests for base utility functions."""

    def test_format_dataframe(self):
        """Test DataFrame formatting."""
        df = pd.DataFrame({
            "代码": ["000001", "000002"],
            "名称": ["平安银行", "万科A"],
            "价格": [10.5, 15.2],
        })
        result = format_dataframe(df)
        assert "000001" in result
        assert "平安银行" in result

    def test_format_empty_dataframe(self):
        """Test formatting empty DataFrame."""
        df = pd.DataFrame()
        result = format_dataframe(df)
        assert result == "(无数据)"

    def test_format_dataframe_truncation(self):
        """Test DataFrame truncation for large data."""
        df = pd.DataFrame({"col": range(100)})
        result = format_dataframe(df, max_rows=10)
        assert "显示前 10 条" in result

    def test_validate_stock_code(self):
        """Test stock code validation."""
        assert validate_stock_code("000001") == "000001"
        assert validate_stock_code("SH600519") == "600519"
        assert validate_stock_code("600519.SH") == "600519"
        assert validate_stock_code("sz000001") == "000001"
        assert validate_stock_code("1") == "000001"  # Pad with zeros

    def test_validate_date(self):
        """Test date validation."""
        assert validate_date("20231015") == "20231015"
        assert validate_date("2023-10-15") == "20231015"
        assert validate_date("2023/10/15") == "20231015"

    def test_validate_date_invalid(self):
        """Test invalid date raises error."""
        with pytest.raises(ValueError):
            validate_date("invalid")
        with pytest.raises(ValueError):
            validate_date("2023")


class TestStockTools:
    """Tests for stock data tools."""

    @patch("openfr.tools.stock.ak")
    def test_get_stock_realtime(self, mock_ak):
        """Test getting stock realtime data."""
        mock_df = pd.DataFrame({
            "代码": ["000001"],
            "名称": ["平安银行"],
            "最新价": [10.5],
            "涨跌幅": [1.23],
        })
        mock_ak.stock_zh_a_spot_em.return_value = mock_df

        result = get_stock_realtime.invoke({"symbol": "000001"})
        assert "000001" in result
        assert "平安银行" in result or "实时行情" in result

    @patch("openfr.tools.stock.ak")
    def test_get_stock_realtime_not_found(self, mock_ak):
        """Test stock not found."""
        mock_ak.stock_zh_a_spot_em.return_value = pd.DataFrame({
            "代码": ["000002"],
            "名称": ["万科A"],
        })

        result = get_stock_realtime.invoke({"symbol": "000001"})
        assert "未找到" in result

    @patch("openfr.tools.stock.ak")
    def test_get_hot_stocks(self, mock_ak):
        """Test getting hot stocks."""
        mock_df = pd.DataFrame({
            "代码": ["000001", "000002"],
            "股票名称": ["平安银行", "万科A"],
            "最新价": [10.5, 15.2],
            "涨跌幅": [1.23, -0.5],
        })
        mock_ak.stock_hot_rank_em.return_value = mock_df

        result = get_hot_stocks.invoke({})
        assert "热门股票" in result

    @patch("openfr.tools.stock.ak")
    def test_search_stock(self, mock_ak):
        """Test stock search."""
        mock_df = pd.DataFrame({
            "代码": ["000001", "600000"],
            "名称": ["平安银行", "浦发银行"],
            "最新价": [10.5, 8.2],
            "涨跌幅": [1.23, 0.5],
        })
        mock_ak.stock_zh_a_spot_em.return_value = mock_df

        result = search_stock.invoke({"keyword": "银行"})
        assert "银行" in result or "搜索" in result


class TestIndexTools:
    """Tests for index data tools."""

    @patch("openfr.tools.index.ak")
    def test_get_index_realtime(self, mock_ak):
        """Test getting index realtime data."""
        mock_df = pd.DataFrame({
            "代码": ["000001"],
            "名称": ["上证指数"],
            "最新价": [3200.0],
            "涨跌幅": [0.5],
        })
        mock_ak.stock_zh_index_spot_em.return_value = mock_df

        result = get_index_realtime.invoke({})
        assert "指数" in result or "上证" in result


class TestMacroTools:
    """Tests for macro data tools."""

    @patch("openfr.tools.macro.ak")
    def test_get_macro_cpi(self, mock_ak):
        """Test getting CPI data."""
        mock_df = pd.DataFrame({
            "月份": ["2023-10"],
            "CPI同比": [2.1],
        })
        mock_ak.macro_china_cpi.return_value = mock_df

        result = get_macro_cpi.invoke({})
        assert "CPI" in result

    @patch("openfr.tools.macro.ak")
    def test_get_macro_cpi_error(self, mock_ak):
        """Test CPI data fetch error."""
        mock_ak.macro_china_cpi.side_effect = Exception("Network error")

        result = get_macro_cpi.invoke({})
        assert "失败" in result
