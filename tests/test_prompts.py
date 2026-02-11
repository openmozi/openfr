"""Tests for prompts and plan parsing."""

import pytest

from openfr.prompts import parse_plan


class TestParsePlan:
    """Tests for parse_plan used in plan-and-execute flow."""

    def test_valid_json_steps(self):
        out = parse_plan('{"steps": [{"goal": "搜索贵州茅台"}, {"goal": "获取行情与财务"}]}')
        assert len(out) == 2
        assert out[0]["goal"] == "搜索贵州茅台"
        assert out[1]["goal"] == "获取行情与财务"

    def test_json_with_markdown_code_block(self):
        raw = '''```json
{"steps": [{"goal": "步骤1"}, {"goal": "步骤2"}]}
```'''
        out = parse_plan(raw)
        assert len(out) == 2
        assert out[0]["goal"] == "步骤1"

    def test_fallback_numbered_lines(self):
        raw = """1. 搜索茅台相关股票
2. 获取实时行情
3. 查看行业板块"""
        out = parse_plan(raw)
        assert len(out) == 3
        assert "搜索" in out[0]["goal"]
        assert "行情" in out[1]["goal"]

    def test_empty_or_whitespace(self):
        assert parse_plan("") == []
        assert parse_plan("   \n  ") == []

    def test_invalid_json_fallback(self):
        raw = "1. 先搜索股票\n2. 再查行情"
        out = parse_plan(raw)
        assert len(out) == 2

    def test_maotai_style_plan(self):
        """典型「贵州茅台适合买入吗」规划输出."""
        raw = '''{"steps": [
  {"goal": "搜索贵州茅台并确认股票代码"},
  {"goal": "获取贵州茅台实时行情和核心财务指标"},
  {"goal": "查看白酒行业板块表现"},
  {"goal": "综合估值与行业情况给出是否适合买入的结论"}
]}'''
        out = parse_plan(raw)
        assert len(out) == 4
        assert "搜索" in out[0]["goal"]
        assert "行情" in out[1]["goal"] or "财务" in out[1]["goal"]
        assert "白酒" in out[2]["goal"] or "行业" in out[2]["goal"]
        assert "结论" in out[3]["goal"] or "买入" in out[3]["goal"]
