#!/usr/bin/env bash
# 运行 OpenFR 集成测试（含 .env 配置的 TestEnvIntegration）
# 使用前请：pip install -e .  并可选配置 .env

set -e
cd "$(dirname "$0")/.."
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"
python3 -m pytest tests/test_integration.py -v -m integration --tb=short "$@"
