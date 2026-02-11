"""
OpenFR - Financial Research Agent based on AKShare
"""

import os
import warnings

__version__ = "0.1.0"

# 禁用 tqdm 进度条（AKShare 内部使用）
os.environ["TQDM_DISABLE"] = "1"

# 禁用不必要的警告
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

from openfr.agent import FinancialResearchAgent
from openfr.config import Config

__all__ = ["FinancialResearchAgent", "Config", "__version__"]
