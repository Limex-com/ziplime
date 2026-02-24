"""
OpenRouter LLM client and conversation management.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from openai import AsyncOpenAI

from .prompts import SYSTEM_PROMPT


@dataclass
class BacktestConfig:
    """Parsed backtest configuration from LLM response."""
    symbols: list[str]
    start_date: str        # YYYY-MM-DD
    end_date: str          # YYYY-MM-DD
    capital: float = 100_000.0
    benchmark: Optional[str] = "SPY"


@dataclass
class AgentResponse:
    """Structured response from the LLM."""
    text: str                             # Full text response
    backtest_config: Optional[BacktestConfig] = None
    algorithm_code: Optional[str] = None  # Python algorithm code (functions only)
    has_backtest: bool = False


class ZiplimeAgent:
    """
    Manages conversation with an LLM via OpenRouter API.
    Parses structured backtest requests from LLM responses.
    """

    def __init__(self, api_key: str, model: str = "x-ai/grok-4.1-fast"):
        self.model = model
        self.conversation_history: list[dict] = []
        self._client = AsyncOpenAI(
            api_key=api_key,
            base_url="https://openrouter.ai/api/v1",
            default_headers={
                "HTTP-Referer": "https://github.com/Limex-com/ziplime",
                "X-Title": "Ziplime AI Backtesting Assistant",
            },
        )

    async def chat(self, user_message: str) -> AgentResponse:
        """Send a message and get a parsed response."""
        self.conversation_history.append({
            "role": "user",
            "content": user_message,
        })

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            *self.conversation_history,
        ]

        completion = await self._client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0.2,
            max_tokens=4096,
        )

        response_text = completion.choices[0].message.content or ""
        self.conversation_history.append({
            "role": "assistant",
            "content": response_text,
        })

        return self._parse_response(response_text)

    def add_result_context(self, results_summary: str) -> None:
        """Add backtest results to conversation history for follow-up questions."""
        self.conversation_history.append({
            "role": "user",
            "content": f"[BACKTEST COMPLETED]\n{results_summary}\n\nPlease interpret these results for me.",
        })

    def clear_history(self) -> None:
        """Clear conversation history (start a new session)."""
        self.conversation_history.clear()

    # ------------------------------------------------------------------ #
    # Private helpers                                                      #
    # ------------------------------------------------------------------ #

    def _parse_response(self, text: str) -> AgentResponse:
        """
        Extract backtest config and algorithm code from LLM response.

        Expected format in the response:
            <BACKTEST>
            symbols: AAPL, MSFT
            start_date: 2024-01-03
            end_date: 2025-01-01
            capital: 100000
            benchmark: SPY
            </BACKTEST>

            ```python
            async def initialize(context):
                ...
            async def handle_data(context, data):
                ...
            ```
        """
        config = self._extract_backtest_config(text)
        code = self._extract_algorithm_code(text)

        has_backtest = config is not None and code is not None

        return AgentResponse(
            text=self._clean_display_text(text),
            backtest_config=config,
            algorithm_code=code,
            has_backtest=has_backtest,
        )

    def _extract_backtest_config(self, text: str) -> Optional[BacktestConfig]:
        """Parse <BACKTEST>...</BACKTEST> block."""
        match = re.search(r"<BACKTEST>(.*?)</BACKTEST>", text, re.DOTALL | re.IGNORECASE)
        if not match:
            return None

        block = match.group(1)
        data: dict = {}

        for line in block.strip().splitlines():
            line = line.strip()
            if ":" in line:
                key, _, value = line.partition(":")
                data[key.strip().lower()] = value.strip()

        raw_symbols = data.get("symbols", "")
        symbols = [s.strip().upper() for s in raw_symbols.split(",") if s.strip()]
        if not symbols:
            return None

        return BacktestConfig(
            symbols=symbols,
            start_date=data.get("start_date", "2024-01-03"),
            end_date=data.get("end_date", "2025-01-01"),
            capital=float(data.get("capital", 100_000)),
            benchmark=data.get("benchmark", "SPY") or None,
        )

    def _extract_algorithm_code(self, text: str) -> Optional[str]:
        """Extract the first Python code block that contains async def functions."""
        # Match fenced code blocks (```python ... ```)
        code_blocks = re.findall(r"```python\s*(.*?)```", text, re.DOTALL)

        for block in code_blocks:
            # Accept blocks that define at least handle_data or initialize
            if "async def handle_data" in block or "async def initialize" in block:
                return block.strip()

        return None

    def _clean_display_text(self, text: str) -> str:
        """Remove technical markers from text before displaying to user."""
        # Remove the <BACKTEST>...</BACKTEST> block from displayed text
        text = re.sub(r"<BACKTEST>.*?</BACKTEST>", "", text, flags=re.DOTALL | re.IGNORECASE)
        # Remove ```python...``` code blocks (shown separately)
        text = re.sub(r"```python\s*.*?```", "", text, flags=re.DOTALL)
        return text.strip()
