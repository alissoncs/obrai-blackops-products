from __future__ import annotations

from dataclasses import dataclass

from shared.categorization.llm.providers.openai_compatible import OpenAICompatibleProvider


@dataclass
class GeminiProvider(OpenAICompatibleProvider):
    name: str = "gemini"
