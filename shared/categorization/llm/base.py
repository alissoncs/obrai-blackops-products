from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Stage1Candidate:
    category_id: str
    category_name: str
    confidence: int
    reason: str


@dataclass(frozen=True)
class Stage2Decision:
    category_id: str
    category_name: str
    category_path: str
    confidence: int
    reason: str


@dataclass(frozen=True)
class ProviderUsage:
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    estimated_cost_usd: float | None = None


@dataclass(frozen=True)
class ProviderVote:
    provider: str
    stage1: Stage1Candidate | None
    stage2: Stage2Decision | None
    usage: ProviderUsage
    latency_ms: int
    error: str | None = None


class LLMProvider(Protocol):
    name: str

    def classify_stage1(
        self,
        *,
        product_context: str,
        top_categories: list[dict[str, str]],
    ) -> tuple[Stage1Candidate | None, ProviderUsage]:
        ...

    def classify_stage2(
        self,
        *,
        product_context: str,
        parent_category_id: str,
        candidate_level3: list[dict[str, str]],
    ) -> tuple[Stage2Decision | None, ProviderUsage]:
        ...
