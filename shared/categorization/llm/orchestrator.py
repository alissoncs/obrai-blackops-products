from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from shared.categorization.llm.base import (
    LLMProvider,
    ProviderUsage,
    ProviderVote,
    Stage1Candidate,
    Stage2Decision,
)


@dataclass(frozen=True)
class OrchestratorDecision:
    stage1: Stage1Candidate | None
    stage2: Stage2Decision | None
    provider_votes: list[ProviderVote]
    winner_provider: str | None
    agreement_score: float


@dataclass(frozen=True)
class OrchestratorConfig:
    mode: str
    timeout_seconds: int
    max_parallel: int
    primary_provider: str
    ambiguity_threshold: int = 3


def _run_provider(
    provider: LLMProvider,
    *,
    product_context: str,
    top_categories: list[dict[str, str]],
    stage2_candidates_map: dict[str, list[dict[str, str]]],
) -> ProviderVote:
    started = time.perf_counter()
    usage_total = ProviderUsage()
    try:
        stage1, usage1 = provider.classify_stage1(
            product_context=product_context,
            top_categories=top_categories,
        )
        stage2 = None
        usage2 = ProviderUsage()
        if stage1:
            candidates = stage2_candidates_map.get(stage1.category_id, [])
            stage2, usage2 = provider.classify_stage2(
                product_context=product_context,
                parent_category_id=stage1.category_id,
                candidate_level3=candidates,
            )
        elapsed = int((time.perf_counter() - started) * 1000)
        usage_total = ProviderUsage(
            prompt_tokens=(usage1.prompt_tokens or 0) + (usage2.prompt_tokens or 0),
            completion_tokens=(usage1.completion_tokens or 0) + (usage2.completion_tokens or 0),
            total_tokens=(usage1.total_tokens or 0) + (usage2.total_tokens or 0),
            estimated_cost_usd=None,
        )
        return ProviderVote(
            provider=provider.name,
            stage1=stage1,
            stage2=stage2,
            usage=usage_total,
            latency_ms=elapsed,
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        elapsed = int((time.perf_counter() - started) * 1000)
        return ProviderVote(
            provider=provider.name,
            stage1=None,
            stage2=None,
            usage=usage_total,
            latency_ms=elapsed,
            error=str(exc)[:300],
        )


class LLMOrchestrator:
    def __init__(self, providers: list[LLMProvider], config: OrchestratorConfig) -> None:
        self.providers = providers
        self.config = config

    def _choose_votes(self, product_context: str) -> list[LLMProvider]:
        mode = self.config.mode.lower()
        primary = next((p for p in self.providers if p.name == self.config.primary_provider), None)
        primary = primary or (self.providers[0] if self.providers else None)
        if not primary:
            return []
        if mode == "off":
            return [primary]
        if mode == "full":
            return self.providers[: self.config.max_parallel]
        low_context = len(product_context) < 120
        if low_context:
            return [primary]
        return self.providers[: self.config.max_parallel]

    @staticmethod
    def _pick_consensus(votes: list[ProviderVote]) -> tuple[Stage2Decision | None, str | None, float]:
        valid_votes = [v for v in votes if not v.error and v.stage2]
        if not valid_votes:
            return None, None, 0.0
        by_category: dict[str, list[ProviderVote]] = {}
        for vote in valid_votes:
            assert vote.stage2 is not None
            by_category.setdefault(vote.stage2.category_id, []).append(vote)
        winner_category = max(by_category.items(), key=lambda item: len(item[1]))[0]
        winner_votes = by_category[winner_category]
        agreement = len(winner_votes) / len(valid_votes)
        best_vote = max(
            winner_votes,
            key=lambda v: (
                v.stage2.confidence if v.stage2 else 0,
                -(v.latency_ms),
            ),
        )
        winner_provider = best_vote.provider
        return best_vote.stage2, winner_provider, round(agreement, 3)

    def classify(
        self,
        *,
        product_context: str,
        top_categories: list[dict[str, str]],
        stage2_candidates_map: dict[str, list[dict[str, str]]],
    ) -> OrchestratorDecision:
        selected = self._choose_votes(product_context)
        if not selected:
            return OrchestratorDecision(
                stage1=None,
                stage2=None,
                provider_votes=[],
                winner_provider=None,
                agreement_score=0.0,
            )
        votes: list[ProviderVote] = []
        with ThreadPoolExecutor(max_workers=max(1, len(selected))) as executor:
            futures = [
                executor.submit(
                    _run_provider,
                    provider,
                    product_context=product_context,
                    top_categories=top_categories,
                    stage2_candidates_map=stage2_candidates_map,
                )
                for provider in selected
            ]
            for future in as_completed(futures, timeout=self.config.timeout_seconds):
                votes.append(future.result())
        final_stage2, winner_provider, agreement = self._pick_consensus(votes)
        winner_vote = next((v for v in votes if v.provider == winner_provider), None)
        return OrchestratorDecision(
            stage1=winner_vote.stage1 if winner_vote else None,
            stage2=final_stage2,
            provider_votes=votes,
            winner_provider=winner_provider,
            agreement_score=agreement,
        )
