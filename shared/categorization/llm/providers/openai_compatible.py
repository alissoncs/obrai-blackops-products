from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

import requests

from shared.categorization.llm.base import ProviderUsage, Stage1Candidate, Stage2Decision


def _extract_json(content: str) -> dict[str, Any] | None:
    text = content.strip()
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if fenced:
        text = fenced.group(1).strip()
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                data = json.loads(text[start : end + 1])
                return data if isinstance(data, dict) else None
            except json.JSONDecodeError:
                return None
    return None


def _normalize_confidence(raw: Any) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 3
    return max(1, min(5, value))


@dataclass
class OpenAICompatibleProvider:
    name: str
    base_url: str
    model: str
    api_key: str
    timeout_seconds: int = 60

    def _chat(self, system_prompt: str, user_prompt: str) -> tuple[dict[str, Any] | None, ProviderUsage]:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"}
        response = requests.post(
            f"{self.base_url.rstrip('/')}/chat/completions",
            json=payload,
            headers=headers,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
        usage_raw = data.get("usage") or {}
        usage = ProviderUsage(
            prompt_tokens=usage_raw.get("prompt_tokens"),
            completion_tokens=usage_raw.get("completion_tokens"),
            total_tokens=usage_raw.get("total_tokens"),
        )
        choices = data.get("choices") or []
        if not choices:
            return None, usage
        message = (choices[0].get("message") or {}).get("content") or ""
        return _extract_json(message), usage

    def classify_stage1(
        self,
        *,
        product_context: str,
        top_categories: list[dict[str, str]],
    ) -> tuple[Stage1Candidate | None, ProviderUsage]:
        taxonomy = "\n".join(f"{node['id']}|{node['name']}" for node in top_categories)
        system_prompt = (
            "Classifique o produto em uma categoria pai válida. "
            "Responda JSON: {category_id, category_name, confidence, reason}. "
            "Escolha somente category_id listado."
        )
        user_prompt = f"Produto:\n{product_context}\n\nCategorias válidas:\n{taxonomy}"
        parsed, usage = self._chat(system_prompt, user_prompt)
        if not parsed:
            return None, usage
        cid = str(parsed.get("category_id", "")).strip()
        if not cid:
            return None, usage
        return (
            Stage1Candidate(
                category_id=cid,
                category_name=str(parsed.get("category_name", "")).strip(),
                confidence=_normalize_confidence(parsed.get("confidence")),
                reason=str(parsed.get("reason", "")).strip(),
            ),
            usage,
        )

    def classify_stage2(
        self,
        *,
        product_context: str,
        parent_category_id: str,
        candidate_level3: list[dict[str, str]],
    ) -> tuple[Stage2Decision | None, ProviderUsage]:
        taxonomy = "\n".join(
            f"{node['id']}|{node['name']}|{node['path']}" for node in candidate_level3
        )
        system_prompt = (
            "Escolha UMA categoria final nível 3 válida. "
            "Responda JSON: {category_id, category_name, category_path, confidence, reason}. "
            "Escolha somente category_id listado."
        )
        user_prompt = (
            f"Produto:\n{product_context}\n\nCategoria pai sugerida: {parent_category_id}\n\n"
            f"Categorias nível 3 válidas:\n{taxonomy}"
        )
        parsed, usage = self._chat(system_prompt, user_prompt)
        if not parsed:
            return None, usage
        cid = str(parsed.get("category_id", "")).strip()
        if not cid:
            return None, usage
        return (
            Stage2Decision(
                category_id=cid,
                category_name=str(parsed.get("category_name", "")).strip(),
                category_path=str(parsed.get("category_path", "")).strip(),
                confidence=_normalize_confidence(parsed.get("confidence")),
                reason=str(parsed.get("reason", "")).strip(),
            ),
            usage,
        )
