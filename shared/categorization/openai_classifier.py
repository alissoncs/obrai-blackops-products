from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen

from .categories_api import CategoryLevel3

DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
DEFAULT_MODEL = "gpt-4o-mini"


@dataclass(frozen=True)
class CategorySelection:
    level1_id: str
    level1_name: str
    level3_id: str
    level3_name: str
    confidence: int
    reason: str


class OpenAiClassifier:
    """Two-stage classification via OpenAI Chat Completions (JSON mode)."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str = DEFAULT_MODEL,
        base_url: str = DEFAULT_OPENAI_BASE_URL,
        timeout_s: float = 45.0,
    ) -> None:
        self.api_key = api_key.strip()
        self.model = model.strip() or DEFAULT_MODEL
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY não definido")

    def classify_product(
        self,
        *,
        product: dict[str, Any],
        level3_categories: list[CategoryLevel3],
    ) -> CategorySelection:
        l1_options = self._level1_options(level3_categories)
        stage1 = self._pick_level1(product=product, level1_options=l1_options)
        level3_options = [c for c in level3_categories if c.level1_id == stage1["level1Id"]]
        if not level3_options:
            raise ValueError("Nenhuma subcategoria disponível para level1 selecionado")
        stage2 = self._pick_level3(product=product, level3_options=level3_options)
        confidence = int(stage2.get("confidence", 3))
        confidence = max(1, min(5, confidence))
        return CategorySelection(
            level1_id=stage1["level1Id"],
            level1_name=stage1["level1Name"],
            level3_id=stage2["level3Id"],
            level3_name=stage2["level3Name"],
            confidence=confidence,
            reason=str(stage2.get("reason") or "").strip()[:280],
        )

    def _level1_options(self, level3_categories: list[CategoryLevel3]) -> list[dict[str, str]]:
        dedup: dict[str, dict[str, str]] = {}
        for c in level3_categories:
            if c.level1_id not in dedup:
                dedup[c.level1_id] = {"level1Id": c.level1_id, "level1Name": c.level1_name}
        return list(dedup.values())

    def _product_context(self, product: dict[str, Any]) -> str:
        name = str(product.get("name") or "").strip()
        brand = str(product.get("brandName") or "").strip()
        sku = str(product.get("sku") or "").strip()
        description = str(product.get("description") or "").replace("\n", " ").strip()
        description = description[:400]
        return (
            f"name={name}\n"
            f"brand={brand}\n"
            f"sku={sku}\n"
            f"description={description}"
        )

    def _pick_level1(
        self,
        *,
        product: dict[str, Any],
        level1_options: list[dict[str, str]],
    ) -> dict[str, str]:
        options_text = "\n".join(
            f"- level1Id={o['level1Id']} | level1Name={o['level1Name']}" for o in level1_options
        )
        prompt = (
            "Selecione a melhor categoria de NIVEL 1 para este produto.\n"
            "Responda SOMENTE JSON válido com: level1Id, level1Name.\n"
            "level1Id deve ser exatamente um item da lista.\n\n"
            f"Produto:\n{self._product_context(product)}\n\n"
            f"Opções nivel 1:\n{options_text}\n"
        )
        data = self._generate_json(prompt)
        level1_id = str(data.get("level1Id") or "").strip()
        match = next((o for o in level1_options if o["level1Id"] == level1_id), None)
        if not match:
            raise ValueError("OpenAI retornou level1Id inválido")
        return match

    def _pick_level3(
        self,
        *,
        product: dict[str, Any],
        level3_options: list[CategoryLevel3],
    ) -> dict[str, Any]:
        options_text = "\n".join(
            f"- level3Id={c.id} | level3Name={c.name} | path={c.path}" for c in level3_options
        )
        prompt = (
            "Selecione a melhor categoria FINAL (nivel 3) para este produto.\n"
            "Responda SOMENTE JSON válido com: level3Id, level3Name, confidence, reason.\n"
            "confidence é inteiro de 1 a 5.\n"
            "level3Id deve ser exatamente um item da lista.\n\n"
            f"Produto:\n{self._product_context(product)}\n\n"
            f"Opções nivel 3:\n{options_text}\n"
        )
        data = self._generate_json(prompt)
        level3_id = str(data.get("level3Id") or "").strip()
        match = next((c for c in level3_options if c.id == level3_id), None)
        if not match:
            raise ValueError("OpenAI retornou level3Id inválido")
        return {
            "level3Id": match.id,
            "level3Name": match.name,
            "confidence": data.get("confidence", 3),
            "reason": data.get("reason", ""),
        }

    def _generate_json(self, user_content: str) -> dict[str, Any]:
        url = f"{self.base_url}/chat/completions"
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "user", "content": user_content}],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }
        req = Request(
            url,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            data=json.dumps(payload).encode("utf-8"),
        )
        with urlopen(req, timeout=self.timeout_s) as resp:
            status = getattr(resp, "status", 200)
            if status < 200 or status >= 300:
                body = resp.read().decode("utf-8", errors="replace")
                raise ValueError(f"Falha na chamada OpenAI: HTTP {status} {body[:500]}")
            data = json.loads(resp.read().decode("utf-8"))
        content = self._extract_message_content(data)
        parsed = self._extract_json(content)
        if not isinstance(parsed, dict):
            raise ValueError("OpenAI retornou JSON inválido")
        return parsed

    @staticmethod
    def _extract_message_content(data: dict[str, Any]) -> str:
        choices = data.get("choices") or []
        if not choices:
            raise ValueError("Resposta OpenAI sem choices")
        message = (choices[0].get("message") or {})
        content = message.get("content")
        if isinstance(content, str) and content.strip():
            return content.strip()
        raise ValueError("Resposta OpenAI sem conteúdo de mensagem")

    @staticmethod
    def _extract_json(text: str) -> dict[str, Any] | None:
        cleaned = text.strip()
        match = re.search(r"```(?:json)?\s*([\s\S]*?)```", cleaned)
        if match:
            cleaned = match.group(1).strip()
        try:
            parsed = json.loads(cleaned)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            pass
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start >= 0 and end > start:
            try:
                parsed = json.loads(cleaned[start : end + 1])
                return parsed if isinstance(parsed, dict) else None
            except json.JSONDecodeError:
                return None
        return None
