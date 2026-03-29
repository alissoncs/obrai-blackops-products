import json
import os
import sys
import urllib.request
import urllib.error


def main() -> int:
    # Espera uma API OpenAI-compatible exposta pelo LLM Studio.
    # Ex.: http://localhost:1234/v1
    base_url = os.getenv("LLM_STUDIO_BASE_URL", "http://localhost:1234/v1").rstrip("/")
    api_key = os.getenv("LLM_STUDIO_API_KEY", "").strip()
    model = os.getenv("LLM_STUDIO_MODEL", "qwen35").strip()

    prompt = "Pergunta simples: Qual é a capital do Brasil?"

    url = f"{base_url}/chat/completions"

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
    }

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        # HTTPError já vem com status != 2xx.
        err_body = ""
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        print(f"Erro HTTP {e.code} ao chamar {url}", file=sys.stderr)
        if err_body:
            print(err_body, file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Erro ao chamar {url}: {e}", file=sys.stderr)
        return 1

    try:
        content = body["choices"][0]["message"]["content"]
    except Exception:
        print("Não consegui extrair o texto da resposta. Resposta bruta:", file=sys.stderr)
        print(json.dumps(body, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1

    print(content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

