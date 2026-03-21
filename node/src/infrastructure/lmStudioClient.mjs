/**
 * Infrastructure: HTTP client for LM Studio OpenAI-compatible API.
 * POST {baseUrl}/chat/completions
 */

export async function chatCompletion({
  baseUrl,
  apiKey,
  model,
  messages,
  temperature = 0.1,
  maxTokens = 8192,
  signal,
}) {
  const url = `${baseUrl.replace(/\/$/, "")}/chat/completions`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model,
      messages,
      temperature,
      max_tokens: maxTokens,
      stream: false,
    }),
    signal,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`LM Studio HTTP ${res.status}: ${text.slice(0, 500)}`);
  }

  const data = await res.json();
  const content = data?.choices?.[0]?.message?.content;
  if (typeof content !== "string") {
    throw new Error("Missing choices[0].message.content in API response");
  }
  return content.trim();
}
