# Silmarillion Q&A (Milvus/Zilliz + OpenAI) — Hugging Face Space

This Space exposes a simple Gradio app that retrieves the most relevant passages from your Milvus/Zilliz collection and asks an OpenAI chat model to answer using only those passages.

## 1) Secrets & Variables (Space Settings → Secrets)
Add these keys in your Space settings so the app can access them via environment variables:

- `OPENAI_API_KEY`
- `ZILLIZ_URI` (e.g. `https://in03-xxxx.serverless.gcp-us-west1.cloud.zilliz.com`)
- `ZILLIZ_API_KEY`

Optional variables (also in Settings → Secrets):
- `COLLECTION_NAME` (default: `silmarillion`)
- `EMBEDDING_MODEL` (default: `text-embedding-3-large`)
- `CHAT_MODEL` (default: `gpt-5`)

> **Important:** Your Milvus collection's vector **dimension must match** the chosen embedding model (e.g. `text-embedding-3-large` produces 3072-d vectors). If your collection was created with a different model (e.g., 1536-d `text-embedding-3-small`), update `EMBEDDING_MODEL` or rebuild the collection.

## 2) Run locally (optional)
```bash
pip install -r requirements.txt
export OPENAI_API_KEY=...  # or set in your shell profile
export ZILLIZ_URI=...
export ZILLIZ_API_KEY=...
python app.py