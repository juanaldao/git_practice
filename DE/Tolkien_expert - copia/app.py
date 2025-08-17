import os
import gradio as gr
from openai import OpenAI
from pymilvus import connections, Collection

# --- Config via environment variables (set these in your Space settings) ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ZILLIZ_URI = os.getenv("ZILLIZ_URI")
ZILLIZ_API_KEY = os.getenv("ZILLIZ_API_KEY")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "silmarillion")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-large")
CHAT_MODEL = os.getenv("CHAT_MODEL", "gpt-5")  # change if you prefer another chat model

missing = [k for k, v in {
    "OPENAI_API_KEY": OPENAI_API_KEY,
    "ZILLIZ_URI": ZILLIZ_URI,
    "ZILLIZ_API_KEY": ZILLIZ_API_KEY,
}.items() if not v]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

# --- OpenAI client ---
client = OpenAI(api_key=OPENAI_API_KEY)

# --- Milvus/Zilliz connection + collection ---
connections.connect(alias="default", uri=ZILLIZ_URI, token=ZILLIZ_API_KEY)
collection = Collection(COLLECTION_NAME)
try:
    collection.load()
except Exception:
    # In Zilliz serverless, this may be managed automatically; ignore if not needed.
    pass


def create_embedding(text: str):
    resp = client.embeddings.create(model=EMBEDDING_MODEL, input=text)
    return resp.data[0].embedding


def answer_query(query: str, top_k: int = 5):
    query = (query or "").strip()
    if not query:
        return "Please enter a question.", ""

    # 1) Embed the query
    query_emb = create_embedding(query)

    # 2) Vector search in Milvus/Zilliz
    results = collection.search(
        data=[query_emb],
        anns_field="embedding",  # must match your vector field name
        param={"metric_type": "COSINE", "params": {}},
        limit=int(top_k),
        output_fields=["text", "page", "source"],
    )

    # 3) Build context and tidy retrieved passages
    passages = []
    for hit in results[0]:
        passages.append({
            "score": round(float(hit.score), 4),
            "page": hit.entity.get("page"),
            "source": hit.entity.get("source"),
            "text": hit.entity.get("text"),
        })

    context = "\n\n".join([p["text"] for p in passages if p.get("text")])

    # 4) Ask the chat model to answer using only the provided context
    messages = [
        {
            "role": "system",
            "content": (
                "You are an expert assistant answering questions based only on the provided context. "
                "If the answer is not contained in the context, say 'I don't know.' Be concise and clear."
            ),
        },
        {
            "role": "user",
            "content": f"Question: {query}\n\nContext:\n{context}\n\nAnswer in natural language:",
        },
    ]

    completion = client.chat.completions.create(
        model=CHAT_MODEL,
        temperature=0.2,
        messages=messages,
    )

    answer = completion.choices[0].message.content

    # 5) Present answer + sources
    source_lines = [
        f"(Score {p['score']}) Page {p['page']} • {p['source']}" for p in passages
    ]
    sources_text = "\n".join(source_lines)

    return answer, sources_text


# --- Gradio UI ---
with gr.Blocks(theme=gr.themes.Soft()) as demo:
    gr.Markdown("# Silmarillion Q&A (Milvus + OpenAI)")
    gr.Markdown(
        "Ask a question about your indexed Silmarillion PDF. This demo uses Milvus/Zilliz for vector search and an OpenAI chat model for generation.")

    with gr.Row():
        query = gr.Textbox(label="Your question", placeholder="e.g., Tell me about Thangorodrim")
        k = gr.Slider(1, 10, value=5, step=1, label="Top K (retrieved passages)")

    answer = gr.Markdown(label="Answer")
    sources = gr.Textbox(label="Top Retrieved Passages", lines=8)

    ask = gr.Button("Ask")
    ask.click(fn=answer_query, inputs=[query, k], outputs=[answer, sources])

    gr.Examples([
        "Who is Morgoth?",
        "Tell me about Thangorodrim",
        "What are the Silmarils?",
    ], inputs=query)

if __name__ == "__main__":
    demo.launch()