import streamlit as st
import openai
from pymilvus import connections, Collection
from datetime import datetime

# --- CONFIG ---
OPENAI_API_KEY = "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"
ZILLIZ_URI = "https://in03-9582f01f90e5d55.serverless.gcp-us-west1.cloud.zilliz.com"
ZILLIZ_API_KEY = "a49a9b93f296b5a17c29d653b10ba03280801d941c73ffdd21ff5868789fe366ebb9a47367869994ca4739828f30a43e597a8101"
COLLECTION_NAME = "metrics_vectors_dual_dates"
EMBEDDING_MODEL = "text-embedding-3-large"
GPT_MODEL = "gpt-4"

# --- Init ---
openai.api_key = OPENAI_API_KEY
connections.connect(alias="default", uri=ZILLIZ_URI, token=ZILLIZ_API_KEY)
collection = Collection(COLLECTION_NAME)
collection.load()

# --- Helper: Create embedding ---
def create_embedding(text):
    response = openai.embeddings.create(
        model=EMBEDDING_MODEL,
        input=text
    )
    return response.data[0].embedding

# --- Semantic Search ---
def semantic_search(query, top_k=5):
    query_emb = create_embedding(query)
    search_args = {
        "data": [query_emb],
        "anns_field": "embedding",
        "param": {"metric_type": "COSINE", "params": {}},
        "limit": top_k,
        "output_fields": [
            "text", "Month_start", "SHP_SITE_ID", "DISTANCIA_CORREGIDA"
        ]
    }
    results = collection.search(**search_args)
    return results[0]

# --- Generate GPT answer ---
def generate_answer(query, contexts):
    system_prompt = (
        "You are an expert assistant answering questions based only on the provided context.\n"
        "If the answer is not contained in the context, say 'I don't know.'\n"
        "Be concise and clear."
    )

    context_text = "\n---\n".join([f"{c.entity.get('text')}" for c in contexts])
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Based on the following context:\n{context_text}\n\nAnswer this question:\n{query}"}
    ]

    response = openai.chat.completions.create(
        model=GPT_MODEL,
        messages=messages,
        temperature=0
    )
    return response.choices[0].message.content.strip()

# --- Streamlit UI ---
st.title("📚 MELI 2024: Sustainability Report - Semantic Search")

# --- Query input ---
query = st.text_input("¿Qué te gustaría saber del Sustainability report?")

# --- Search & Display ---
if st.button("🔎 Buscar"):
    if not query.strip():
        st.warning("Por favor, ingresa una pregunta.")
    else:
        with st.spinner("Buscando y generando respuesta..."):
            try:
                contexts = semantic_search(query, top_k=5)
                answer = generate_answer(query, contexts)

                st.markdown("### 💡 Respuesta de GPT:")
                st.write(answer)

                st.markdown("---")
                st.markdown("### 🔍 Fragmentos más relevantes:")
                for i, ctx in enumerate(contexts):
                    st.markdown(f"**Resultado {i+1}:**")
                    st.write(ctx.entity.get('text'))
                    st.write(
                        f"_Mes: {ctx.entity.get('Month_start')}, "
                        f"Sitio: {ctx.entity.get('SHP_SITE_ID')}, "
                        f"Distancia: {ctx.entity.get('DISTANCIA_CORREGIDA')}_"
                    )
                    st.markdown("---")

            except Exception as e:
                st.error(f"Error: {e}")
