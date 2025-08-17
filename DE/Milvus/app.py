import streamlit as st
import openai
from pymilvus import connections, Collection

# --- CONFIG ---
OPENAI_API_KEY = "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"
ZILLIZ_URI = "https://in03-9582f01f90e5d55.serverless.gcp-us-west1.cloud.zilliz.com"
ZILLIZ_API_KEY = "a49a9b93f296b5a17c29d653b10ba03280801d941c73ffdd21ff5868789fe366ebb9a47367869994ca4739828f30a43e597a8101"
COLLECTION_NAME = "sustainability_report_meli_2024"
EMBEDDING_MODEL = "text-embedding-3-large"
GPT_MODEL = "gpt-4"  # or "gpt-3.5-turbo"

# Initialize OpenAI and Milvus connections once
openai.api_key = OPENAI_API_KEY
connections.connect(
    alias="default",
    uri=ZILLIZ_URI,
    token=ZILLIZ_API_KEY
)
collection = Collection(COLLECTION_NAME)
collection.load()

# --- Helper functions ---
def create_embedding(text):
    response = openai.chat.completions.create(
        model="gpt-4o-mini",  # or your preferred embedding model, e.g. text-embedding-3-large
        messages=[
            {"role": "system", "content": "Create an embedding vector for the text."},
            {"role": "user", "content": text}
        ],
    )
    # For embedding, actually use embeddings API instead (see below)
    # This is placeholder, correct usage is below
    pass

def create_embedding(text):
    # Correct embeddings API call (new openai python client)
    response = openai.embeddings.create(
        model=EMBEDDING_MODEL,
        input=text
    )
    return response.data[0].embedding

def semantic_search(query, top_k=5):
    query_emb = create_embedding(query)
    results = collection.search(
        data=[query_emb],
        anns_field="embedding",
        param={"metric_type": "COSINE", "params": {}},
        limit=top_k,
        output_fields=["text", "page", "source"]
    )
    return results[0]

def generate_answer(query, contexts):
    system_prompt = (
        "You are an expert assistant answering questions based only on the provided context.\n"
        "If the answer is not contained in the context, say 'I don't know.'\n"
        "Be concise and clear."
    )

    context_text = "\n---\n".join([f"Page {c.entity.get('page')}: {c.entity.get('text')}" for c in contexts])

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
st.title("📚 MELI 2024: Sustainability report - Semantic Search with GPT Answers")

query = st.text_input("Qué te gustaría saber del Sustainability report?")

if st.button("Search"):
    if not query.strip():
        st.warning("Please enter a question.")
    else:
        with st.spinner("Searching and generating answer..."):
            try:
                contexts = semantic_search(query)
                answer = generate_answer(query, contexts)
                st.markdown("### 💡 GPT Answer:")
                st.write(answer)

                st.markdown("---")
                st.markdown("### 🔍 Top relevant chunks:")
                for i, ctx in enumerate(contexts):
                    st.markdown(f"**Result {i+1} (Page {ctx.entity.get('page')}):**")
                    st.write(ctx.entity.get('text'))
                    st.write(f"_Source: {ctx.entity.get('source')}_")
                    st.markdown("---")

            except Exception as e:
                st.error(f"Error: {e}")
