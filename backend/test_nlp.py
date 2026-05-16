from sentence_transformers import SentenceTransformer
model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
sentences = ["Data Engineer avec Python", "Ingénieur données Python"]
embeddings = model.encode(sentences)
print("Modèle chargé, shape des embeddings:", embeddings.shape)