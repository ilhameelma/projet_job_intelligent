from fastapi import FastAPI
import redis, psycopg2, os

app = FastAPI()
r = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: str):
    # 1. Chercher dans le cache Redis
    cached = r.get(f"reco:{user_id}")
    if cached:
        return {"source": "cache", "data": cached}
    # 2. Sinon requêter le DWH
    return {"source": "dwh", "data": []}