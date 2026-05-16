#!/usr/bin/env python3
import httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from database import get_dwh_db
from auth import get_current_user_from_request
import os
import logging

router = APIRouter(prefix="/api/assistant", tags=["Assistant"])
logger = logging.getLogger(__name__)

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"

async def query_groq(prompt: str) -> str:
    if not GROQ_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            payload = {
                "model": GROQ_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.7,
                "max_tokens": 800
            }
            headers = {
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            }
            response = await client.post(GROQ_URL, json=payload, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                return data["choices"][0]["message"]["content"]
            else:
                logger.error(f"Erreur Groq {response.status_code}")
                return None
    except Exception as e:
        logger.error(f"Exception Groq: {e}")
        return None

@router.post("/chat")
async def chat(
    request: Request, 
    dwh_db: Session = Depends(get_dwh_db),
    user = Depends(get_current_user_from_request)
):
    try:
        body = await request.json()
        user_message = body.get("message", "")
        
        if not user_message:
            return JSONResponse({"success": False, "error": "Message vide"}, status_code=400)
        
        prompt = f"""Tu es un assistant IA utile, amical et polyvalent. Tu réponds en français de manière claire, précise et bienveillante.

Question : {user_message}

Réponse :"""

        response = await query_groq(prompt)
        
        if response:
            return {"success": True, "response": response}
        else:
            return {
                "success": True, 
                "response": "🤖 Je suis votre assistant IA. L'API est temporairement indisponible, veuillez réessayer dans quelques instants."
            }
    
    except Exception as e:
        logger.error(f"Erreur dans le chat: {e}")
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)