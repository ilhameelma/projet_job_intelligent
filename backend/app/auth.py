#!/usr/bin/env python3
"""
Authentification centralisée
✅ CORRIGÉ : get_dwh_db au lieu de get_user_db
"""

from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_dwh_db
import jwt
import os

SECRET_KEY = os.getenv("JWT_SECRET", "job-intelligent-secret-key")
ALGORITHM  = "HS256"

def get_current_user_from_token(token: str, db: Session):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if user_id is None:
            return None
        result = db.execute(
            text("""
                SELECT id_utilisateur, email, nom, prenom, telephone, role,
                       est_actif, preferences, cv_nom_fichier
                FROM client.utilisateurs
                WHERE id_utilisateur = :id
            """),
            {"id": user_id}
        )
        row = result.fetchone()
        if not row:
            return None
        return {
            "id":             row[0],
            "email":          row[1],
            "nom":            row[2],
            "prenom":         row[3],
            "telephone":      row[4],
            "role":           row[5],
            "est_actif":      row[6],
            "preferences":    row[7] if row[7] else {},
            "cv_nom_fichier": row[8]
        }
    except jwt.PyJWTError:
        return None

async def get_current_user_from_request(
    request: Request,
    db: Session = Depends(get_dwh_db)
):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Non authentifié")
    token = auth_header.replace("Bearer ", "")
    user  = get_current_user_from_token(token, db)
    if not user:
        raise HTTPException(status_code=401, detail="Token invalide")
    if not user.get("est_actif"):
        raise HTTPException(status_code=403, detail="Compte désactivé")
    return user