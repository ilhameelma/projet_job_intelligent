#!/usr/bin/env python3
"""
Routes d'authentification
✅ CORRIGÉ :
- get_dwh_db au lieu de get_user_db
- import relatif pour auth et database
- passlib remplacé par bcrypt direct
- suppression du mot 'python' en début de fichier
"""

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel, EmailStr
from datetime import datetime, timedelta
import jwt
import os
import bcrypt

# ✅ Imports depuis app/
from database import get_dwh_db
from auth import get_current_user_from_request

SECRET_KEY = os.getenv("JWT_SECRET", "job-intelligent-secret-key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440

router = APIRouter(prefix="/api/auth", tags=["Authentication"])

# ============================================================
# MODÈLES PYDANTIC
# ============================================================
class UserRegister(BaseModel):
    email: EmailStr
    mot_de_passe: str
    nom: str
    prenom: str
    telephone: str | None = None

class UserLogin(BaseModel):
    email: EmailStr
    mot_de_passe: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int

# ============================================================
# UTILITAIRES
# ============================================================
def hash_password(pw: str) -> str:
    return bcrypt.hashpw(pw.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode('utf-8'), hashed.encode('utf-8'))

def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# ============================================================
# ROUTES
# ============================================================
@router.post("/register", response_model=TokenResponse)
async def register(user: UserRegister, db: Session = Depends(get_dwh_db)):
    existing = db.execute(
        text("SELECT id_utilisateur FROM client.utilisateurs WHERE email = :email"),
        {"email": user.email}
    ).fetchone()
    if existing:
        raise HTTPException(status_code=400, detail="Email déjà utilisé")

    hashed = hash_password(user.mot_de_passe)
    db.execute(
        text("""
            INSERT INTO client.utilisateurs
            (email, mot_de_passe, nom, prenom, telephone, date_inscription)
            VALUES (:email, :pwd, :nom, :prenom, :tel, NOW())
        """),
        {
            "email": user.email,
            "pwd": hashed,
            "nom": user.nom,
            "prenom": user.prenom,
            "tel": user.telephone
        }
    )
    db.commit()

    result = db.execute(
        text("SELECT id_utilisateur FROM client.utilisateurs WHERE email = :email"),
        {"email": user.email}
    )
    user_id = result.fetchone()[0]
    token = create_access_token({"sub": str(user_id)})
    return TokenResponse(
        access_token=token,
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )

@router.post("/login", response_model=TokenResponse)
async def login(creds: UserLogin, db: Session = Depends(get_dwh_db)):
    row = db.execute(
        text("""
            SELECT id_utilisateur, mot_de_passe, est_actif
            FROM client.utilisateurs
            WHERE email = :email
        """),
        {"email": creds.email}
    ).fetchone()

    if not row or not verify_password(creds.mot_de_passe, row[1]):
        raise HTTPException(status_code=401, detail="Email ou mot de passe incorrect")
    if not row[2]:
        raise HTTPException(status_code=403, detail="Compte désactivé")

    token = create_access_token({"sub": str(row[0])})

    try:
        db.execute(
            text("""
                INSERT INTO client.sessions
                (id_utilisateur, token_jwt, date_connexion, date_expiration, est_active)
                VALUES (:uid, :token, NOW(), NOW() + INTERVAL '1 day', true)
            """),
            {"uid": row[0], "token": token}
        )
        db.commit()
    except Exception:
        db.rollback()

    return TokenResponse(
        access_token=token,
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )

@router.get("/me")
async def get_profile(user=Depends(get_current_user_from_request)):
    return {
        "id_utilisateur": user["id"],
        "email": user["email"],
        "nom": user["nom"],
        "prenom": user["prenom"],
        "telephone": user.get("telephone"),
        "role": user.get("role", "candidat"),
        "has_cv": user.get("cv_nom_fichier") is not None
    }

@router.post("/logout")
async def logout(request: Request, db: Session = Depends(get_dwh_db)):
    auth = request.headers.get("Authorization")
    if auth and auth.startswith("Bearer "):
        token = auth.replace("Bearer ", "")
        try:
            db.execute(
                text("UPDATE client.sessions SET est_active = false WHERE token_jwt = :token"),
                {"token": token}
            )
            db.commit()
        except Exception:
            db.rollback()
    return {"success": True}