# backend/app/client_db.py
from sqlalchemy.orm import Session
from .database import dwh_engine
from .client_models import BaseClient, Utilisateur, SessionUtilisateur
from datetime import datetime, timedelta

# Créer les tables (à exécuter une fois au démarrage)
def init_client_schema():
    BaseClient.metadata.create_all(bind=dwh_engine)

def get_user_by_email(db: Session, email: str):
    return db.query(Utilisateur).filter(Utilisateur.email == email).first()

def create_user(db: Session, email: str, hashed_password: str, nom: str = None):
    user = Utilisateur(email=email, mot_de_passe=hashed_password, nom=nom)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

def save_user_session(db: Session, user_id: int, token: str, expires_in_hours: int = 24*7):
    expires_at = datetime.utcnow() + timedelta(hours=expires_in_hours)
    session = SessionUtilisateur(user_id=user_id, token=token, expires_at=expires_at)
    db.add(session)
    db.commit()
    return session

def get_user_by_token(db: Session, token: str):
    session = db.query(SessionUtilisateur).filter(
        SessionUtilisateur.token == token,
        SessionUtilisateur.expires_at > datetime.utcnow()
    ).first()
    if session:
        return db.query(Utilisateur).filter(Utilisateur.id == session.user_id).first()
    return None