# backend/app/client_models.py
from sqlalchemy import Column, Integer, String, DateTime, JSON, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

BaseClient = declarative_base()

class Utilisateur(BaseClient):
    __tablename__ = "utilisateurs"
    __table_args__ = {"schema": "client"}
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    mot_de_passe = Column(String(255), nullable=False)
    nom = Column(String(100))
    prenom = Column(String(100))
    date_inscription = Column(DateTime, default=datetime.utcnow)
    cv_skills = Column(JSON, default=[])
    cv_filename = Column(String(255))
    est_actif = Column(Boolean, default=True)

class SessionUtilisateur(BaseClient):
    __tablename__ = "sessions"
    __table_args__ = {"schema": "client"}
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)
    token = Column(String(500))
    expires_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)