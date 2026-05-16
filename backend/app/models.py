#!/usr/bin/env python3
"""
Modèles SQLAlchemy pour utilisateurs
"""

from sqlalchemy import Column, Integer, String, DateTime, JSON, ForeignKey
from datetime import datetime
from auth_db import Base


class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), unique=True, index=True, nullable=False)
    hashed_password = Column(String(200), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    cv_skills = Column(JSON, default=[])  # Compétences extraites du CV
    last_recommendations = Column(JSON, default=[])


class UserAlert(Base):
    __tablename__ = "user_alerts"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    alert_type = Column(String(50))  # 'keyword', 'company', 'contract', 'salary'
    alert_value = Column(String(200))
    is_active = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)


class CVHistory(Base):
    __tablename__ = "cv_history"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    filename = Column(String(255))
    skills = Column(JSON)
    recommendations = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)