import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator

DWH_DATABASE_URL = os.getenv("DATABASE_DWH_URL")
USER_DATABASE_URL = os.getenv("DATABASE_USER_URL")

if not DWH_DATABASE_URL:
    # Fallback pour Docker (quand .env n’est pas chargé)
    POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "postgres-dwh")
    POSTGRES_PORT     = os.getenv("POSTGRES_PORT",     "5432")
    POSTGRES_DB       = os.getenv("POSTGRES_DB",       "job_intelligent_dwh")
    POSTGRES_USER     = os.getenv("POSTGRES_USER",     "dwh_user")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "dwh_password")
    DWH_DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

if not USER_DATABASE_URL:
    USER_DATABASE_URL = DWH_DATABASE_URL

dwh_engine = create_engine(DWH_DATABASE_URL)
user_engine = create_engine(USER_DATABASE_URL)

DwhSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=dwh_engine)
UserSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=user_engine)

def get_dwh_db() -> Generator[Session, None, None]:
    db = DwhSessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_user_db() -> Generator[Session, None, None]:
    db = UserSessionLocal()
    try:
        yield db
    finally:
        db.close()