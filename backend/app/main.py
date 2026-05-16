#!/usr/bin/env python3
"""
Job Intelligent API - Point d'entrée principal
"""

import sys
import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from dotenv import load_dotenv

# ============================================================
# CHARGEMENT ROBUSTE DU FICHIER .env
# ============================================================
backend_dir = Path(__file__).parent.parent          # dossier backend
project_dir = backend_dir.parent                    # dossier racine du projet

env_locations = [
    backend_dir / ".env",       # backend/.env
    project_dir / ".env"        # projet_job_intelligent/.env
]

env_loaded = False
for env_path in env_locations:
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        print(f"✅ Fichier .env chargé depuis : {env_path}")
        env_loaded = True
        break

if not env_loaded:
    print("⚠️ Aucun fichier .env trouvé. Utilisation des variables d'environnement système ou des valeurs par défaut.")

print(f"DATABASE_DWH_URL = {os.getenv('DATABASE_DWH_URL')}")

# ============================================================
# AJOUTER LES CHEMINS D'IMPORT
# ============================================================
app_dir = Path(__file__).parent                     # backend/app
sys.path.insert(0, str(app_dir))                    # pour importer depuis app/
routers_dir = backend_dir / "routers"               # backend/routers
sys.path.insert(0, str(routers_dir))

# ============================================================
# IMPORT DES ROUTERS
# ============================================================
from auth_routes           import router as auth_router
from assistant_routes      import router as assistant_router
from offers_routes         import router as offers_router
from recommendation_routes import router as recommendation_router
from skills_routes import router as skills_router
# ============================================================
# APP FASTAPI
# ============================================================
app = FastAPI(
    title="Job Intelligent API",
    description="Plateforme de recommandation d'offres Data",
    version="1.0.0"
)

# ============================================================
# MIDDLEWARE CORS
# ============================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# FICHIERS STATIQUES (CSS, JS, images)
# ============================================================
static_path = Path(__file__).parent / "static"
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

# ============================================================
# ROUTES PAGES HTML (avec FileResponse)
# ============================================================
templates_dir = Path(__file__).parent / "templates"

@app.get("/")
async def index():
    return FileResponse(templates_dir / "index.html")

@app.get("/offers")
async def offers_page():
    return FileResponse(templates_dir / "offers.html")

@app.get("/cv")
async def cv_page():
    return FileResponse(templates_dir / "cv.html")

@app.get("/assistant")
async def assistant_page():
    return FileResponse(templates_dir / "assistant.html")



@app.get("/login")
async def login_page():
    return FileResponse(templates_dir / "login.html")

@app.get("/signup")
async def signup_page():
    return FileResponse(templates_dir / "signup.html")

# ============================================================
# ROUTERS API
# ============================================================
app.include_router(auth_router)
app.include_router(assistant_router)
app.include_router(offers_router)
app.include_router(recommendation_router)
app.include_router(skills_router)
# ============================================================
# ENDPOINTS DE BASE
# ============================================================
@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "Job Intelligent API",
        "version": "1.0.0"
    }

@app.get("/api/root")
def root():
    return {
        "message": "Job Intelligent API",
        "docs": "/docs",
        "health": "/health"
    }
@app.get("/profile")
async def profile_page():
    return FileResponse(templates_dir / "profile.html")