#!/usr/bin/env python3
import io
import time
from typing import List, Optional
from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from pypdf import PdfReader
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from database import get_dwh_db, get_user_db
from auth import get_current_user_from_request

router = APIRouter(prefix="/api/recommendations", tags=["Recommendations"])

# Charger le modèle multilingue (une fois au démarrage)
model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

DATA_SKILLS = [
    "python", "sql", "spark", "airflow", "docker", "kubernetes", "aws", "azure",
    "tensorflow", "pytorch", "pandas", "numpy", "kafka", "flink", "scala", "java",
    "databricks", "dbt", "looker", "gcp", "hadoop", "scikit-learn", "powerbi",
    "tableau", "mongodb", "postgresql", "mysql", "redis"
]

def extract_skills(file_bytes: bytes, filename: str) -> List[str]:
    text_content = ""
    if filename.lower().endswith('.pdf'):
        try:
            pdf = PdfReader(io.BytesIO(file_bytes))
            for page in pdf.pages:
                text_content += page.extract_text() or ""
        except Exception:
            text_content = file_bytes.decode('utf-8', errors='ignore')
    else:
        text_content = file_bytes.decode('utf-8', errors='ignore')
    text_content = ' '.join(text_content.split())
    lower = text_content.lower()
    return list(set(s for s in DATA_SKILLS if s in lower))

def compute_match_score(cv_text: str, cv_skills: List[str],
                        offre_text: str, offre_skills: List[str],
                        cv_ville: str, offre_ville: str,
                        contrat_ok: bool) -> float:
    emb_cv = model.encode([cv_text[:512]])[0]
    emb_offre = model.encode([offre_text[:512]])[0]
    cos_sim = float(np.dot(emb_cv, emb_offre) / (np.linalg.norm(emb_cv) * np.linalg.norm(emb_offre)))
    cv_set = set(s.lower() for s in cv_skills)
    off_set = set(s.lower() for s in offre_skills)
    kw_score = len(cv_set & off_set) / max(len(off_set), 1)
    loc_ok = 1.0 if cv_ville.lower() in offre_ville.lower() else 0.5
    filter_score = (float(contrat_ok) + loc_ok) / 2
    final = 0.50 * cos_sim + 0.35 * kw_score + 0.15 * filter_score
    return round(final * 100, 1)
def get_recommendations(skills: List[str], dwh_db: Session,
                        cv_ville: str, cv_contrat: str, limit=10):
    # 1. Récupérer toutes les offres avec l'URL
    rows = dwh_db.execute(text("""
        SELECT
            o.id_offre,
            o.titre_clean,
            COALESCE(e.nom, 'Entreprise inconnue') AS entreprise,
            o.ville,
            o.salaire_mensuel,
            o.source,
            o.description_clean AS description,
            o.type_contrat,
            o.url,
            array_agg(DISTINCT c.nom) FILTER (WHERE c.nom IS NOT NULL) AS competences_offre
        FROM gold.offres o
        LEFT JOIN gold.entreprises e ON o.id_entreprise = e.id_entreprise
        LEFT JOIN gold.offre_competence oc ON o.id_offre = oc.id_offre
        LEFT JOIN gold.competences c ON oc.id_competence = c.id_competence
        WHERE o.titre_clean IS NOT NULL
        GROUP BY o.id_offre, o.titre_clean, e.nom, o.ville, o.salaire_mensuel,
                 o.source, o.description_clean, o.type_contrat, o.url
    """)).fetchall()

    # 2. Pré-filtrer : garder uniquement les offres ayant au moins une compétence commune
    cv_skills_set = set(s.lower() for s in skills)
    filtered_rows = []
    for r in rows:
        offre_skills = set(s.lower() for s in (r.competences_offre or []))
        if cv_skills_set & offre_skills:   # intersection non vide
            filtered_rows.append(r)
        if len(filtered_rows) >= 100:      # 🔥 MODIFIÉ : 100 au lieu de 500
            break

    cv_text = " ".join(skills)
    results = []
    for r in filtered_rows:
        offre_skills = r.competences_offre or []
        contrat_ok = (cv_contrat == r.type_contrat) if cv_contrat else True
        score = compute_match_score(
            cv_text, skills,
            r.description or "", offre_skills,
            cv_ville, r.ville,
            contrat_ok
        )
        results.append({
            "id_offre": r[0],
            "titre": r[1],
            "entreprise": r[2],
            "ville": r[3],
            "salaire": r[4],
            "source": r[5],
            "url": r[8] or "#",
            "score_match": score,
            "competences_matchees": list(cv_skills_set & set(offre_skills))
        })
    results.sort(key=lambda x: x["score_match"], reverse=True)
    return results[:limit]

@router.post("/upload-cv")
async def upload_cv(
    file: UploadFile = File(...),
    ville_souhaitee: Optional[str] = Form(None),
    contrat_souhaite: Optional[str] = Form(None),
    user = Depends(get_current_user_from_request),
    user_db: Session = Depends(get_user_db),
    dwh_db: Session = Depends(get_dwh_db)
):
    start_time = time.time()

    ville = ville_souhaitee if ville_souhaitee else "Paris"
    contrat = contrat_souhaite if contrat_souhaite else "CDI"

    content = await file.read()
    skills = extract_skills(content, file.filename)
    recos = get_recommendations(skills, dwh_db, ville, contrat)

    # Sauvegarde en base
    user_db.execute(
        text("""
            UPDATE client.utilisateurs
            SET cv_original = :data, cv_nom_fichier = :name,
                cv_type_fichier = :type, cv_date_upload = NOW(),
                ville_souhaitee = :ville, contrat_souhaite = :contrat
            WHERE id_utilisateur = :uid
        """),
        {"data": content, "name": file.filename, "type": file.content_type,
         "uid": user["id"], "ville": ville, "contrat": contrat}
    )
    user_db.execute(text("DELETE FROM client.competences_utilisateur WHERE id_utilisateur = :uid"), {"uid": user["id"]})
    for s in skills:
        user_db.execute(
            text("INSERT INTO client.competences_utilisateur (id_utilisateur, nom_competence, source_detection) VALUES (:uid, :skill, 'ia')"),
            {"uid": user["id"], "skill": s}
        )
    for r in recos:
        score_natif = float(r["score_match"])
        user_db.execute(
            text("""
                INSERT INTO client.recommandations (id_utilisateur, id_offre, source, score_recommandation)
                VALUES (:uid, :oid, :src, :score)
            """),
            {"uid": user["id"], "oid": r["id_offre"], "src": r["source"], "score": score_natif}
        )
    user_db.commit()

    elapsed = time.time() - start_time
    print(f"⏱️ Analyse CV terminée en {elapsed:.2f} secondes")

    return {
        "success": True,
        "skills": skills,
        "recommendations": recos,
        "processing_time_seconds": round(elapsed, 2)
    }

@router.get("/my-recommendations")
async def my_recs(
    user = Depends(get_current_user_from_request),
    user_db: Session = Depends(get_user_db),
    dwh_db: Session = Depends(get_dwh_db)
):
    user_info = user_db.execute(
        text("SELECT ville_souhaitee, contrat_souhaite FROM client.utilisateurs WHERE id_utilisateur = :uid"),
        {"uid": user["id"]}
    ).fetchone()
    ville = user_info[0] if user_info and user_info[0] else "Paris"
    contrat = user_info[1] if user_info and user_info[1] else "CDI"
    skills = [row[0] for row in user_db.execute(
        text("SELECT nom_competence FROM client.competences_utilisateur WHERE id_utilisateur = :uid"),
        {"uid": user["id"]}
    ).fetchall()]
    if not skills:
        return {"success": True, "has_cv": False}
    recos = get_recommendations(skills, dwh_db, ville, contrat)
    for rec in recos:
        rec["score_match"] = float(rec["score_match"])
    return {"success": True, "has_cv": True, "skills": skills, "recommendations": recos}