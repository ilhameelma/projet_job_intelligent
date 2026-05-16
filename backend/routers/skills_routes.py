from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.database import get_dwh_db

router = APIRouter(prefix="/api/skills", tags=["Skills"])

@router.get("/top")
async def top_skills(limit: int = 12, db: Session = Depends(get_dwh_db)):
    """Top compétences les plus demandées (nombre d'offres associées)"""
    query = """
        SELECT c.nom, COUNT(DISTINCT oc.id_offre) as count
        FROM gold.competences c
        JOIN gold.offre_competence oc ON c.id_competence = oc.id_competence
        GROUP BY c.id_competence, c.nom
        ORDER BY count DESC
        LIMIT :limit
    """
    result = db.execute(text(query), {"limit": limit})
    skills = [{"nom": row[0], "count": row[1]} for row in result]
    return {"success": True, "skills": skills}