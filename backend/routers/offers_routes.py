from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.database import get_dwh_db

router = APIRouter(prefix="/api/offers", tags=["Offers"])

@router.get("/recent")
async def recent_offers(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_dwh_db)
):
    try:
        query = """
            SELECT 
                o.id_offre,
                o.titre_clean,
                e.nom AS entreprise,
                o.ville,
                o.salaire_mensuel,
                o.source,
                o.url,                                              -- URL ajoutée
                COALESCE(array_agg(DISTINCT c.nom) FILTER (WHERE c.nom IS NOT NULL), '{}') AS competences
            FROM gold.offres o
            LEFT JOIN gold.entreprises e ON o.id_entreprise = e.id_entreprise
            LEFT JOIN gold.offre_competence oc ON o.id_offre = oc.id_offre
            LEFT JOIN gold.competences c ON oc.id_competence = c.id_competence
            WHERE o.titre_clean IS NOT NULL
            GROUP BY o.id_offre, o.titre_clean, e.nom, o.ville, o.salaire_mensuel, o.source, o.url
            ORDER BY o.date_scraping DESC
            LIMIT :limit
        """
        result = db.execute(text(query), {"limit": limit})
        offers = []
        for row in result:
            offer = dict(row._mapping)
            comp_count = len(offer.get("competences") or [])
            offer["score_match"] = min(60 + comp_count * 2, 99) if comp_count else 65
            offers.append(offer)
        
        total = db.execute(text("SELECT COUNT(*) FROM gold.offres WHERE titre_clean IS NOT NULL")).scalar()
        return {"success": True, "offers": offers, "total": total}
    except Exception as e:
        return {"success": False, "error": str(e), "offers": [], "total": 0}

@router.get("/search")
async def search_offers(
    q: str = Query("", description="Mot-clé de recherche"),
    db: Session = Depends(get_dwh_db)
):
    """Recherche d'offres par mot-clé (titre ou description)"""
    if not q:
        return {"success": True, "offers": []}
    try:
        query = """
            SELECT 
                o.id_offre,
                o.titre_clean,
                e.nom AS entreprise,
                o.ville,
                o.salaire_mensuel,
                o.source,
                o.url,
                o.type_contrat,
                COALESCE(array_agg(DISTINCT c.nom) FILTER (WHERE c.nom IS NOT NULL), '{}') AS competences
            FROM gold.offres o
            LEFT JOIN gold.entreprises e ON o.id_entreprise = e.id_entreprise
            LEFT JOIN gold.offre_competence oc ON o.id_offre = oc.id_offre
            LEFT JOIN gold.competences c ON oc.id_competence = c.id_competence
            WHERE o.titre_clean ILIKE :q OR o.description_clean ILIKE :q
            GROUP BY o.id_offre, o.titre_clean, e.nom, o.ville, o.salaire_mensuel, o.source, o.url, o.type_contrat
            ORDER BY o.date_scraping DESC
            LIMIT 50
        """
        result = db.execute(text(query), {"q": f"%{q}%"})
        offers = []
        for row in result:
            offer = dict(row._mapping)
            comp_count = len(offer.get("competences") or [])
            offer["score_match"] = min(60 + comp_count * 2, 99) if comp_count else 65
            offers.append(offer)
        return {"success": True, "offers": offers}
    except Exception as e:
        return {"success": False, "error": str(e), "offers": []}