#!/usr/bin/env python3
"""
Assistant IA - Chatbot NLP
Répond aux questions sur les offres, conseils carrière, etc.
"""

import re
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

# Intentions de base
INTENTS = {
    "salaire": {
        "keywords": ["salaire", "rémunération", "combien", "paye", "rémunéré"],
        "response": "Le salaire moyen pour un poste {role} est d'environ {salary}€/mois. Les offres varient entre {min_salary}€ et {max_salary}€."
    },
    "competences": {
        "keywords": ["compétence", "skill", "formation", "apprendre"],
        "response": "Les compétences les plus demandées dans le secteur Data sont : {skills}. Souhaitez-vous des formations spécifiques ?"
    },
    "offres": {
        "keywords": ["offre", "poste", "job", "emploi", "recrute"],
        "response": "Il y a actuellement {total} offres Data en France. Voici quelques-unes des plus récentes : {offers}."
    },
    "evolution": {
        "keywords": ["évolution", "carrière", "progression", "futur"],
        "response": "Le marché Data est en forte croissance (+{growth}% cette année). Les rôles comme Data Engineer et MLOps sont particulièrement recherchés."
    },
    "cv": {
        "keywords": ["cv", "curriculum", "candidature", "postuler"],
        "response": "Pour optimiser votre CV, mettez en avant vos compétences techniques (Python, SQL, Spark) et vos projets concrets. Notre IA peut analyser votre CV gratuitement !"
    }
}


def get_top_skills(db: Session, limit: int = 5) -> List[str]:
    """Récupère les compétences les plus demandées"""
    result = db.execute(text("""
        SELECT c.nom, COUNT(*) as count
        FROM gold.competences c
        JOIN gold.offre_competence oc ON c.id_competence = oc.id_competence
        GROUP BY c.nom
        ORDER BY count DESC
        LIMIT :limit
    """), {"limit": limit})
    return [row[0] for row in result.fetchall()]


def get_total_offres(db: Session) -> int:
    """Récupère le nombre total d'offres"""
    result = db.execute(text("SELECT COUNT(*) FROM gold.offres"))
    return result.fetchone()[0]


def get_salary_stats(db: Session, role: Optional[str] = None) -> Dict:
    """Récupère les statistiques de salaire"""
    if role:
        result = db.execute(text("""
            SELECT 
                AVG(salaire_mensuel) as avg_salary,
                MIN(salaire_mensuel) as min_salary,
                MAX(salaire_mensuel) as max_salary
            FROM gold.offres
            WHERE salaire_mensuel IS NOT NULL
            AND (titre_clean ILIKE :role OR titre_original ILIKE :role)
        """), {"role": f"%{role}%"})
    else:
        result = db.execute(text("""
            SELECT 
                AVG(salaire_mensuel) as avg_salary,
                MIN(salaire_mensuel) as min_salary,
                MAX(salaire_mensuel) as max_salary
            FROM gold.offres
            WHERE salaire_mensuel IS NOT NULL
        """))
    
    row = result.fetchone()
    return {
        "avg_salary": round(row[0] or 0),
        "min_salary": round(row[1] or 0),
        "max_salary": round(row[2] or 0)
    }


def get_market_growth(db: Session) -> float:
    """Calcule la croissance du marché (offres récentes vs anciennes)"""
    result = db.execute(text("""
        SELECT 
            COUNT(CASE WHEN date_scraping > NOW() - INTERVAL '30 days' THEN 1 END) as recent,
            COUNT(CASE WHEN date_scraping < NOW() - INTERVAL '60 days' THEN 1 END) as past
        FROM gold.offres
    """))
    row = result.fetchone()
    recent = row[0] or 0
    past = row[1] or 1
    return round((recent - past) / past * 100, 1) if past > 0 else 0


def get_ai_response(message: str, db: Session) -> str:
    """
    Génère une réponse IA basée sur l'intention détectée
    """
    message_lower = message.lower()
    
    # Détecter l'intention
    intent = None
    detected_role = None
    
    for intent_name, intent_data in INTENTS.items():
        for keyword in intent_data["keywords"]:
            if keyword in message_lower:
                intent = intent_name
                break
        if intent:
            break
    
    # Chercher un rôle mentionné (Data Scientist, Data Engineer, etc.)
    roles = ["data scientist", "data engineer", "data analyst", "ml engineer", "data architect"]
    for role in roles:
        if role in message_lower:
            detected_role = role.title()
            break
    
    # Générer la réponse selon l'intention
    if intent == "salaire":
        salary_stats = get_salary_stats(db, detected_role)
        role_text = detected_role if detected_role else "data"
        return f"Le salaire moyen pour un poste {role_text} est d'environ {salary_stats['avg_salary']}€/mois. Les offres varient entre {salary_stats['min_salary']}€ et {salary_stats['max_salary']}€."
    
    elif intent == "competences":
        skills = get_top_skills(db, 10)
        return f"🔧 **Compétences les plus demandées :**\n\n" + "\n".join([f"• {s}" for s in skills[:10]]) + "\n\nSouhaitez-vous des informations sur des formations spécifiques pour ces compétences ?"
    
    elif intent == "offres":
        total = get_total_offres(db)
        return f"📊 Il y a actuellement **{total} offres Data en France**. \n\nAccédez à la page 'Offres' pour les consulter en détail et filtrer par compétences, ville ou type de contrat."
    
    elif intent == "evolution":
        growth = get_market_growth(db)
        return f"📈 Le marché Data est en forte croissance (**+{growth}% cette année**). Les rôles comme **Data Engineer**, **MLOps** et **Data Scientist** sont particulièrement recherchés. Souhaitez-vous des conseils pour évoluer vers ces métiers ?"
    
    elif intent == "cv":
        return "📄 Pour optimiser votre CV Data :\n\n1. Mettez en avant vos compétences techniques (Python, SQL, Spark, Cloud)\n2. Détaillez vos projets concrets avec des résultats quantifiables\n3. Mentionnez vos certifications (AWS, Databricks, etc.)\n\n**Notre IA peut analyser votre CV gratuitement !** Uploadez-le dans la section 'Mon CV'."
    
    else:
        # Réponse par défaut en cas d'intention non détectée
        return f"Je suis votre assistant IA pour le marché Data. Je peux vous renseigner sur :\n\n• 💰 **Salaire** : salaires moyens par poste\n• 🔧 **Compétences** : les skills les plus demandés\n• 📊 **Offres** : nombre d'offres disponibles\n• 📈 **Évolution** : tendances du marché Data\n• 📄 **CV** : conseils pour optimiser votre CV\n\nQue souhaitez-vous savoir ?"