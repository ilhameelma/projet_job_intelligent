#!/usr/bin/env python3
"""
ETL France Travail - Bronze → Silver + Gold
Lecture depuis Minio (bronze/raw/france_travail/)
Nettoyage et enrichissement (compétences, score qualité, recherche textuelle)
"""

import json
import re
import pandas as pd
from datetime import datetime
import os
from minio import Minio

# ============================================================
# CONFIGURATION MINIO
# ============================================================
MINIO_CLIENT = Minio(
    "localhost:9000",
    access_key="minio_admin",
    secret_key="minio_password",
    secure=False
)
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"

# Liste des compétences (identique à Adzuna)
SKILLS_KEYWORDS = {
    "Python": r'\bpython\b',
    "SQL": r'\bsql\b',
    "Spark": r'\bspark\b',
    "AWS": r'\baws\b',
    "Azure": r'\bazure\b',
    "GCP": r'\bgcp\b',
    "Docker": r'\bdocker\b',
    "Kubernetes": r'\bkubernetes\b',
    "TensorFlow": r'\btensorflow\b',
    "PyTorch": r'\bpytorch\b',
    "Scikit-learn": r'\bscikit[-]?learn\b',
    "Pandas": r'\bpandas\b',
    "NumPy": r'\bnumpy\b',
    "Power BI": r'\bpower\s*bi\b',
    "Tableau": r'\btableau\b',
    "Airflow": r'\bairflow\b',
    "Kafka": r'\bkafka\b',
    "Hadoop": r'\bhadoop\b',
    "Java": r'\bjava\b',
    "Scala": r'\bscala\b',
    "R": r'\br\b(?!obot)',
    "Git": r'\bgit\b',
    "CI/CD": r'\bci[/]?cd\b',
}

def extract_skills(text):
    """Extrait les compétences techniques d'un texte"""
    if not text:
        return []
    text = str(text).lower()
    found = []
    for skill, pattern in SKILLS_KEYWORDS.items():
        if re.search(pattern, text):
            found.append(skill)
    return list(set(found))

# ============================================================
# EXTRACTION SALAIRE
# ============================================================
def extract_salary(text):
    """Extraction améliorée des salaires"""
    if not text:
        return None
    text = str(text).lower()
    patterns = [
        (r'(\d{4,5})\s*€?\s*/\s*mois', lambda m: int(m.group(1))),
        (r'(\d{4,5})\s*€?\s*par\s*mois', lambda m: int(m.group(1))),
        (r'(\d{4,5})\s*[-]\s*(\d{4,5})\s*€?\s*/\s*mois',
         lambda m: (int(m.group(1)) + int(m.group(2))) // 2),
        (r'(\d{5,6})\s*€?\s*/\s*an', lambda m: int(m.group(1)) // 12),
        (r'(\d{5,6})\s*€?\s*par\s*an', lambda m: int(m.group(1)) // 12),
        (r'(\d{5,6})\s*[-]\s*(\d{5,6})\s*€?\s*/\s*an',
         lambda m: (int(m.group(1)) + int(m.group(2))) // 24),
        (r'(\d{2,3})\s*k\s*€?\s*/\s*an', lambda m: int(m.group(1)) * 1000 // 12),
        (r'(\d{2,3})\s*k€', lambda m: int(m.group(1)) * 1000 // 12),
        (r'salaire\s*:\s*(\d{4,5})\s*€', lambda m: int(m.group(1))),
        (r'(\d{4,5})\s*€\s*brut\s*/\s*mois', lambda m: int(m.group(1))),
        (r'tjm\s*:\s*(\d{3,5})', lambda m: int(m.group(1)) * 20),
        (r'(\d{3,5})\s*€\s*/\s*jour', lambda m: int(m.group(1)) * 20),
        (r'à\s*partir\s*de\s*(\d{4,5})\s*€', lambda m: int(m.group(1))),
    ]
    for pattern, extractor in patterns:
        match = re.search(pattern, text)
        if match:
            return extractor(match)
    return None

# ============================================================
# FONCTIONS DE NETTOYAGE (SILVER)
# ============================================================
def clean_titre(titre):
    if not titre:
        return ""
    titre = re.sub(r'\s*\([H/Fh/f]+\)\s*$', '', titre)
    titre = re.sub(r'\s*-\s*\([H/Fh/f]+\)\s*$', '', titre)
    return titre.strip().title()

def extract_ville(lieu):
    if not lieu:
        return "Non spécifié"
    ville = lieu.split(',')[0].strip()
    ville = re.sub(r'\d{5}', '', ville).strip()
    ville = re.sub(r'^[0-9]{2,3}\s*-\s*', '', ville)
    return ville if ville else "Non spécifié"

def normalize_contrat(contrat):
    if not contrat:
        return "Non spécifié"
    contrat = contrat.upper()
    mapping = {
        "CDI": "CDI", "CDD": "CDD", "INTERIM": "Intérim",
        "MIS": "Intérim", "FREELANCE": "Freelance", "LIB": "Freelance",
        "STAGE": "Stage", "ALTERNANCE": "Alternance", "APPRENTISSAGE": "Alternance"
    }
    return mapping.get(contrat, "Non spécifié")

# ============================================================
# LECTURE DEPUIS MINIO (BRONZE)
# ============================================================
def lire_bronze():
    """Lit tous les fichiers JSON France Travail depuis Minio"""
    toutes_offres = []
    print("📁 Recherche des fichiers JSON dans Minio...")
    
    try:
        # Parcourir les dossiers dans bronze/raw/france_travail/
        prefix = "raw/france_travail/"
        objects = MINIO_CLIENT.list_objects(BUCKET_BRONZE, prefix=prefix, recursive=True)
        
        for obj in objects:
            if obj.object_name.endswith('.json'):
                print(f"   📖 Lecture: {obj.object_name}")
                try:
                    response = MINIO_CLIENT.get_object(BUCKET_BRONZE, obj.object_name)
                    content = response.read().decode('utf-8')
                    data = json.loads(content)
                    
                    if isinstance(data, list):
                        for offre in data:
                            offre['source'] = "france_travail"
                        toutes_offres.extend(data)
                        print(f"      ✅ {len(data)} offres")
                    elif isinstance(data, dict):
                        data['source'] = "france_travail"
                        toutes_offres.append(data)
                        print(f"      ✅ 1 offre")
                    else:
                        print(f"      ⚠️ Format inconnu: {type(data)}")
                        
                except Exception as e:
                    print(f"      ❌ Erreur: {e}")
                    
    except Exception as e:
        print(f"   ❌ Erreur connexion Minio: {e}")
        print("   💡 Vérifiez que Minio est démarré: docker-compose up -d minio")
    
    return toutes_offres

# ============================================================
# TRAITEMENT (SILVER + GOLD)
# ============================================================
def traiter_offres(offres_brutes):
    print("\n🧹 Nettoyage et enrichissement...")
    offres_silver = []
    offres_gold = []
    stats = {"total": 0, "avec_salaire": 0}

    for offre in offres_brutes:
        description = offre.get("description", "")
        titre = offre.get("titre", "")
        entreprise = offre.get("entreprise", "Non spécifié")
        lieu = offre.get("lieu", "")
        type_contrat_original = offre.get("type_contrat", "")

        # --- SILVER : nettoyage de base ---
        salaire = extract_salary(description)
        if salaire is None:
            salaire_min = offre.get("salaire_min")
            if salaire_min and salaire_min > 10000:
                salaire = salaire_min // 12
            elif salaire_min and salaire_min < 10000:
                salaire = salaire_min
        if salaire:
            stats["avec_salaire"] += 1

        silver_row = {
            "id": offre.get("id"),
            "source": offre.get("source", "unknown"),
            "titre_original": titre,
            "titre_clean": clean_titre(titre),
            "entreprise": entreprise,
            "lieu_original": lieu,
            "ville": extract_ville(lieu),
            "description_clean": description[:5000],
            "description_length": len(description),
            "type_contrat_original": type_contrat_original,
            "type_contrat": normalize_contrat(type_contrat_original),
            "salaire_mensuel": salaire,
            "date_publication": offre.get("date_publication"),
            "date_scraping": offre.get("date_scraping"),
            "date_nettoyage": datetime.now().isoformat(),
            "annee": datetime.now().year,
            "mois": datetime.now().month,
            "jour": datetime.now().day
        }
        offres_silver.append(silver_row)

        # --- GOLD : enrichissements ---
        texte_complet = f"{titre} {description}"
        competences = extract_skills(texte_complet)
        nb_competences = len(competences)

        # Score de qualité (identique à Adzuna)
        score = 0
        if len(description) > 500:
            score += 2
        elif len(description) > 100:
            score += 1
        if nb_competences >= 5:
            score += 3
        elif nb_competences >= 3:
            score += 2
        elif nb_competences >= 1:
            score += 1
        if silver_row["type_contrat"] != "Non spécifié":
            score += 1
        if salaire is not None:
            score += 1
        score_qualite_pct = round(score / 7 * 100, 1) if score > 0 else 0

        # Texte de recherche
        recherche_text = " ".join([titre, description, entreprise] + competences).lower()
        recherche_text = re.sub(r'[^\w\s]', ' ', recherche_text)

        gold_row = silver_row.copy()
        gold_row.update({
            "competences": competences,
            "nb_competences": nb_competences,
            "score_qualite": score,
            "score_qualite_pct": score_qualite_pct,
            "recherche_text": recherche_text
        })
        offres_gold.append(gold_row)

        stats["total"] += 1

    print(f"   ✅ {stats['total']} offres traitées")
    print(f"   💰 Dont {stats['avec_salaire']} avec salaire ({stats['avec_salaire']/stats['total']*100:.1f}%)" if stats['total'] > 0 else "   💰 0 offres avec salaire")
    return offres_silver, offres_gold

# ============================================================
# SAUVEGARDE (Silver + Gold séparés)
# ============================================================
def sauvegarder(offres_silver, offres_gold):
    print("\n💾 Sauvegarde Silver et Gold...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    annee = datetime.now().year
    mois = datetime.now().month

    # --- Silver ---
    df_silver = pd.DataFrame(offres_silver)
    local_silver = "data_warehouse/silver"
    os.makedirs(local_silver, exist_ok=True)
    fichier_silver = f"{local_silver}/france_travail_silver_{timestamp}.parquet"
    df_silver.to_parquet(fichier_silver, index=False)
    print(f"   ✅ Silver local: {fichier_silver}")

    object_silver = f"france_travail/annee={annee}/mois={mois}/france_travail_silver_{timestamp}.parquet"
    try:
        MINIO_CLIENT.fput_object(BUCKET_SILVER, object_silver, fichier_silver)
        print(f"   ✅ Silver Minio: {BUCKET_SILVER}/{object_silver}")
    except Exception as e:
        print(f"   ❌ Minio Silver: {e}")

    # --- Gold ---
    df_gold = pd.DataFrame(offres_gold)
    local_gold = "data_warehouse/gold"
    os.makedirs(local_gold, exist_ok=True)
    fichier_gold = f"{local_gold}/france_travail_gold_{timestamp}.parquet"
    df_gold.to_parquet(fichier_gold, index=False)
    print(f"   ✅ Gold local: {fichier_gold}")

    object_gold = f"france_travail/annee={annee}/mois={mois}/france_travail_gold_{timestamp}.parquet"
    try:
        MINIO_CLIENT.fput_object(BUCKET_GOLD, object_gold, fichier_gold)
        print(f"   ✅ Gold Minio: {BUCKET_GOLD}/{object_gold}")
    except Exception as e:
        print(f"   ❌ Minio Gold: {e}")

# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("🚀 ETL FRANCE TRAVAIL - Bronze (Minio) → Silver + Gold")
    print("=" * 60)

    offres_brutes = lire_bronze()
    print(f"\n📊 Total: {len(offres_brutes)} offres brutes")
    
    if not offres_brutes:
        print("⚠️ Aucune offre trouvée dans Minio!")
        print("   Vérifiez que des fichiers existent dans: bronze/raw/france_travail/")
        print("\n💡 Pour vérifier:")
        print("   docker exec -it minio-init mc ls local/bronze/raw/france_travail/")
        return

    offres_silver, offres_gold = traiter_offres(offres_brutes)
    sauvegarder(offres_silver, offres_gold)

    print("\n" + "=" * 60)
    print("✅ Silver et Gold créés avec succès!")
    print("=" * 60)

if __name__ == "__main__":
    main()