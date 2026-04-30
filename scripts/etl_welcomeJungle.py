#!/usr/bin/env python3
"""
ETL Welcome to the Jungle - Bronze → Silver + Gold
Lecture depuis Minio (bronze/raw/welcometothejungle/)
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
# APRÈS (correct)
MINIO_CLIENT = Minio(
    "minio:9000",  # ← Utilise le nom du service Docker
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
# EXTRACTION SALAIRE (multi-stratégies WTTJ)
# ============================================================
def extract_salary_from_text(text):
    if not text:
        return None
    text = str(text).lower()
    patterns = [
        (r'(\d{4,5})\s*€?\s*/\s*mois', lambda m: int(m.group(1))),
        (r'(\d{4,5})\s*€?\s*par\s*mois', lambda m: int(m.group(1))),
        (r'(\d{5,6})\s*€?\s*/\s*an', lambda m: int(m.group(1)) // 12),
        (r'(\d{5,6})\s*€?\s*par\s*an', lambda m: int(m.group(1)) // 12),
        (r'(\d{2,3})\s*k\s*€?\s*/\s*an', lambda m: int(m.group(1)) * 1000 // 12),
        (r'(\d{2,3})\s*k€', lambda m: int(m.group(1)) * 1000 // 12),
        (r'tjm\s*:\s*(\d{3,5})', lambda m: int(m.group(1)) * 20),
        (r'(\d{3,5})\s*€\s*/\s*jour', lambda m: int(m.group(1)) * 20),
        (r'salaire\s*:\s*(\d{4,5})\s*€', lambda m: int(m.group(1))),
        (r'(\d{4,5})\s*€\s*brut', lambda m: int(m.group(1))),
    ]
    for pattern, extractor in patterns:
        match = re.search(pattern, text)
        if match:
            return extractor(match)
    return None

def extract_salary_from_titre(titre):
    if not titre:
        return None
    patterns = [
        (r'(\d{4,5})\s*€', lambda m: int(m.group(1)) // 12),
        (r'(\d{2,3})\s*k\s*€', lambda m: int(m.group(1)) * 1000 // 12),
        (r'(\d{4,5})\s*€\s*/\s*an', lambda m: int(m.group(1)) // 12),
        (r'(\d{2,3})\s*k\s*/\s*an', lambda m: int(m.group(1)) * 1000 // 12),
    ]
    for pattern, extractor in patterns:
        match = re.search(pattern, titre, re.IGNORECASE)
        if match:
            return extractor(match)
    return None

def extract_salary_wttj(offre):
    description = offre.get("description", "")
    titre = offre.get("titre", "")
    if description:
        salaire = extract_salary_from_text(description)
        if salaire:
            return salaire
    if titre:
        salaire = extract_salary_from_titre(titre)
        if salaire:
            return salaire
    salaire_min = offre.get("salaire_min")
    if salaire_min and isinstance(salaire_min, (int, float)):
        if salaire_min > 10000:
            return salaire_min // 12
        elif salaire_min > 0:
            return salaire_min
    return None

# ============================================================
# NETTOYAGE SPÉCIFIQUE WTTJ
# ============================================================
def clean_titre(titre):
    if not titre:
        return ""
    titre = re.sub(r'\s*\([H/Fh/f\s/]+\s*\)\s*$', '', titre)
    titre = re.sub(r'\s*\(CDI\)\s*$', '', titre)
    titre = re.sub(r'\s*-\s*\([H/Fh/f]+\s*\)\s*$', '', titre)
    titre = re.sub(r'\s*[\d.,]+\s*[kK]?\s*€\s*', '', titre)
    titre = re.sub(r'\s+', ' ', titre)
    return titre.strip()

def clean_ville(lieu):
    if not lieu:
        return "Non spécifié"
    lieu_lower = lieu.lower()
    if "télétravail" in lieu_lower:
        return "Télétravail"
    if "france" in lieu_lower and len(lieu) < 15:
        return "France"
    ville = re.sub(r'\(\d+\)', '', lieu)
    ville = re.sub(r'\d{5}', '', ville)
    ville = ville.split(',')[0].strip()
    return ville if ville else "Non spécifié"

def clean_contrat(contrat):
    if not contrat:
        return "Non spécifié"
    contrat = contrat.upper().strip()
    mapping = {
        "CDI": "CDI", "CDD": "CDD", "STAGE": "Stage",
        "ALTERNANCE": "Alternance", "APPRENTISSAGE": "Alternance",
        "FREELANCE": "Freelance", "INTERIM": "Intérim", "VIE": "VIE"
    }
    return mapping.get(contrat, contrat)

def clean_entreprise(entreprise):
    if not entreprise:
        return "Non spécifié"
    entreprise = re.sub(r'\s*\(.*?\)\s*$', '', entreprise)
    entreprise = re.sub(r'\s+S\.?A\.?S\.?$', '', entreprise)
    entreprise = re.sub(r'\s+S\.?A\.?$', '', entreprise)
    return entreprise.strip()

# ============================================================
# LECTURE DEPUIS MINIO (BRONZE)
# ============================================================
def lire_bronze_wttj():
    """Lit tous les fichiers JSON Welcome to the Jungle depuis Minio"""
    toutes_offres = []
    print("📁 Recherche des fichiers JSON WTTJ dans Minio...")
    
    try:
        # Parcourir les dossiers dans bronze/raw/welcometothejungle/
        prefix = "raw/welcometothejungle/"
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
                            offre['source'] = "welcometothejungle"
                        toutes_offres.extend(data)
                        print(f"      ✅ {len(data)} offres")
                    elif isinstance(data, dict):
                        data['source'] = "welcometothejungle"
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
def traiter_offres_wttj(offres_brutes):
    print("\n🧹 Nettoyage et enrichissement WTTJ...")
    offres_silver = []
    offres_gold = []
    stats = {"total": 0, "avec_salaire": 0}

    for offre in offres_brutes:
        titre = offre.get("titre", "")
        description = offre.get("description", "")
        if not description:
            description = offre.get("description_snippet", "")
        lieu = offre.get("lieu", "")
        entreprise = offre.get("entreprise", "")
        contrat = offre.get("type_contrat", "")

        # --- SILVER : nettoyage de base ---
        titre_clean = clean_titre(titre)
        ville_clean = clean_ville(lieu)
        entreprise_clean = clean_entreprise(entreprise)
        contrat_clean = clean_contrat(contrat)
        salaire = extract_salary_wttj(offre)
        if salaire:
            stats["avec_salaire"] += 1

        silver_row = {
            "id": offre.get("id"),
            "source": "welcometothejungle",
            "titre_original": titre,
            "titre_clean": titre_clean,
            "entreprise": entreprise_clean,
            "lieu_original": lieu,
            "ville": ville_clean,
            "description_clean": description[:5000] if description else "",
            "description_length": len(description),
            "type_contrat_original": contrat,
            "type_contrat": contrat_clean,
            "salaire_mensuel": salaire,
            "date_publication": offre.get("date_publication"),
            "date_scraping": offre.get("date_scraping"),
            "date_nettoyage": datetime.now().isoformat(),
            "annee": datetime.now().year,
            "mois": datetime.now().month,
            "jour": datetime.now().day,
            "url": offre.get("url", ""),
            "query_recherche": offre.get("query_recherche", "")
        }
        offres_silver.append(silver_row)

        # --- GOLD : enrichissements ---
        texte_complet = f"{titre} {description}"
        competences = extract_skills(texte_complet)
        nb_competences = len(competences)

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
        if contrat_clean != "Non spécifié":
            score += 1
        if salaire is not None:
            score += 1
        score_qualite_pct = round(score / 7 * 100, 1) if score > 0 else 0

        recherche_text = " ".join([titre, description, entreprise_clean] + competences).lower()
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

    # Silver
    df_silver = pd.DataFrame(offres_silver)
    local_silver = "data_warehouse/silver"
    os.makedirs(local_silver, exist_ok=True)
    fichier_silver = f"{local_silver}/wttj_silver_{timestamp}.parquet"
    df_silver.to_parquet(fichier_silver, index=False)
    print(f"   ✅ Silver local: {fichier_silver}")

    object_silver = f"welcometothejungle/annee={annee}/mois={mois}/wttj_silver_{timestamp}.parquet"
    try:
        MINIO_CLIENT.fput_object(BUCKET_SILVER, object_silver, fichier_silver)
        print(f"   ✅ Silver Minio: {BUCKET_SILVER}/{object_silver}")
    except Exception as e:
        print(f"   ❌ Minio Silver: {e}")

    # Gold
    df_gold = pd.DataFrame(offres_gold)
    local_gold = "data_warehouse/gold"
    os.makedirs(local_gold, exist_ok=True)
    fichier_gold = f"{local_gold}/wttj_gold_{timestamp}.parquet"
    df_gold.to_parquet(fichier_gold, index=False)
    print(f"   ✅ Gold local: {fichier_gold}")

    object_gold = f"welcometothejungle/annee={annee}/mois={mois}/wttj_gold_{timestamp}.parquet"
    try:
        MINIO_CLIENT.fput_object(BUCKET_GOLD, object_gold, fichier_gold)
        print(f"   ✅ Gold Minio: {BUCKET_GOLD}/{object_gold}")
    except Exception as e:
        print(f"   ❌ Minio Gold: {e}")

# ============================================================
# STATISTIQUES (optionnel)
# ============================================================
def afficher_statistiques(offres):
    print("\n" + "=" * 50)
    print("📊 STATISTIQUES WTTJ APRÈS NETTOYAGE")
    print("=" * 50)
    df = pd.DataFrame(offres)
    print(f"\n📁 Total: {len(df)} offres")
    print(f"🏢 Entreprises: {df['entreprise'].nunique()}")
    print(f"📍 Villes: {df[df['ville'] != 'Non spécifié']['ville'].nunique()}")
    print(f"\n📄 Types de contrat:")
    for contrat, count in df['type_contrat'].value_counts().head(10).items():
        print(f"   - {contrat}: {count}")
    print(f"\n💰 Salaires (si disponibles):")
    avec_salaire = df['salaire_mensuel'].notna().sum()
    if avec_salaire > 0:
        print(f"   - Salaire moyen: {df['salaire_mensuel'].mean():.0f}€/mois")
    print(f"\n🔧 Compétences: {df['nb_competences'].sum()} total, moyenne {df['nb_competences'].mean():.1f}")

# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("🚀 ETL WELCOME TO THE JUNGLE - Bronze (Minio) → Silver + Gold")
    print("=" * 60)

    offres_brutes = lire_bronze_wttj()
    
    if not offres_brutes:
        print("⚠️ Aucune offre WTTJ trouvée dans Minio!")
        print("   Vérifiez que des fichiers existent dans: bronze/raw/welcometothejungle/")
        print("\n💡 Pour vérifier:")
        print("   docker exec -it minio-init mc ls local/bronze/raw/welcometothejungle/")
        return
    
    print(f"\n📊 Total: {len(offres_brutes)} offres brutes")

    offres_silver, offres_gold = traiter_offres_wttj(offres_brutes)
    sauvegarder(offres_silver, offres_gold)
    afficher_statistiques(offres_gold)

    print("\n" + "=" * 60)
    print("✅ Silver et Gold WTTJ créés avec succès!")
    print("=" * 60)

if __name__ == "__main__":
    main()