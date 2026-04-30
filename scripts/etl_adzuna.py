#!/usr/bin/env python3
"""
ETL PANDAS - ADZUNA (Version corrigée)
Bronze (Minio) → Silver (Minio) → Gold (Minio)

✅ CORRECTIONS :
- Colonnes renommées pour correspondre à load_to_postgresql.py
  titre       → titre_original + titre_clean
  lieu        → lieu_original  + ville
  salaire_min → salaire_mensuel
- description → description_clean
- date_ingestion → date_nettoyage
"""

import pandas as pd
import numpy as np
import os
import re
import json
import boto3
import io
from datetime import datetime
from collections import Counter

# ============================================================
# 1. CONFIGURATION MINIO (S3)
# ============================================================
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
MINIO_BUCKET_BRONZE = "bronze"
MINIO_BUCKET_SILVER = "silver"
MINIO_BUCKET_GOLD   = "gold"

s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    use_ssl=False,
    verify=False
)

print("=" * 80)
print("🚀 ETL ADZUNA CORRIGÉ - Bronze → Silver → Gold")
print("=" * 80)
print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📦 MinIO: {MINIO_ENDPOINT}")

# ============================================================
# 2. LECTURE BRONZE
# ============================================================
print("\n📖 LECTURE BRONZE depuis MinIO...")

def list_json_files(bucket, prefix):
    objects = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.json'):
                        objects.append(obj['Key'])
    except Exception as e:
        print(f"   ❌ Erreur liste: {e}")
    return objects

def read_json(bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"   ❌ Erreur lecture {key}: {e}")
        return None

json_files = list_json_files(MINIO_BUCKET_BRONZE, "raw/adzuna/")
print(f"   📁 {len(json_files)} fichiers JSON trouvés")

all_offers = []
for file in json_files:
    print(f"   📖 {file}")
    data = read_json(MINIO_BUCKET_BRONZE, file)
    if data:
        if isinstance(data, list):
            all_offers.extend(data)
        elif isinstance(data, dict):
            all_offers.append(data)

df_bronze = pd.DataFrame(all_offers)
print(f"\n   ✅ {len(df_bronze)} offres chargées")
print(f"   📋 Colonnes brutes: {list(df_bronze.columns)}")

# ============================================================
# 3. NETTOYAGE + RENOMMAGE DES COLONNES (SILVER)
# ✅ On aligne sur les colonnes attendues par load_to_postgresql.py
# ============================================================
print("\n🧹 NETTOYAGE ET RENOMMAGE DES COLONNES...")

def clean_ville(lieu):
    """Extrait la ville depuis le champ lieu"""
    if not lieu or lieu == 'France':
        return "Non spécifié"
    ville = lieu.split(',')[0].strip()
    ville = re.sub(r'\d{5}', '', ville).strip()
    return ville if ville else "Non spécifié"

def clean_titre(titre):
    """Nettoie le titre"""
    if not titre:
        return ""
    titre = re.sub(r'\s*\([H/Fh/f]+\)\s*$', '', str(titre))
    return titre.strip()

def normalize_contrat(contrat):
    if not contrat or contrat == 'Non spécifié':
        return "Non spécifié"
    mapping = {
        "CDI": "CDI", "CDD": "CDD",
        "FREELANCE": "Freelance", "STAGE": "Stage",
        "ALTERNANCE": "Alternance", "INTERIM": "Intérim"
    }
    return mapping.get(str(contrat).upper(), contrat)

def compute_salaire_mensuel(row):
    """Calcule le salaire mensuel depuis salaire_min/salaire_max"""
    sal_min = row.get('salaire_min')
    sal_max = row.get('salaire_max')
    if sal_min and sal_min > 0:
        # Si annuel (> 10000) → diviser par 12
        if sal_min > 10000:
            return int(sal_min // 12)
        return int(sal_min)
    if sal_max and sal_max > 0:
        if sal_max > 10000:
            return int(sal_max // 12)
        return int(sal_max)
    return None

df = df_bronze.copy()

# Remplir les valeurs manquantes
df['id']                 = df['id'].fillna('unknown').astype(str)
df['titre']              = df['titre'].fillna('').astype(str).str.strip()
df['entreprise']         = df['entreprise'].fillna('Non spécifié').astype(str).str.strip()
df['lieu']               = df['lieu'].fillna('France').astype(str).str.strip()
df['description']        = df['description'].fillna('').astype(str)
df['description_snippet']= df['description_snippet'].fillna('').astype(str)
df['type_contrat']       = df['type_contrat'].fillna('Non spécifié').astype(str)
df['niveau_experience']  = df['niveau_experience'].fillna('Non spécifié').astype(str)
df['source']             = df['source'].fillna('adzuna').astype(str)
df['query_recherche']    = df['query_recherche'].fillna('').astype(str)
df['url']                = df['url'].fillna('').astype(str)
df['salaire_min']        = pd.to_numeric(df['salaire_min'], errors='coerce')
df['salaire_max']        = pd.to_numeric(df['salaire_max'], errors='coerce')
df['date_publication']   = pd.to_datetime(df['date_publication'], errors='coerce')
df['date_scraping']      = pd.to_datetime(df['date_scraping'], errors='coerce')

# Dédoublonnage
initial = len(df)
df = df.drop_duplicates(subset=['id'])
print(f"   ✅ {len(df)} offres après dédoublonnage ({initial - len(df)} doublons supprimés)")

# ✅ Créer les colonnes attendues par load_to_postgresql.py
df['titre_original']        = df['titre']
df['titre_clean']           = df['titre'].apply(clean_titre)
df['lieu_original']         = df['lieu']
df['ville']                 = df['lieu'].apply(clean_ville)
df['type_contrat_original'] = df['type_contrat']
df['type_contrat']          = df['type_contrat'].apply(normalize_contrat)
df['salaire_mensuel']       = df.apply(compute_salaire_mensuel, axis=1)
df['description_clean']     = df['description'].str[:5000]
df['description_length']    = df['description'].str.len()
df['date_nettoyage']        = datetime.now().isoformat()
df['annee']                 = datetime.now().year
df['mois']                  = datetime.now().month
df['jour']                  = datetime.now().day

# ============================================================
# 4. EXTRACTION DES COMPÉTENCES
# ============================================================
print("\n✨ EXTRACTION DES COMPÉTENCES...")

SKILLS_PATTERNS = {
    "Python":       r'\bpython\b',
    "Java":         r'\bjava\b',
    "Scala":        r'\bscala\b',
    "SQL":          r'\bsql\b',
    "NoSQL":        r'\bnosql\b',
    "Spark":        r'\bspark\b',
    "Hadoop":       r'\bhadoop\b',
    "Kafka":        r'\bkafka\b',
    "Airflow":      r'\bairflow\b',
    "TensorFlow":   r'\btensorflow\b',
    "PyTorch":      r'\bpytorch\b',
    "Scikit-learn": r'\bscikit[-\s]?learn\b',
    "Tableau":      r'\btableau\b',
    "Power BI":     r'\bpower\s*bi\b',
    "AWS":          r'\baws\b',
    "Azure":        r'\bazure\b',
    "GCP":          r'\bgcp\b',
    "Docker":       r'\bdocker\b',
    "Kubernetes":   r'\bkubernetes\b',
    "Git":          r'\bgit\b',
    "PostgreSQL":   r'\bpostgresql\b',
    "MySQL":        r'\bmysql\b',
    "MongoDB":      r'\bmongodb\b',
}

def extract_skills(text):
    if not isinstance(text, str) or not text:
        return []
    text_lower = text.lower()
    return list({skill for skill, pattern in SKILLS_PATTERNS.items()
                 if re.search(pattern, text_lower)})

df['texte_complet'] = df['titre'].fillna('') + " " + df['description'].fillna('')
df['competences']   = df['texte_complet'].apply(extract_skills)
df['nb_competences']= df['competences'].apply(len)
print(f"   ✅ Compétences extraites pour {len(df)} offres")

# ============================================================
# 5. SCORE DE QUALITÉ
# ============================================================
print("\n📊 CALCUL DU SCORE DE QUALITÉ...")

def calculate_score(row):
    score = 0
    desc_len = len(str(row.get('description', '')))
    if desc_len > 500:   score += 2
    elif desc_len > 100: score += 1
    nb = row.get('nb_competences', 0)
    if nb >= 5:   score += 3
    elif nb >= 3: score += 2
    elif nb >= 1: score += 1
    if row.get('type_contrat', 'Non spécifié') != 'Non spécifié': score += 1
    if row.get('salaire_mensuel') is not None:                     score += 1
    return score

df['score_qualite']     = df.apply(calculate_score, axis=1)
df['score_qualite_pct'] = (df['score_qualite'] / 7 * 100).round(1)
print(f"   ⭐ Score moyen: {df['score_qualite'].mean():.1f}/7")

# ============================================================
# 6. TEXTE DE RECHERCHE
# ============================================================
def create_search_text(row):
    parts = [str(row.get('titre','')), str(row.get('description','')),
             str(row.get('entreprise','')), " ".join(row.get('competences',[]))]
    return re.sub(r'[^\w\s]', ' ', " ".join(parts).lower())

df['recherche_text'] = df.apply(create_search_text, axis=1)

# ============================================================
# 7. SAUVEGARDE MINIO
# ✅ Colonnes Gold alignées avec load_to_postgresql.py
# ============================================================
def save_parquet_to_minio(df_save, bucket, key):
    buffer = io.BytesIO()
    df_save.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"   ✅ {bucket}/{key} ({len(df_save)} offres, {len(df_save.columns)} colonnes)")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# --- Silver ---
silver_columns = [
    'id', 'titre_original', 'titre_clean', 'entreprise',
    'lieu_original', 'ville', 'description_clean', 'description_length',
    'type_contrat_original', 'type_contrat', 'salaire_mensuel',
    'date_publication', 'date_scraping', 'date_nettoyage',
    'annee', 'mois', 'jour', 'source', 'query_recherche', 'url',
]
print(f"\n💾 Sauvegarde Silver...")
save_parquet_to_minio(df[silver_columns], MINIO_BUCKET_SILVER,
                      f"adzuna/adzuna_silver_{timestamp}.parquet")

# --- Gold ---
gold_columns = silver_columns + [
    'competences', 'nb_competences',
    'score_qualite', 'score_qualite_pct', 'recherche_text',
]
print(f"💾 Sauvegarde Gold...")
save_parquet_to_minio(df[gold_columns], MINIO_BUCKET_GOLD,
                      f"adzuna/adzuna_gold_{timestamp}.parquet")

# ============================================================
# 8. RÉSUMÉ
# ============================================================
print("\n" + "=" * 80)
print("📊 RÉSUMÉ FINAL")
print("=" * 80)
print(f"📈 Total offres : {len(df)}")
print(f"✅ Avec titre   : {(df['titre_clean'] != '').sum()}")
print(f"📍 Avec ville   : {(df['ville'] != 'Non spécifié').sum()}")
print(f"💰 Avec salaire : {df['salaire_mensuel'].notna().sum()}")

all_skills = [s for skills in df['competences'] for s in skills]
print(f"\n🔧 Top 10 compétences:")
for skill, count in Counter(all_skills).most_common(10):
    print(f"   - {skill}: {count}")

print("\n✅ ETL ADZUNA TERMINÉ AVEC SUCCÈS !")
print("=" * 80)