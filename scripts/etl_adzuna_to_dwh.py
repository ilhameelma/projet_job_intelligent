#!/usr/bin/env python3
"""
ETL PANDAS - ADZUNA (Version simplifiée)
Bronze (Minio) → Silver (Minio) → Gold (Minio)

Lecture des JSON depuis Minio (bronze/raw/adzuna/)
Sauvegarde en Parquet dans Minio (silver/ et gold/)
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
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
MINIO_BUCKET_BRONZE = "bronze"
MINIO_BUCKET_SILVER = "silver"
MINIO_BUCKET_GOLD = "gold"

# Configuration du client S3 pour Minio
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    use_ssl=False,
    verify=False
)

print("=" * 80)
print("🚀 ETL PANDAS ADZUNA - Bronze (Minio) → Silver (Minio) → Gold (Minio)")
print("=" * 80)
print(f"📅 Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📦 Minio endpoint: {MINIO_ENDPOINT}")

# ============================================================
# 2. LECTURE DES FICHIERS JSON DEPUIS MINIO (BRONZE)
# ============================================================
print("\n📖 ÉTAPE 1: LECTURE DES DONNÉES BRUTES (BRONZE) DEPUIS MINIO")

def list_json_files_from_minio(bucket, prefix):
    """Liste tous les fichiers JSON dans un bucket Minio"""
    objects = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.json'):
                        objects.append(obj['Key'])
    except Exception as e:
        print(f"   ❌ Erreur liste fichiers: {e}")
    return objects

def read_json_from_minio(bucket, key):
    """Lit un fichier JSON depuis Minio"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        print(f"   ❌ Erreur lecture {key}: {e}")
        return None

# Lister tous les fichiers JSON Adzuna dans bronze
print("   📁 Recherche des fichiers JSON Adzuna dans Minio...")
json_files = list_json_files_from_minio(MINIO_BUCKET_BRONZE, "raw/adzuna/")
print(f"   📁 {len(json_files)} fichiers JSON trouvés")

# Lire tous les fichiers
all_offers = []
for file in json_files:
    print(f"   📖 Lecture: {file}")
    data = read_json_from_minio(MINIO_BUCKET_BRONZE, file)
    if data:
        if isinstance(data, list):
            all_offers.extend(data)
        elif isinstance(data, dict):
            all_offers.append(data)

df_bronze = pd.DataFrame(all_offers)
print(f"\n   ✅ TOTAL: {len(df_bronze)} offres chargées")

# Aperçu
print("\n📋 Aperçu des données brutes:")
print(df_bronze[['id', 'titre', 'entreprise', 'lieu']].head(5))

# ============================================================
# 3. NETTOYAGE GÉNÉRAL (SILVER)
# ============================================================
print("\n🧹 ÉTAPE 2: NETTOYAGE GÉNÉRAL (SILVER)")

def clean_dataframe(df):
    """Nettoie le DataFrame"""
    df_clean = df.copy()
    
    # Remplacer les valeurs NULL
    df_clean['id'] = df_clean['id'].fillna('unknown')
    df_clean['titre'] = df_clean['titre'].fillna('').astype(str).str.strip()
    df_clean['entreprise'] = df_clean['entreprise'].fillna('Non spécifié').astype(str).str.strip()
    df_clean['lieu'] = df_clean['lieu'].fillna('France').astype(str).str.strip()
    df_clean['description'] = df_clean['description'].fillna('').astype(str)
    df_clean['description_snippet'] = df_clean['description_snippet'].fillna('').astype(str)
    df_clean['type_contrat'] = df_clean['type_contrat'].fillna('Non spécifié').astype(str)
    df_clean['niveau_experience'] = df_clean['niveau_experience'].fillna('Non spécifié').astype(str)
    df_clean['devise_salaire'] = df_clean['devise_salaire'].fillna('EUR').astype(str)
    df_clean['statut'] = df_clean['statut'].fillna('active').astype(str)
    df_clean['source'] = df_clean['source'].fillna('adzuna').astype(str)
    df_clean['query_recherche'] = df_clean['query_recherche'].fillna('').astype(str)
    
    # Convertir les salaires en numérique
    df_clean['salaire_min'] = pd.to_numeric(df_clean['salaire_min'], errors='coerce')
    df_clean['salaire_max'] = pd.to_numeric(df_clean['salaire_max'], errors='coerce')
    
    # Convertir les dates
    df_clean['date_publication'] = pd.to_datetime(df_clean['date_publication'], errors='coerce')
    df_clean['date_scraping'] = pd.to_datetime(df_clean['date_scraping'], errors='coerce')
    
    # Ajouter date d'ingestion
    df_clean['date_ingestion'] = datetime.now().isoformat()
    
    # Supprimer les doublons par ID
    initial_count = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=['id'])
    final_count = len(df_clean)
    
    print(f"   ✅ {final_count} offres après nettoyage")
    print(f"   🗑️ {initial_count - final_count} doublons supprimés")
    
    return df_clean

df_silver = clean_dataframe(df_bronze)

# ============================================================
# 4. EXTRACTION DES COMPÉTENCES (GOLD)
# ============================================================
print("\n✨ ÉTAPE 3: EXTRACTION DES COMPÉTENCES")

# Liste des compétences (sans "R")
SKILLS_PATTERNS = {
    "Python": r'\bPython\b',
    "Java": r'\bJava\b',
    "Scala": r'\bScala\b',
    "SQL": r'\bSQL\b',
    "NoSQL": r'\bNoSQL\b',
    "Spark": r'\bSpark\b',
    "Hadoop": r'\bHadoop\b',
    "Kafka": r'\bKafka\b',
    "Airflow": r'\bAirflow\b',
    "TensorFlow": r'\bTensorFlow\b',
    "PyTorch": r'\bPyTorch\b',
    "Scikit-learn": r'\bScikit[-\s]?learn\b',
    "Tableau": r'\bTableau\b',
    "Power BI": r'\bPower\s+BI\b',
    "AWS": r'\bAWS\b',
    "Azure": r'\bAzure\b',
    "GCP": r'\bGCP\b',
    "Docker": r'\bDocker\b',
    "Kubernetes": r'\bKubernetes\b',
    "Git": r'\bGit\b(?!\s+(?:clone|commit|push|pull))',
    "PostgreSQL": r'\bPostgreSQL\b',
    "MySQL": r'\bMySQL\b',
    "MongoDB": r'\bMongoDB\b',
}

def extract_skills(text):
    """Extrait les compétences d'un texte"""
    if not isinstance(text, str) or not text:
        return []
    found = []
    for skill, pattern in SKILLS_PATTERNS.items():
        if re.search(pattern, text, re.IGNORECASE):
            found.append(skill)
    return list(set(found))

# Appliquer l'extraction sur titre + description
df_gold = df_silver.copy()
df_gold['texte_complet'] = df_gold['titre'].fillna('') + " " + df_gold['description'].fillna('')
df_gold['competences'] = df_gold['texte_complet'].apply(extract_skills)
df_gold['nb_competences'] = df_gold['competences'].apply(len)

print(f"   ✅ Compétences extraites pour {len(df_gold)} offres")

# ============================================================
# 5. CALCUL DU SCORE DE QUALITÉ
# ============================================================
print("\n📊 ÉTAPE 4: CALCUL DU SCORE DE QUALITÉ")

def calculate_score(row):
    """Calcule le score de qualité d'une offre"""
    score = 0
    
    # Description complète (max 2 points)
    desc_len = len(row.get('description', ''))
    if desc_len > 500:
        score += 2
    elif desc_len > 100:
        score += 1
    
    # Compétences trouvées (max 3 points)
    nb_comp = row.get('nb_competences', 0)
    if nb_comp >= 5:
        score += 3
    elif nb_comp >= 3:
        score += 2
    elif nb_comp >= 1:
        score += 1
    
    # Type de contrat spécifié (1 point)
    if row.get('type_contrat', 'Non spécifié') != 'Non spécifié':
        score += 1
    
    # Niveau d'expérience spécifié (1 point)
    if row.get('niveau_experience', 'Non spécifié') != 'Non spécifié':
        score += 1
    
    return score

df_gold['score_qualite'] = df_gold.apply(calculate_score, axis=1)
df_gold['score_qualite_pct'] = (df_gold['score_qualite'] / 7 * 100).round(1)

print(f"\n📊 Distribution des scores de qualité:")
print(df_gold['score_qualite'].value_counts().sort_index())
print(f"\n⭐ Score moyen: {df_gold['score_qualite'].mean():.1f}/7")

# ============================================================
# 6. CRÉATION DE LA COLONNE DE RECHERCHE
# ============================================================
print("\n🔍 ÉTAPE 5: CRÉATION DE LA COLONNE DE RECHERCHE")

def create_search_text(row):
    """Crée le texte de recherche"""
    parts = [
        str(row.get('titre', '')),
        str(row.get('description', '')),
        str(row.get('entreprise', '')),
        " ".join(row.get('competences', []))
    ]
    text = " ".join(parts).lower()
    # Supprimer les caractères spéciaux
    text = re.sub(r'[^\w\s]', ' ', text)
    return text

df_gold['recherche_text'] = df_gold.apply(create_search_text, axis=1)
print("✅ Colonne de recherche textuelle créée")

# ============================================================
# 7. SAUVEGARDE DANS MINIO (SILVER)
# ============================================================
print("\n💾 ÉTAPE 6: SAUVEGARDE DES DONNÉES SILVER DANS MINIO")

def save_parquet_to_minio(df, bucket, key):
    """Sauvegarde un DataFrame pandas en Parquet dans Minio"""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue()
    )
    print(f"   ✅ {bucket}/{key} ({len(df)} offres, {len(df.columns)} colonnes)")

# Sélectionner les colonnes pour Silver
silver_columns = [
    'id', 'titre', 'entreprise', 'lieu', 'description',
    'description_snippet', 'type_contrat', 'niveau_experience',
    'salaire_min', 'salaire_max', 'devise_salaire',
    'date_publication', 'date_scraping', 'date_ingestion',
    'source', 'statut', 'query_recherche'
]

df_silver_output = df_silver[silver_columns]

# Sauvegarde en Parquet dans Minio (Silver)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
silver_key = f"adzuna/offres_{timestamp}.parquet"
save_parquet_to_minio(df_silver_output, MINIO_BUCKET_SILVER, silver_key)

# ============================================================
# 8. SAUVEGARDE DANS MINIO (GOLD)
# ============================================================
print("\n💾 ÉTAPE 7: SAUVEGARDE DES DONNÉES GOLD DANS MINIO")

# Sélectionner les colonnes pour Gold
gold_columns = [
    'id', 'titre', 'entreprise', 'lieu', 'description',
    'description_snippet', 'type_contrat', 'niveau_experience',
    'salaire_min', 'salaire_max', 'devise_salaire',
    'competences', 'nb_competences',
    'score_qualite', 'score_qualite_pct',
    'recherche_text',
    'date_publication', 'date_scraping', 'date_ingestion',
    'source', 'statut', 'query_recherche'
]

df_gold_output = df_gold[gold_columns]

# Sauvegarde en Parquet dans Minio (Gold)
gold_key = f"adzuna/offres_enriched_{timestamp}.parquet"
save_parquet_to_minio(df_gold_output, MINIO_BUCKET_GOLD, gold_key)

# ============================================================
# 9. STATISTIQUES FINALES
# ============================================================
print("\n" + "=" * 80)
print("📊 RÉSUMÉ FINAL")
print("=" * 80)

print(f"\n📈 VOLUME: {len(df_gold_output)} offres")

print(f"\n🔧 TOP 10 COMPÉTENCES:")
all_skills = []
for skills in df_gold['competences']:
    all_skills.extend(skills)
skill_counts = Counter(all_skills)
for skill, count in skill_counts.most_common(10):
    print(f"   - {skill}: {count}")

print(f"\n📄 TYPES DE CONTRAT:")
print(df_gold['type_contrat'].value_counts().head(5))

print(f"\n📍 TOP 10 LOCALISATIONS:")
print(df_gold['lieu'].value_counts().head(10))

print(f"\n⭐ SCORE DE QUALITÉ - Distribution:")
print(df_gold['score_qualite'].value_counts().sort_index())

print(f"\n📁 STOCKAGE DANS MINIO:")
print(f"   Silver: s3a://{MINIO_BUCKET_SILVER}/{silver_key}")
print(f"   Gold:   s3a://{MINIO_BUCKET_GOLD}/{gold_key}")

print("\n" + "=" * 80)
print("✅ ETL PANDAS AVEC MINIO TERMINÉ AVEC SUCCÈS !")
print("=" * 80)