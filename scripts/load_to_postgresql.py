#!/usr/bin/env python3
"""
CHARGEMENT DES DONNÉES GOLD DANS POSTGRESQL
Lit les fichiers Parquet depuis MINIO (gold bucket) et les insère dans les tables
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from minio import Minio
import io
from datetime import datetime

# ============================================================
# CONFIGURATION MINIO
# ============================================================
MINIO_CLIENT = Minio(
    "localhost:9000",
    access_key="minio_admin",
    secret_key="minio_password",
    secure=False
)
BUCKET_GOLD = "gold"

# ============================================================
# CONFIGURATION POSTGRESQL
# ============================================================
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "job_intelligent_dwh"
POSTGRES_USER = "dwh_user"
POSTGRES_PASSWORD = "dwh_password"

# ============================================================
# FONCTIONS
# ============================================================

def get_engine():
    """Crée la connexion PostgreSQL"""
    return create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')

def clean_date(date_value):
    """Convertit les chaînes vides en None pour PostgreSQL"""
    if date_value == "" or date_value is None:
        return None
    return date_value

def lire_dernier_fichier_gold_minio(source):
    """Lit le dernier fichier Parquet Gold depuis Minio"""
    prefix = f"{source}/"
    try:
        objects = list(MINIO_CLIENT.list_objects(BUCKET_GOLD, prefix=prefix, recursive=True))
        
        # Filtrer les fichiers Parquet
        parquet_files = [obj for obj in objects if obj.object_name.endswith('.parquet')]
        
        if not parquet_files:
            print(f"   ❌ Aucun fichier trouvé pour {source} dans Minio")
            return None
        
        # Prendre le plus récent
        latest = max(parquet_files, key=lambda x: x.last_modified)
        print(f"   📖 Lecture: {latest.object_name}")
        
        # Lire le fichier
        response = MINIO_CLIENT.get_object(BUCKET_GOLD, latest.object_name)
        df = pd.read_parquet(io.BytesIO(response.read()))
        
        # Convertir les colonnes problématiques
        if 'competences' in df.columns:
            # Convertir les arrays numpy en listes
            df['competences'] = df['competences'].apply(
                lambda x: x.tolist() if hasattr(x, 'tolist') else (x if x is not None else [])
            )
        
        return df
        
    except Exception as e:
        print(f"   ❌ Erreur Minio: {e}")
        return None

def upsert_entreprises(df, engine):
    """Insert ou met à jour les entreprises"""
    if 'entreprise' not in df.columns:
        print("   ⚠️ Colonne 'entreprise' non trouvée")
        return
    
    entreprises = df[['entreprise']].drop_duplicates().rename(columns={'entreprise': 'nom'})
    entreprises = entreprises[entreprises['nom'] != 'Non spécifié']
    entreprises = entreprises[entreprises['nom'].notna()]
    
    with engine.connect() as conn:
        for _, row in entreprises.iterrows():
            conn.execute(
                text("INSERT INTO gold.entreprises (nom) VALUES (:nom) ON CONFLICT (nom) DO NOTHING"),
                {"nom": row['nom']}
            )
        conn.commit()
    print(f"   ✅ {len(entreprises)} entreprises insérées")

def upsert_competences(df, engine):
    """Insert ou met à jour les compétences"""
    if 'competences' not in df.columns:
        print("   ⚠️ Colonne 'competences' non trouvée")
        return
    
    all_skills = []
    for skills in df['competences']:
        if skills is not None:
            if isinstance(skills, (list, tuple)) and len(skills) > 0:
                all_skills.extend(skills)
    
    if not all_skills:
        print("   ⚠️ Aucune compétence trouvée")
        return
    
    competences = pd.Series(all_skills).drop_duplicates().reset_index(drop=True)
    competences = competences[competences.notna()]
    
    with engine.connect() as conn:
        for skill in competences:
            conn.execute(
                text("INSERT INTO gold.competences (nom) VALUES (:nom) ON CONFLICT (nom) DO NOTHING"),
                {"nom": skill}
            )
        conn.commit()
    print(f"   ✅ {len(competences)} compétences insérées")

def charger_offres(df, engine, source):
    """Charge les offres dans la table gold.offres"""
    # Récupérer les IDs des entreprises
    df_entreprise = pd.read_sql("SELECT id_entreprise, nom FROM gold.entreprises", engine)
    entreprise_map = dict(zip(df_entreprise['nom'], df_entreprise['id_entreprise']))
    
    # Récupérer les IDs des compétences
    df_competence = pd.read_sql("SELECT id_competence, nom FROM gold.competences", engine)
    competence_map = dict(zip(df_competence['nom'], df_competence['id_competence']))
    
    # Préparer les offres
    df_offres = df.copy()
    df_offres['id_entreprise'] = df_offres['entreprise'].map(entreprise_map)
    
    # S'assurer que les colonnes existent
    colonnes_attendues = [
        'id', 'id_entreprise', 'source', 'titre_original', 'titre_clean',
        'ville', 'lieu_original', 'type_contrat', 'type_contrat_original',
        'salaire_mensuel', 'description_clean', 'description_length',
        'score_qualite', 'score_qualite_pct', 'recherche_text',
        'date_publication', 'date_scraping', 'date_nettoyage',
        'annee', 'mois', 'jour', 'url', 'query_recherche'
    ]
    
    # Ajouter les colonnes manquantes
    for col in colonnes_attendues:
        if col not in df_offres.columns:
            df_offres[col] = None
    
    df_offres = df_offres[colonnes_attendues]
    df_offres = df_offres.rename(columns={'id': 'id_offre'})
    
    # 🔧 CORRECTION : Nettoyer les dates
    for col in ['date_publication', 'date_scraping', 'date_nettoyage']:
        if col in df_offres.columns:
            df_offres[col] = df_offres[col].apply(clean_date)
    
    # Remplacer les NaN par None
    df_offres = df_offres.where(pd.notna(df_offres), None)
    
    # Charger
    try:
        df_offres.to_sql('offres', engine, schema='gold', if_exists='append', index=False)
        print(f"   ✅ {len(df_offres)} offres chargées")
    except Exception as e:
        print(f"   ❌ Erreur chargement offres: {e}")
        return
    
    # Charger les relations offre-compétence
    relations = []
    for _, row in df.iterrows():
        id_offre = row['id']
        if 'competences' in df.columns and row['competences']:
            skills = row['competences']
            if isinstance(skills, (list, tuple)):
                for skill in skills:
                    if skill in competence_map:
                        relations.append({
                            'id_offre': id_offre,
                            'id_competence': competence_map[skill]
                        })
    
    if relations:
        df_relations = pd.DataFrame(relations)
        df_relations.to_sql('offre_competence', engine, schema='gold', if_exists='append', index=False)
        print(f"   ✅ {len(relations)} relations offre-compétence chargées")

def vider_tables(engine):
    """Vide les tables avant chargement (optionnel)"""
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE gold.offre_competence RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE gold.offres RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE gold.entreprises RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE gold.competences RESTART IDENTITY CASCADE"))
        conn.commit()
    print("   ✅ Tables vidées")

def lister_fichiers_minio():
    """Liste tous les fichiers disponibles dans le bucket gold"""
    print("\n📁 Fichiers disponibles dans Minio (bucket: gold):")
    try:
        objects = MINIO_CLIENT.list_objects(BUCKET_GOLD, recursive=True)
        for obj in objects:
            if obj.object_name.endswith('.parquet'):
                print(f"   - {obj.object_name} ({obj.size} bytes, modifié: {obj.last_modified})")
    except Exception as e:
        print(f"   ❌ Erreur: {e}")

# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 60)
    print("🚀 CHARGEMENT DES DONNÉES GOLD (MINIO) DANS POSTGRESQL")
    print("=" * 60)
    
    # Lister les fichiers disponibles
    lister_fichiers_minio()
    
    # Connexion
    print("\n🔌 Connexion à PostgreSQL...")
    engine = get_engine()
    print("   ✅ Connecté")
    
    # Vider les tables (optionnel - commenter si vous voulez conserver les données)
    print("\n🗑️  Vidage des tables...")
    vider_tables(engine)
    
    # Sources à charger (correspondent aux dossiers dans Minio)
    sources = [
        ("adzuna", "Adzuna"),
        ("france_travail", "France Travail"),
        ("welcometothejungle", "Welcome to the Jungle")
    ]
    
    for source, nom in sources:
        print(f"\n📁 Chargement {nom}...")
        df = lire_dernier_fichier_gold_minio(source)
        if df is None:
            print(f"   ⚠️ Pas de données pour {nom}")
            continue
        
        print(f"   📊 {len(df)} offres à charger")
        
        # Insérer entreprises
        upsert_entreprises(df, engine)
        
        # Insérer compétences
        upsert_competences(df, engine)
        
        # Insérer offres et relations
        charger_offres(df, engine, source)
    
    # Vérification finale
    print("\n" + "=" * 60)
    print("📊 VÉRIFICATION FINALE")
    print("=" * 60)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM gold.offres"))
        print(f"   Total offres: {result.fetchone()[0]}")
        
        result = conn.execute(text("SELECT COUNT(*) FROM gold.entreprises"))
        print(f"   Total entreprises: {result.fetchone()[0]}")
        
        result = conn.execute(text("SELECT COUNT(*) FROM gold.competences"))
        print(f"   Total compétences: {result.fetchone()[0]}")
        
        result = conn.execute(text("SELECT COUNT(*) FROM gold.offre_competence"))
        print(f"   Total relations: {result.fetchone()[0]}")
    
    print("\n" + "=" * 60)
    print("✅ CHARGEMENT TERMINÉ AVEC SUCCÈS !")
    print("=" * 60)

if __name__ == "__main__":
    main()