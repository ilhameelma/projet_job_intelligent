#!/usr/bin/env python3
"""
CHARGEMENT DES DONNÉES GOLD DANS POSTGRESQL
✅ FIX : SQLAlchemy 1.x compatible (conn.execute + trans.commit)
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from minio import Minio
import io
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
MINIO_ENDPOINT_RAW = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_HOST         = MINIO_ENDPOINT_RAW.replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY   = os.getenv("MINIO_SECRET_KEY", "minio_password")
MINIO_SECURE       = MINIO_ENDPOINT_RAW.startswith("https://")

POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "job_intelligent_dwh")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "dwh_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "dwh_password")

BUCKET_GOLD = "gold"

print(f"🔧 Config MinIO  : {MINIO_HOST} (secure={MINIO_SECURE})")
print(f"🔧 Config PG     : {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

MINIO_CLIENT = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)

def get_engine():
    return create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

# ============================================================
# UTILITAIRES
# ============================================================
def clean_date(v):
    return None if (v == "" or v is None) else v

def nan_to_none(value):
    if value is None:
        return None
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value

def clean_row(row_dict):
    return {k: nan_to_none(v) for k, v in row_dict.items()}

# ============================================================
# LECTURE MINIO
# ============================================================
def lire_fichier_gold_du_jour(source):
    prefix    = f"{source}/"
    today_str = datetime.now().strftime("%Y%m%d")
    try:
        objects = list(MINIO_CLIENT.list_objects(BUCKET_GOLD, prefix=prefix, recursive=True))
        parquet_files = [
            o for o in objects
            if o.object_name.endswith('.parquet') and today_str in o.object_name
        ]
        if not parquet_files:
            print(f"   ⚠️  Aucun fichier du jour ({today_str}) pour {source}")
            return None
        latest   = max(parquet_files, key=lambda x: x.last_modified)
        print(f"   📖 Lecture: {latest.object_name}")
        response = MINIO_CLIENT.get_object(BUCKET_GOLD, latest.object_name)
        df       = pd.read_parquet(io.BytesIO(response.read()))
        if 'competences' in df.columns:
            df['competences'] = df['competences'].apply(
                lambda x: x.tolist() if hasattr(x, 'tolist') else (x if x is not None else [])
            )
        return df
    except Exception as e:
        print(f"   ❌ Erreur Minio: {e}")
        return None

# ============================================================
# INSERTIONS  — ✅ SQLAlchemy 1.x : utilise begin() comme context manager
# ============================================================
def vider_tables(engine):
    """✅ FIX principal : begin() gère commit/rollback automatiquement"""
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE gold.offre_competence RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE gold.offres RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE gold.entreprises RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE gold.competences RESTART IDENTITY CASCADE"))
    print("   ✅ Tables vidées")

def upsert_entreprises(df, engine):
    if 'entreprise' not in df.columns:
        print("   ⚠️  Colonne 'entreprise' non trouvée")
        return
    entreprises = (df[['entreprise']].drop_duplicates()
                   .rename(columns={'entreprise': 'nom'}))
    entreprises = entreprises[
        (entreprises['nom'] != 'Non spécifié') & entreprises['nom'].notna()
    ]
    with engine.begin() as conn:
        for _, row in entreprises.iterrows():
            conn.execute(
                text("INSERT INTO gold.entreprises (nom) VALUES (:nom) ON CONFLICT (nom) DO NOTHING"),
                {"nom": row['nom']}
            )
    print(f"   ✅ {len(entreprises)} entreprises insérées")

def upsert_competences(df, engine):
    if 'competences' not in df.columns:
        print("   ⚠️  Colonne 'competences' non trouvée")
        return
    all_skills = []
    for skills in df['competences']:
        if isinstance(skills, (list, tuple)):
            all_skills.extend(skills)
    if not all_skills:
        print("   ⚠️  Aucune compétence trouvée")
        return
    competences = pd.Series(all_skills).drop_duplicates().dropna().reset_index(drop=True)
    with engine.begin() as conn:
        for skill in competences:
            conn.execute(
                text("INSERT INTO gold.competences (nom) VALUES (:nom) ON CONFLICT (nom) DO NOTHING"),
                {"nom": skill}
            )
    print(f"   ✅ {len(competences)} compétences insérées")

def charger_offres(df, engine, source):
    df_entreprise  = pd.read_sql("SELECT id_entreprise, nom FROM gold.entreprises", engine)
    entreprise_map = dict(zip(df_entreprise['nom'], df_entreprise['id_entreprise']))
    df_competence  = pd.read_sql("SELECT id_competence, nom FROM gold.competences", engine)
    competence_map = dict(zip(df_competence['nom'], df_competence['id_competence']))

    df_offres = df.copy()
    df_offres['id_entreprise'] = df_offres['entreprise'].map(entreprise_map)

    colonnes = [
        'id', 'id_entreprise', 'source', 'titre_original', 'titre_clean',
        'ville', 'lieu_original', 'type_contrat', 'type_contrat_original',
        'salaire_mensuel', 'description_clean', 'description_length',
        'score_qualite', 'score_qualite_pct', 'recherche_text',
        'date_publication', 'date_scraping', 'date_nettoyage',
        'annee', 'mois', 'jour', 'url', 'query_recherche'
    ]
    for col in colonnes:
        if col not in df_offres.columns:
            df_offres[col] = None
    df_offres = df_offres[colonnes].rename(columns={'id': 'id_offre'})

    for col in ['date_publication', 'date_scraping', 'date_nettoyage']:
        df_offres[col] = df_offres[col].apply(clean_date)

    df_offres = df_offres.where(pd.notna(df_offres), None)
    for col in ['description_length', 'score_qualite', 'annee', 'mois', 'jour', 'id_entreprise']:
        df_offres[col] = pd.to_numeric(df_offres[col], errors='coerce')
        df_offres[col] = df_offres[col].where(pd.notna(df_offres[col]), None)

    inserted = errors = 0
    with engine.begin() as conn:
        for _, row in df_offres.iterrows():
            rc = clean_row(row.to_dict())
            try:
                conn.execute(text("""
                    INSERT INTO gold.offres (
                        id_offre, id_entreprise, source, titre_original, titre_clean,
                        ville, lieu_original, type_contrat, type_contrat_original,
                        salaire_mensuel, description_clean, description_length,
                        score_qualite, score_qualite_pct, recherche_text,
                        date_publication, date_scraping, date_nettoyage,
                        annee, mois, jour, url, query_recherche
                    ) VALUES (
                        :id_offre, :id_entreprise, :source, :titre_original, :titre_clean,
                        :ville, :lieu_original, :type_contrat, :type_contrat_original,
                        :salaire_mensuel, :description_clean, :description_length,
                        :score_qualite, :score_qualite_pct, :recherche_text,
                        :date_publication, :date_scraping, :date_nettoyage,
                        :annee, :mois, :jour, :url, :query_recherche
                    )
                    ON CONFLICT (id_offre) DO UPDATE SET
                        titre_clean       = EXCLUDED.titre_clean,
                        score_qualite     = EXCLUDED.score_qualite,
                        score_qualite_pct = EXCLUDED.score_qualite_pct,
                        description_clean = EXCLUDED.description_clean,
                        date_nettoyage    = EXCLUDED.date_nettoyage
                """), rc)
                inserted += 1
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"   ⚠️  Ligne ignorée ({rc.get('id_offre')}): {e}")
    print(f"   ✅ {inserted} offres insérées | {errors} ignorées")

    relations = []
    for _, row in df.iterrows():
        id_offre = row.get('id')
        for skill in (row.get('competences') or []):
            if skill in competence_map:
                relations.append({'id_offre': id_offre, 'id_competence': competence_map[skill]})
    if relations:
        with engine.begin() as conn:
            for rel in relations:
                conn.execute(text("""
                    INSERT INTO gold.offre_competence (id_offre, id_competence)
                    VALUES (:id_offre, :id_competence)
                    ON CONFLICT DO NOTHING
                """), rel)
        print(f"   ✅ {len(relations)} relations offre-compétence")

def lister_fichiers_minio():
    print(f"\n📁 Fichiers dans Minio (bucket: {BUCKET_GOLD}):")
    try:
        for obj in MINIO_CLIENT.list_objects(BUCKET_GOLD, recursive=True):
            if obj.object_name.endswith('.parquet'):
                print(f"   - {obj.object_name} ({obj.size} bytes)")
    except Exception as e:
        print(f"   ❌ {e}")

# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("🚀 CHARGEMENT DES DONNÉES GOLD → POSTGRESQL")
    print("=" * 60)
    today_str = datetime.now().strftime("%Y%m%d")
    print(f"📅 Fichiers du jour : {today_str}")

    lister_fichiers_minio()

    print("\n🔌 Connexion PostgreSQL...")
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("   ✅ Connecté")

    print("\n🗑️  Vidage des tables...")
    vider_tables(engine)

    for source, nom in [
        ("adzuna",             "Adzuna"),
        ("france_travail",     "France Travail"),
        ("welcometothejungle", "Welcome to the Jungle"),
    ]:
        print(f"\n📁 Chargement {nom}...")
        df = lire_fichier_gold_du_jour(source)
        if df is None:
            continue
        print(f"   📊 {len(df)} offres")
        upsert_entreprises(df, engine)
        upsert_competences(df, engine)
        charger_offres(df, engine, source)

    print("\n" + "=" * 60)
    print("📊 VÉRIFICATION FINALE")
    print("=" * 60)
    with engine.connect() as conn:
        for table, label in [
            ("gold.offres",           "Total offres"),
            ("gold.entreprises",      "Total entreprises"),
            ("gold.competences",      "Total compétences"),
            ("gold.offre_competence", "Total relations"),
        ]:
            n = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()[0]
            print(f"   {label}: {n}")
        rows = conn.execute(
            text("SELECT source, COUNT(*) as nb FROM gold.offres GROUP BY source ORDER BY nb DESC")
        ).fetchall()
        print("\n📊 Offres par source:")
        for row in rows:
            print(f"   {row[0]}: {row[1]} offres")

    print("\n" + "=" * 60)
    print("✅ CHARGEMENT TERMINÉ AVEC SUCCÈS !")
    print("=" * 60)

if __name__ == "__main__":
    main()