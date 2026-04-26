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
# FONCTIONS UTILITAIRES
# ============================================================
 
def get_engine():
    return create_engine(
        f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
    )
 
def clean_date(date_value):
    """Convertit les chaînes vides en None pour PostgreSQL"""
    if date_value == "" or date_value is None:
        return None
    return date_value
 
def nan_to_none(value):
    """Convertit NaN / NaT / inf en None pour PostgreSQL"""
    if value is None:
        return None
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return None
    try:
        # Capture pandas NaT
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value
 
def clean_row(row_dict):
    """Nettoie toutes les valeurs d'une ligne avant INSERT"""
    return {k: nan_to_none(v) for k, v in row_dict.items()}
 
# ============================================================
# LECTURE MINIO
# ============================================================
 
def lire_fichier_gold_du_jour(source):
    """Lit le fichier Parquet Gold du jour depuis Minio"""
    prefix = f"{source}/"
    today_str = datetime.now().strftime("%Y%m%d")  # ex: "20260424"
 
    try:
        objects = list(MINIO_CLIENT.list_objects(BUCKET_GOLD, prefix=prefix, recursive=True))
 
        # Filtrer fichiers Parquet du jour uniquement
        parquet_files = [
            obj for obj in objects
            if obj.object_name.endswith('.parquet') and today_str in obj.object_name
        ]
 
        if not parquet_files:
            print(f"   ⚠️ Aucun fichier du jour ({today_str}) pour {source} — source ignorée")
            return None
 
        # Prendre le plus récent parmi ceux du jour
        latest = max(parquet_files, key=lambda x: x.last_modified)
        print(f"   📖 Lecture: {latest.object_name}")
 
        response = MINIO_CLIENT.get_object(BUCKET_GOLD, latest.object_name)
        df = pd.read_parquet(io.BytesIO(response.read()))
 
        # Convertir les arrays numpy en listes
        if 'competences' in df.columns:
            df['competences'] = df['competences'].apply(
                lambda x: x.tolist() if hasattr(x, 'tolist') else (x if x is not None else [])
            )
 
        return df
 
    except Exception as e:
        print(f"   ❌ Erreur Minio: {e}")
        return None
 
# ============================================================
# INSERTIONS
# ============================================================
 
def upsert_entreprises(df, engine):
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
    if 'competences' not in df.columns:
        print("   ⚠️ Colonne 'competences' non trouvée")
        return
 
    all_skills = []
    for skills in df['competences']:
        if skills is not None and isinstance(skills, (list, tuple)):
            all_skills.extend(skills)
 
    if not all_skills:
        print("   ⚠️ Aucune compétence trouvée")
        return
 
    competences = pd.Series(all_skills).drop_duplicates().dropna().reset_index(drop=True)
 
    with engine.connect() as conn:
        for skill in competences:
            conn.execute(
                text("INSERT INTO gold.competences (nom) VALUES (:nom) ON CONFLICT (nom) DO NOTHING"),
                {"nom": skill}
            )
        conn.commit()
    print(f"   ✅ {len(competences)} compétences insérées")
 
def charger_offres(df, engine, source):
    # Maps entreprise / competence
    df_entreprise = pd.read_sql("SELECT id_entreprise, nom FROM gold.entreprises", engine)
    entreprise_map = dict(zip(df_entreprise['nom'], df_entreprise['id_entreprise']))
 
    df_competence = pd.read_sql("SELECT id_competence, nom FROM gold.competences", engine)
    competence_map = dict(zip(df_competence['nom'], df_competence['id_competence']))
 
    # Préparer le dataframe
    df_offres = df.copy()
    df_offres['id_entreprise'] = df_offres['entreprise'].map(entreprise_map)
 
    colonnes_attendues = [
        'id', 'id_entreprise', 'source', 'titre_original', 'titre_clean',
        'ville', 'lieu_original', 'type_contrat', 'type_contrat_original',
        'salaire_mensuel', 'description_clean', 'description_length',
        'score_qualite', 'score_qualite_pct', 'recherche_text',
        'date_publication', 'date_scraping', 'date_nettoyage',
        'annee', 'mois', 'jour', 'url', 'query_recherche'
    ]
 
    for col in colonnes_attendues:
        if col not in df_offres.columns:
            df_offres[col] = None
 
    df_offres = df_offres[colonnes_attendues].rename(columns={'id': 'id_offre'})
 
    # Nettoyer les dates
    for col in ['date_publication', 'date_scraping', 'date_nettoyage']:
        if col in df_offres.columns:
            df_offres[col] = df_offres[col].apply(clean_date)
 
    # ✅ FIX PRINCIPAL : convertir TOUS les NaN/NaT en None
    df_offres = df_offres.where(pd.notna(df_offres), None)
 
    # ✅ FIX : forcer les colonnes entières en Int64 nullable (évite integer out of range)
    for col in ['description_length', 'score_qualite', 'annee', 'mois', 'jour', 'id_entreprise']:
        if col in df_offres.columns:
            df_offres[col] = pd.to_numeric(df_offres[col], errors='coerce')
            df_offres[col] = df_offres[col].where(pd.notna(df_offres[col]), None)
 
    # UPSERT ligne par ligne
    try:
        with engine.connect() as conn:
            inserted = 0
            updated = 0
            errors = 0
            for _, row in df_offres.iterrows():
                row_clean = clean_row(row.to_dict())
                try:
                    conn.execute(
                        text("""
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
                        """),
                        row_clean
                    )
                    inserted += 1
                except Exception as row_err:
                    errors += 1
                    if errors <= 3:
                        print(f"   ⚠️ Ligne ignorée ({row_clean.get('id_offre')}): {row_err}")
            conn.commit()
        print(f"   ✅ {inserted} offres insérées/mises à jour | {errors} lignes ignorées")
    except Exception as e:
        print(f"   ❌ Erreur critique chargement offres: {e}")
        return
 
    # Relations offre-compétence
    relations = []
    for _, row in df.iterrows():
        id_offre = row.get('id')
        skills = row.get('competences', [])
        if isinstance(skills, (list, tuple)):
            for skill in skills:
                if skill in competence_map:
                    relations.append({'id_offre': id_offre, 'id_competence': competence_map[skill]})
 
    if relations:
        with engine.connect() as conn:
            for rel in relations:
                conn.execute(
                    text("""
                        INSERT INTO gold.offre_competence (id_offre, id_competence)
                        VALUES (:id_offre, :id_competence)
                        ON CONFLICT DO NOTHING
                    """),
                    rel
                )
            conn.commit()
        print(f"   ✅ {len(relations)} relations offre-compétence chargées")
 
def vider_tables(engine):
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE gold.offre_competence RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE gold.offres RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE gold.entreprises RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE gold.competences RESTART IDENTITY CASCADE"))
        conn.commit()
    print("   ✅ Tables vidées")
 
def lister_fichiers_minio():
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
 
    today_str = datetime.now().strftime("%Y%m%d")
    print(f"📅 Chargement des fichiers du jour : {today_str}")
 
    lister_fichiers_minio()
 
    print("\n🔌 Connexion à PostgreSQL...")
    engine = get_engine()
    print("   ✅ Connecté")
 
    print("\n🗑️  Vidage des tables...")
    vider_tables(engine)
 
    sources = [
        ("adzuna",             "Adzuna"),
        ("france_travail",     "France Travail"),
        ("welcometothejungle", "Welcome to the Jungle"),
    ]
 
    for source, nom in sources:
        print(f"\n📁 Chargement {nom}...")
        df = lire_fichier_gold_du_jour(source)
        if df is None:
            continue
 
        print(f"   📊 {len(df)} offres à charger")
        upsert_entreprises(df, engine)
        upsert_competences(df, engine)
        charger_offres(df, engine, source)
 
    # Vérification finale
    print("\n" + "=" * 60)
    print("📊 VÉRIFICATION FINALE")
    print("=" * 60)
    with engine.connect() as conn:
        for table, label in [
            ("gold.offres",          "Total offres"),
            ("gold.entreprises",     "Total entreprises"),
            ("gold.competences",     "Total compétences"),
            ("gold.offre_competence","Total relations"),
        ]:
            n = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()[0]
            print(f"   {label}: {n}")
 
        print("\n📊 Offres par source:")
        rows = conn.execute(
            text("SELECT source, COUNT(*) as nb FROM gold.offres GROUP BY source ORDER BY nb DESC")
        ).fetchall()
        for row in rows:
            print(f"   {row[0]}: {row[1]} offres")
 
    print("\n" + "=" * 60)
    print("✅ CHARGEMENT TERMINÉ AVEC SUCCÈS !")
    print("=" * 60)
 
if __name__ == "__main__":
    main()