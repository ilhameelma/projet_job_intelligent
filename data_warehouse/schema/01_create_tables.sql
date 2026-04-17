-- ============================================================
-- DATA WAREHOUSE JOB INTELLIGENT - MODÈLE ÉTOILE
-- Schéma GOLD pour les données enrichies
-- ============================================================

-- Créer le schéma gold
CREATE SCHEMA IF NOT EXISTS gold;

-- 1. TABLE ENTREPRISES (Dimension)
CREATE TABLE IF NOT EXISTS gold.entreprises (
    id_entreprise SERIAL PRIMARY KEY,
    nom VARCHAR(200) UNIQUE NOT NULL
);

-- 2. TABLE COMPETENCES (Dimension)
CREATE TABLE IF NOT EXISTS gold.competences (
    id_competence SERIAL PRIMARY KEY,
    nom VARCHAR(100) UNIQUE NOT NULL
);

-- 3. TABLE OFFRES (Table des faits)
CREATE TABLE IF NOT EXISTS gold.offres (
    id_offre VARCHAR(100) PRIMARY KEY,
    id_entreprise INTEGER REFERENCES gold.entreprises(id_entreprise),
    source VARCHAR(50),
    titre_original TEXT,
    titre_clean TEXT,
    ville VARCHAR(100),
    lieu_original TEXT,
    type_contrat VARCHAR(50),
    type_contrat_original VARCHAR(50),
    salaire_mensuel INTEGER,
    description_clean TEXT,
    description_length INTEGER,
    score_qualite INTEGER,
    score_qualite_pct DECIMAL(5,2),
    recherche_text TEXT,
    date_publication TIMESTAMP,
    date_scraping TIMESTAMP,
    date_nettoyage TIMESTAMP,
    annee INTEGER,
    mois INTEGER,
    jour INTEGER,
    url TEXT,
    query_recherche VARCHAR(100)
);

-- 4. TABLE DE LIAISON OFFRE-COMPETENCE
CREATE TABLE IF NOT EXISTS gold.offre_competence (
    id_offre VARCHAR(100) REFERENCES gold.offres(id_offre),
    id_competence INTEGER REFERENCES gold.competences(id_competence),
    PRIMARY KEY (id_offre, id_competence)
);

-- INDEX POUR PERFORMANCES
CREATE INDEX IF NOT EXISTS idx_offres_recherche ON gold.offres USING GIN (to_tsvector('french', recherche_text));
CREATE INDEX IF NOT EXISTS idx_offres_entreprise ON gold.offres(id_entreprise);
CREATE INDEX IF NOT EXISTS idx_offres_date ON gold.offres(date_publication DESC);
CREATE INDEX IF NOT EXISTS idx_offres_contrat ON gold.offres(type_contrat);
CREATE INDEX IF NOT EXISTS idx_offres_ville ON gold.offres(ville);
CREATE INDEX IF NOT EXISTS idx_offres_score ON gold.offres(score_qualite);

-- Afficher les tables créées
SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'gold';