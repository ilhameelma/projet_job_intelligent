-- Créer le schéma client
CREATE SCHEMA IF NOT EXISTS client;

-- Table UTILISATEURS
CREATE TABLE IF NOT EXISTS client.utilisateurs (
    id_utilisateur SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    mot_de_passe VARCHAR(255) NOT NULL,
    nom VARCHAR(100),
    prenom VARCHAR(100),
    telephone VARCHAR(20),
    role VARCHAR(50) DEFAULT 'candidat',
    est_actif BOOLEAN DEFAULT true,
    date_inscription TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    derniere_connexion TIMESTAMP,
    cv_original BYTEA,
    cv_nom_fichier VARCHAR(255),
    cv_type_fichier VARCHAR(50),
    cv_date_upload TIMESTAMP,
    preferences JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table COMPETENCES_UTILISATEUR
CREATE TABLE IF NOT EXISTS client.competences_utilisateur (
    id_competence SERIAL PRIMARY KEY,
    id_utilisateur INTEGER REFERENCES client.utilisateurs(id_utilisateur) ON DELETE CASCADE,
    nom_competence VARCHAR(100),
    niveau VARCHAR(50),
    annee_experience DECIMAL(3,1),
    source_detection VARCHAR(50) DEFAULT 'ia',
    date_ajout TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table ALERTES
CREATE TABLE IF NOT EXISTS client.alertes (
    id_alerte SERIAL PRIMARY KEY,
    id_utilisateur INTEGER REFERENCES client.utilisateurs(id_utilisateur) ON DELETE CASCADE,
    nom_alerte VARCHAR(100),
    type_alerte VARCHAR(50),
    valeur_alerte TEXT,
    est_active BOOLEAN DEFAULT true,
    frequence VARCHAR(50) DEFAULT 'quotidien',
    derniere_envoi TIMESTAMP,
    prochain_envoi TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table HISTORIQUE_RECHERCHES
CREATE TABLE IF NOT EXISTS client.historique_recherches (
    id_recherche SERIAL PRIMARY KEY,
    id_utilisateur INTEGER REFERENCES client.utilisateurs(id_utilisateur) ON DELETE CASCADE,
    mots_cles TEXT,
    filtres JSONB,
    nombre_resultats INTEGER,
    date_recherche TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table RECOMMANDATIONS
CREATE TABLE IF NOT EXISTS client.recommandations (
    id_recommandation SERIAL PRIMARY KEY,
    id_utilisateur INTEGER REFERENCES client.utilisateurs(id_utilisateur) ON DELETE CASCADE,
    id_offre VARCHAR(255),
    source VARCHAR(50),
    score_recommandation DECIMAL(5,2),
    date_recommandation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    est_consulte BOOLEAN DEFAULT false,
    est_sauvegarde BOOLEAN DEFAULT false
);

-- Table SESSIONS
CREATE TABLE IF NOT EXISTS client.sessions (
    id_session SERIAL PRIMARY KEY,
    id_utilisateur INTEGER REFERENCES client.utilisateurs(id_utilisateur) ON DELETE CASCADE,
    token_jwt TEXT,
    ip_adresse INET,
    user_agent TEXT,
    date_connexion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_expiration TIMESTAMP,
    est_active BOOLEAN DEFAULT true
);

-- Index pour performances
CREATE INDEX IF NOT EXISTS idx_client_email ON client.utilisateurs(email);
CREATE INDEX IF NOT EXISTS idx_client_competences ON client.competences_utilisateur(id_utilisateur);
CREATE INDEX IF NOT EXISTS idx_client_alertes ON client.alertes(id_utilisateur, est_active);
CREATE INDEX IF NOT EXISTS idx_client_sessions ON client.sessions(token_jwt, est_active);

-- Vue (optionnelle) : Utilisateurs avec leurs compétences
CREATE OR REPLACE VIEW client.vue_utilisateurs_competences AS
SELECT 
    u.id_utilisateur,
    u.email,
    u.nom,
    u.prenom,
    array_agg(c.nom_competence) as competences
FROM client.utilisateurs u
LEFT JOIN client.competences_utilisateur c ON u.id_utilisateur = c.id_utilisateur
GROUP BY u.id_utilisateur;

-- Vue : Top compétences utilisateurs
CREATE OR REPLACE VIEW client.vue_top_competences AS
SELECT 
    nom_competence,
    COUNT(DISTINCT id_utilisateur) as nb_utilisateurs,
    ROUND(AVG(annee_experience), 1) as experience_moyenne
FROM client.competences_utilisateur
GROUP BY nom_competence
ORDER BY nb_utilisateurs DESC;

-- Fonction pour mise à jour auto de updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger sur utilisateurs
DROP TRIGGER IF EXISTS update_utilisateurs_updated_at ON client.utilisateurs;
CREATE TRIGGER update_utilisateurs_updated_at 
    BEFORE UPDATE ON client.utilisateurs 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();