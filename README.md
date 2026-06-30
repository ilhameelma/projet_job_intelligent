# 🚀 Job Intelligent

> **Plateforme intelligente d'agrégation, centralisation et recommandation d'offres d'emploi Data**  
> Architecture Medallion · Airflow 3 · MinIO · PostgreSQL · FastAPI · NLP · Power BI

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/FastAPI-0.100+-009688?style=for-the-badge&logo=fastapi&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-3.1.7-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/PostgreSQL-15-336791?style=for-the-badge&logo=postgresql&logoColor=white"/>
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
  <img src="https://img.shields.io/badge/MinIO-S3-C72C48?style=for-the-badge&logo=minio&logoColor=white"/>
</p>

---

## 📌 À propos du projet

**Job Intelligent** est une plateforme de data engineering académique dédiée aux métiers de la Data (Data Engineering, Data Science, Data Analysis). Elle automatise la collecte d'offres d'emploi depuis plusieurs sources, les centralise dans un data warehouse structuré, et propose des recommandations personnalisées via NLP.

**Réalisé par :** Ily & Ilhame El Mettichi  
**Encadrant :** Mme Fadwa Bouhafer

### Fonctionnalités clés

- 🕷️ **Scraping multi-sources** automatisé (Adzuna, France Travail, Welcome to the Jungle)
- 🔄 **Pipeline ETL** en architecture Medallion Bronze → Silver → Gold
- 🧠 **Matching CV↔Offre par NLP** avec Sentence Transformers (score harmonique coverage/relevance)
- 🤖 **Assistant conversationnel IA** propulsé par Llama 3.3 via Groq API
- 📊 **Dashboard Power BI** pour l'analyse du marché Data
- 🔐 **API REST sécurisée** (FastAPI + JWT)

---

## 📊 Chiffres clés

| Métrique | Valeur |
|---|---|
| Sources scrappées | 3 (Adzuna, France Travail, WTTJ) |
| Offres collectées | ~2 000 / jour |
| Scripts ETL | 4 |
| Phases du projet | 7 |
| Modèle NLP | `all-MiniLM-L6-v2` (Sentence Transformers) |
| LLM assistant | Llama 3.3 (Groq API) |

---

## 🏗️ Architecture globale

```
┌─────────────────────────────────────────────────────────────────┐
│                         INGESTION                                │
│   Adzuna API    France Travail API    Welcome to the Jungle      │
└────────────┬────────────┬──────────────────┬────────────────────┘
             │            │                  │
             ▼            ▼                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DATA LAKE — MinIO (S3)                        │
│   Bronze (JSON brut)  →  Silver (Parquet)  →  Gold (enrichi)    │
└──────────────────────────────┬──────────────────────────────────┘
                               │  ETL Medallion
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│               DATA WAREHOUSE — PostgreSQL 15                     │
│         schema gold : offres · competences · offre_competence    │
│         schema client : users · cv_uploads · match_history       │
└──────────┬───────────────────────────────────────┬──────────────┘
           │                                       │
           ▼                                       ▼
┌──────────────────────┐               ┌───────────────────────────┐
│   FastAPI Backend    │               │    Dashboard Power BI      │
│  JWT · NLP · Groq   │               │  Marché Data · Salaires    │
└──────────┬───────────┘               └───────────────────────────┘
           │
           ▼
┌──────────────────────┐
│   Frontend HTML/JS   │
│  Search · Match · AI │
└──────────────────────┘
                    ↑ orchestré par
         ┌─────────────────────┐
         │   Apache Airflow 3   │
         │   DAG quotidien      │
         └─────────────────────┘
```

---

## 🛠️ Stack technique

| Couche | Outil | Version |
|---|---|---|
| **Orchestration** | Apache Airflow | 3.1.7 |
| **Data Lake** | MinIO (S3-compatible) | latest |
| **Data Warehouse** | PostgreSQL | 15 |
| **Messaging** | Apache Kafka | 7.5.0 |
| **Streaming** | Apache Flink | 1.18 |
| **Batch / ML** | Apache Spark | 3.5.0 |
| **Cache / Broker** | Redis | 7 |
| **Backend API** | FastAPI + SQLAlchemy | - |
| **Auth** | JWT (PyJWT) | - |
| **NLP Matching** | Sentence Transformers | `all-MiniLM-L6-v2` |
| **LLM Assistant** | Llama 3.3 via Groq API | - |
| **CV Parsing** | pdfplumber · python-docx | - |
| **Frontend** | HTML / CSS / JS | - |
| **Dashboard** | Power BI Desktop | - |
| **Conteneurs** | Docker Compose | - |

---

## 📁 Structure du projet

```
job_intelligent/
├── docker-compose.yml
├── .env.example                        ← Template variables d'env (jamais le .env réel)
├── requirements.txt
├── run_pipeline.ps1                    ← Script one-shot (scraping + ETL)
│
├── airflow_dags/
│   └── scraping_dag.py                 ← DAG scraping quotidien (minuit)
│
├── ingestion/scrapers/
│   ├── indeed_scraper.py               ← Adzuna API
│   ├── france_travail_api.py           ← France Travail API
│   └── jungle_scraper.py               ← Welcome to the Jungle
│
├── scripts/
│   ├── etl_adzuna.py                   ← Bronze → Silver → Gold
│   ├── etl_france_travail.py
│   ├── etl_welcomeJungle.py
│   └── load_to_postgresql.py           ← Gold → PostgreSQL (filtre du jour + UPSERT)
│
├── data_warehouse/
│   └── schema/
│       └── 01_create_tables.sql        ← Schémas gold + client
│
├── api/
│   ├── main.py                         ← FastAPI : auth, recommandation, assistant IA
│   ├── models.py                       ← SQLAlchemy ORM
│   ├── nlp_matcher.py                  ← Sentence Transformers + scoring harmonique
│   ├── cv_parser.py                    ← Extraction de compétences PDF/DOCX
│   ├── ai_assistant.py                 ← Groq API (Llama 3.3)
│   └── Dockerfile
│
├── frontend/
│   ├── index.html                      ← Page d'accueil / recherche
│   ├── match.html                      ← Upload CV + recommandations
│   ├── assistant.html                  ← Chatbot IA
│   └── assets/
│
├── data_lake/
│   └── bronze/                         ← JSON bruts (local dev uniquement)
│
├── spark/ml_jobs/
├── flink/jobs/
└── kafka/connectors/
```

---

## ⚡ Démarrage rapide

### Prérequis

- Docker Desktop (min. **8 Go RAM** alloués)
- Python 3.10+
- PowerShell 5+ (Windows)
- Clés API :
  - Adzuna : [developer.adzuna.com](https://developer.adzuna.com)
  - France Travail : [francetravail.io](https://francetravail.io)
  - Groq (LLM) : [console.groq.com](https://console.groq.com)

### Étape 1 — Variables d'environnement

```bash
cp .env.example .env
# Remplir .env avec vos credentials
```

```env
# Scrapers
ADZUNA_APP_ID=votre_app_id
ADZUNA_APP_KEY=votre_app_key
FRANCE_TRAVAIL_CLIENT_ID=votre_client_id
FRANCE_TRAVAIL_CLIENT_SECRET=votre_secret

# Data Lake
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minio_admin
MINIO_SECRET_KEY=minio_password

# Data Warehouse
POSTGRES_HOST=postgres-dwh
POSTGRES_PORT=5432
POSTGRES_DB=job_intelligent_dwh
POSTGRES_USER=dwh_user
POSTGRES_PASSWORD=dwh_password

# API
SECRET_KEY=votre_jwt_secret_key
GROQ_API_KEY=votre_groq_key
```

> ⚠️ Ne jamais commiter le fichier `.env` — il est dans `.gitignore`

### Étape 2 — Lancer tous les services Docker

```bash
# Première fois : supprimer les anciens volumes si migration
docker volume rm job_inteligent_postgres-db-volume

# Démarrer (attendre ~3 minutes)
docker-compose up -d

# Vérifier l'état
docker-compose ps
```

### Étape 3 — Autoriser PowerShell (Windows)

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Étape 4 — Lancer le pipeline complet

```powershell
.\run_pipeline.ps1
```

Ce script enchaîne automatiquement :
1. Vérification des conteneurs Docker
2. Scraping Adzuna + France Travail (+ WTTJ si activé)
3. ETL Bronze → Silver → Gold → PostgreSQL
4. Vérification des données chargées

**Options disponibles :**

```powershell
.\run_pipeline.ps1                        # Pipeline complet (défaut)
.\run_pipeline.ps1 -Date "2026-04-24"    # Date spécifique
.\run_pipeline.ps1 -SkipETL              # Scraping uniquement
.\run_pipeline.ps1 -ETLOnly              # ETL uniquement
```

### Étape 5 — Connecter Power BI

```
Accueil → Obtenir les données → Base de données PostgreSQL
Serveur  : localhost
Base     : job_intelligent_dwh
Port     : 5432
Tables   : gold.offres | gold.competences | gold.offre_competence
```

---

## 🌐 Interfaces web

| Service | URL | Identifiants |
|---|---|---|
| **Airflow UI** | http://localhost:8081 | `admin / admin` |
| **MinIO Console** | http://localhost:9001 | `minio_admin / minio_password` |
| **API REST** | http://localhost:8000 | JWT requis |
| **API Docs (Swagger)** | http://localhost:8000/docs | — |
| **API Health** | http://localhost:8000/health | — |
| **Kafka UI** | http://localhost:8085 | — |
| **Spark UI** | http://localhost:8080 | — |
| **Flink UI** | http://localhost:8082 | — |

---

## 📋 DAG Airflow — Pipeline de scraping

Le DAG `scraping_dag` s'exécute **chaque jour à minuit** :

```
check_scripts_exist
       │
       ├── scrape_adzuna             (~2 min, ~1 200 offres)
       ├── scrape_france_travail     (~1 min, ~500 offres)
       └── scrape_welcometothejungle (~5 min, ~300 offres)
              │
              └── [toutes terminées] → load_to_postgresql
```

Les données brutes sont stockées dans MinIO :

```
bronze/raw/adzuna/YYYY/MM/DD/offres_*.json
bronze/raw/france_travail/YYYY/MM/DD/offres_*.json
bronze/raw/wttj/YYYY/MM/DD/offres_*.json
```

> **Astuce Windows :** Les scripts `.sh` utilisés par Airflow doivent avoir des fins de ligne **LF** (Unix), pas CRLF. Conversion en batch via PowerShell :
> ```powershell
> Get-ChildItem -Recurse -Filter "*.sh" | ForEach-Object {
>     (Get-Content $_.FullName -Raw) -replace "`r`n", "`n" | Set-Content $_.FullName -NoNewline
> }
> ```

---

## 🔄 Pipeline ETL — Architecture Medallion

```
Bronze (JSON brut)
    ↓  etl_adzuna.py / etl_france_travail.py / etl_welcomeJungle.py
Silver (Parquet nettoyé — déduplication, typage, normalisation)
    ↓
Gold (Parquet enrichi — compétences extraites, score qualité)
    ↓  load_to_postgresql.py
PostgreSQL DWH
    ├── gold.offres
    ├── gold.competences
    └── gold.offre_competence
```

**Comportements clés de `load_to_postgresql.py` :**

- **Filtre du jour** : charge uniquement les fichiers `YYYYMMDD` du jour courant
- **UPSERT** : mise à jour automatique si `id_offre` existe déjà
- **Nettoyage NaN** : conversion de tous les `NaN` / `NaT` en `NULL` PostgreSQL
- Source absente du jour → ignorée sans erreur

---

## 🧠 NLP — Matching CV ↔ Offres

Le module de recommandation compare un CV uploadé aux offres en base via **Sentence Transformers**.

### Algorithme de scoring (score harmonique)

```python
# Extraction des compétences
cv_skills    = extract_skills(cv_text)      # pdfplumber / python-docx + regex word-boundary
offer_skills = extract_skills(offer_text)

# Score de couverture : proportion des compétences de l'offre présentes dans le CV
coverage  = len(cv_skills & offer_skills) / len(offer_skills)

# Score de pertinence : proportion des compétences du CV qui matchent l'offre
relevance = len(cv_skills & offer_skills) / len(cv_skills)

# Score harmonique (robuste aux offres avec peu de compétences)
if len(offer_skills) < 3:
    score = coverage * 0.5      # pénalité offres pauvres en skills
else:
    score = 2 * (coverage * relevance) / (coverage + relevance + 1e-9)
```

> Les compétences courtes et ambiguës (`R`, `AWS`, `Git`) sont détectées par **contexte** (mots-clés environnants) pour éviter les faux positifs.

### Endpoints API

```
POST /auth/register          ← Création de compte
POST /auth/login             ← Login → JWT token
POST /cv/upload              ← Upload CV (PDF ou DOCX)
GET  /recommendations/{uid}  ← Top offres matchées pour un user
POST /assistant/chat         ← Chat avec l'assistant IA
GET  /jobs/search            ← Recherche d'offres avec filtres
GET  /health                 ← Santé de l'API
```

---

## 🤖 Assistant IA

L'assistant conversationnel est propulsé par **Llama 3.3** (Groq API) et répond aux questions sur :
- Le marché de l'emploi Data
- Les offres disponibles en base
- Les compétences à développer selon un profil
- Le contenu d'un CV uploadé

---

## 📈 Bilan d'avancement

| Phase | Description | Statut | % |
|---|---|---|---|
| 01 | Infrastructure Docker | ✅ Terminé | 100% |
| 02 | Ingestion & Scraping (Bronze) | ✅ Terminé | 100% |
| 03 | ETL Medallion Bronze → Gold | ✅ Terminé | 95% |
| 04 | Airflow DAGs (Orchestration) | ✅ Terminé | 95% |
| 05 | Data Warehouse PostgreSQL | 🔄 En cours | 75% |
| 06 | API REST + Recommandation NLP + Assistant IA | 🔄 En cours | 60% |
| 07 | Dashboard Power BI | ⏳ À faire | 0% |

---

## 🔧 Commandes utiles

### Docker

```bash
docker-compose up -d                        # Démarrer tous les services
docker-compose down                         # Arrêter
docker-compose down --remove-orphans        # Arrêter + supprimer orphelins
docker-compose ps                           # État des services
docker-compose logs -f airflow-scheduler    # Logs en direct
docker-compose restart airflow-scheduler    # Redémarrer un service
```

### Airflow

```bash
# Déclencher le DAG manuellement
docker exec airflow-scheduler airflow dags trigger scraping_dag

# Tester une tâche individuellement
docker exec airflow-worker airflow tasks test scraping_dag scrape_adzuna 2026-04-24

# Lister les DAGs
docker exec airflow-scheduler airflow dags list
```

### MinIO — Vérifier les données

```bash
docker exec minio-init mc ls local/bronze/raw/ --recursive
docker exec minio-init mc ls local/silver/ --recursive
docker exec minio-init mc ls local/gold/ --recursive
```

### PostgreSQL — Vérifier les données

```bash
docker exec -it postgres-dwh psql -U dwh_user -d job_intelligent_dwh
```

```sql
-- Nombre total d'offres
SELECT COUNT(*) FROM gold.offres;

-- Offres par source
SELECT source, COUNT(*) FROM gold.offres GROUP BY source;

-- Top 10 compétences demandées
SELECT c.nom, COUNT(*) AS nb
FROM gold.competences c
JOIN gold.offre_competence oc ON c.id_competence = oc.id_competence
GROUP BY c.nom ORDER BY nb DESC LIMIT 10;
```

---

## 🐛 Dépannage

| Problème | Solution |
|---|---|
| **Airflow UI inaccessible** | Attendre 3 min. Vérifier : `docker logs airflow-webserver` |
| **DAG ne se déclenche pas** | Vérifier que le DAG est activé (toggle ON dans l'UI) |
| **Tâche failed sans logs** | `docker logs airflow-scheduler --tail=50` |
| **MinIO inaccessible** | `docker-compose restart minio` |
| **Kafka en erreur** | Attendre 60s. Si persistant : `docker-compose restart kafka` |
| **ETL échoue (connexion MinIO)** | Vérifier `MINIO_ENDPOINT=http://minio:9000` (pas localhost) |
| **PostgreSQL SSL error** | Ajouter `sslmode=disable` dans la chaîne de connexion |
| **integer out of range** | Vérifier `load_to_postgresql.py` — nettoyage NaN activé |
| **duplicate key violation** | Vérifier `load_to_postgresql.py` — UPSERT activé |
| **Scraper échoue (CRLF)** | Convertir les `.sh` en LF (voir section DAG Airflow ci-dessus) |
| **404 dans l'UI Airflow** | Normal avec `tasks test` — bug Airflow 3, l'exécution réelle fonctionne |
| **minio-init s'arrête** | Normal — conteneur one-shot qui s'arrête après création des buckets |
| **Manque de RAM** | Désactiver `spark-ml-trainer` et `flink-streaming-job` dans docker-compose |

---

## ⚠️ Points importants

- Dans Docker, les services communiquent par **nom de service** (pas `localhost`) :
  - MinIO → `http://minio:9000`
  - Kafka → `kafka:29092`
  - PostgreSQL → `postgres-dwh:5432`
- Les scripts ETL lancés **hors Docker** utilisent `localhost`
- Ne **jamais commiter** `.env` — utilisez `.env.example` comme template
- Airflow utilise **CeleryExecutor** avec Redis comme broker de messages
- `postgres-db-volume` (Airflow metadata) ≠ `postgres-dwh-data` (Data Warehouse)

---

## 🗺️ Prochaines étapes

1. **Finaliser le Data Warehouse** — contraintes et index sur `gold.offres`
2. **DAG ETL automatisé** — `02_etl_dag.py` (Bronze → Silver → Gold → PostgreSQL)
3. **Activer WTTJ** — décommenter dans `run_pipeline.ps1` quand le scraper est stable
4. **Dashboard Power BI** — connexion aux tables `gold.*`, vues marché & salaires
5. **Améliorer l'API NLP** — fine-tuning du scoring, prise en charge des soft skills

---
👥 Auteur
Samira EL YAAGOUBI
Co-auteur : Ilhame EL METTICHI

## 📄 Licence

Projet académique — tous droits réservés.  
Réalisé dans le cadre d'un projet de fin d'études en Data Engineering.

---

*Job Intelligent · README v4.0 · Airflow 3.1.7 · Architecture Medallion Bronze/Silver/Gold*
