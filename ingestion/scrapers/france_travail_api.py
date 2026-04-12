#!/usr/bin/env python3
"""
SCRAPER FRANCE-TRAVAIL v3 - API officielle gratuite
Inscription : https://francetravail.io/data/api
Architecture : Bronze (raw) → Silver (clean) → Gold (enrichi)
Stockage     : MinIO (bronze/raw/france_travail/YYYY/MM/DD/) + Kafka + JSON local
"""

import json
import logging
import time
import os
import re
import io
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from collections import Counter
from dotenv import load_dotenv

import requests

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FranceTravailScraper:

    # ✅ URL correcte avec realm dans l'URL (corrige l'erreur 403)
    TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire"
    API_URL   = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"

    # ── Topics Kafka conformes à l'architecture Medallion ──
    KAFKA_TOPIC_BRONZE = "bronze.france_travail.offres"
    KAFKA_TOPIC_SILVER = "silver.france_travail.offres"

    # ── Buckets MinIO conformes ──
    MINIO_BUCKET_BRONZE = "bronze"
    MINIO_BUCKET_SILVER = "silver"

    # ── Pagination API ──
    PAGE_SIZE    = 50
    MAX_PER_CALL = 149
    RETRY_DELAYS = [2, 5, 10]

    SKILLS = {
        "Python":        r"\bPython\b",
        "Java":          r"\bJava\b",
        "Scala":         r"\bScala\b",
        "R":             r"\bR\b",
        "SQL":           r"\bSQL\b",
        "NoSQL":         r"\bNoSQL\b",
        "Spark":         r"\bSpark\b",
        "Hadoop":        r"\bHadoop\b",
        "Kafka":         r"\bKafka\b",
        "Flink":         r"\bFlink\b",
        "Airflow":       r"\bAirflow\b",
        "TensorFlow":    r"\bTensorFlow\b",
        "PyTorch":       r"\bPyTorch\b",
        "Scikit-learn":  r"\bScikit[-\s]?learn\b",
        "XGBoost":       r"\bXGBoost\b",
        "LightGBM":      r"\bLightGBM\b",
        "Tableau":       r"\bTableau\b",
        "Power BI":      r"\bPower\s*BI\b",
        "Looker":        r"\bLooker\b",
        "Qlik":          r"\bQlik\b",
        "Superset":      r"\bSuperset\b",
        "AWS":           r"\bAWS\b",
        "Azure":         r"\bAzure\b",
        "GCP":           r"\bGCP\b",
        "Databricks":    r"\bDatabricks\b",
        "Snowflake":     r"\bSnowflake\b",
        "BigQuery":      r"\bBigQuery\b",
        "Docker":        r"\bDocker\b",
        "Kubernetes":    r"\bKubernetes\b",
        "Git":           r"\bGit\b",
        "Terraform":     r"\bTerraform\b",
        "PostgreSQL":    r"\bPostgreSQL\b",
        "MySQL":         r"\bMySQL\b",
        "MongoDB":       r"\bMongoDB\b",
        "Elasticsearch": r"\bElasticsearch\b",
        "dbt":           r"\bdbt\b",
        "MLflow":        r"\bMLflow\b",
        "FastAPI":       r"\bFastAPI\b",
    }

    def __init__(self, use_kafka: bool = True):
        self.client_id     = os.getenv("FRANCE_TRAVAIL_CLIENT_ID")
        self.client_secret = os.getenv("FRANCE_TRAVAIL_CLIENT_SECRET")
        self.use_kafka     = use_kafka
        self.producer      = None
        self.token         = None
        self.token_expiry  = 0

        self.stats = {
            "total_appels_api":   0,
            "total_brut":         0,
            "total_doublons":     0,
            "total_erreurs":      0,
            "offres_par_keyword": {},
        }

        self.data_jobs_keywords = [
            "Data Scientist",
            "Data Engineer",
            "Data Analyst",
            "Data Architect",
            "Machine Learning Engineer",
            "Business Intelligence",
            "Data Consultant",
            "Analytics Engineer",
            "MLOps",
            "Data Product Manager",
        ]

        if use_kafka:
            self._init_kafka_producer()

    # ══════════════════════════════════════════
    # KAFKA
    # ══════════════════════════════════════════

    def _init_kafka_producer(self):
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                compression_type="gzip",
            )
            logger.info("✅ Kafka connecté")
        except Exception as e:
            logger.warning(f"⚠️ Kafka non disponible: {e}")
            self.use_kafka = False

    # ══════════════════════════════════════════
    # TOKEN — ✅ Corrige l'erreur 403
    # ══════════════════════════════════════════

    def _obtenir_token(self) -> Optional[str]:
        """Token OAuth2 — realm dans l'URL pour éviter l'erreur 403"""
        if self.token and time.time() < self.token_expiry - 60:
            return self.token

        for attempt in range(3):
            try:
                logger.info("🔐 Obtention du token France-Travail...")
                response = requests.post(
                    self.TOKEN_URL,
                    data={
                        "grant_type":    "client_credentials",
                        "client_id":     self.client_id,
                        "client_secret": self.client_secret,
                        "scope":         "api_offresdemploiv2 o2dsoffre",
                    },
                    timeout=10
                )
                if response.status_code == 200:
                    data              = response.json()
                    self.token        = data["access_token"]
                    self.token_expiry = time.time() + data.get("expires_in", 1500)
                    logger.info(f"✅ Token obtenu (valide {data.get('expires_in', 1500)}s)")
                    return self.token
                else:
                    logger.error(f"❌ Token HTTP {response.status_code}: {response.text[:100]}")
            except Exception as e:
                logger.error(f"❌ Erreur token tentative {attempt+1}: {e}")
                if attempt < 2:
                    time.sleep(self.RETRY_DELAYS[attempt])
        return None

    # ══════════════════════════════════════════
    # EXTRACTION MÉTADONNÉES
    # ══════════════════════════════════════════

    def _extract_skills(self, text: str) -> List[str]:
        return [skill for skill, pattern in self.SKILLS.items()
                if re.search(pattern, text, re.IGNORECASE)]

    def _extract_contract(self, type_contrat: str) -> str:
        return {
            "CDI": "CDI", "CDD": "CDD", "MIS": "Intérim",
            "SAI": "Saisonnier", "LIB": "Freelance", "CCE": "Freelance",
        }.get(type_contrat, "Non spécifié")

    def _extract_experience(self, text: str) -> str:
        for lv, p in {
            "Entry":  r"\bjunior\b|\bdébutant\b|\bentry\b|\bsans expérience\b",
            "Mid":    r"\bconfirmé\b|\b2[-–]5 ans\b|\bmid\b|\b2 ans\b|\b3 ans\b",
            "Senior": r"\bsenior\b|\b5\+\s*ans\b|\bexpérimenté\b",
            "Lead":   r"\blead\b|\bmanager\b|\bprincipal\b|\bresponsable\b",
        }.items():
            if re.search(p, text, re.IGNORECASE):
                return lv
        return "Non spécifié"

    def _extract_salary(self, salaire: dict) -> Tuple[Optional[int], Optional[int]]:
        if not salaire:
            return None, None
        numbers = re.findall(r'\d+', salaire.get("libelle", "").replace(" ", ""))
        if len(numbers) >= 2:
            return int(numbers[0]), int(numbers[1])
        elif len(numbers) == 1:
            return int(numbers[0]), None
        return None, None

    # ══════════════════════════════════════════
    # APPEL API — PAGINATION + RETRY
    # ══════════════════════════════════════════

    def _search_page(self, keyword: str, start: int) -> Tuple[List[Dict], int]:
        token = self._obtenir_token()
        if not token:
            return [], 0

        end     = min(start + self.PAGE_SIZE - 1, self.MAX_PER_CALL)
        headers = {"Authorization": f"Bearer {token}"}
        params  = {"motsCles": keyword, "range": f"{start}-{end}", "sort": 1}

        for attempt in range(3):
            try:
                self.stats["total_appels_api"] += 1
                resp = requests.get(self.API_URL, headers=headers, params=params, timeout=15)

                if resp.status_code == 401:
                    self.token = None
                    token      = self._obtenir_token()
                    headers    = {"Authorization": f"Bearer {token}"}
                    resp       = requests.get(self.API_URL, headers=headers, params=params, timeout=15)

                if resp.status_code in (200, 206):
                    results = resp.json().get("resultats", [])
                    total   = 0
                    match   = re.search(r'/(\d+)', resp.headers.get("Content-Range", ""))
                    if match:
                        total = int(match.group(1))
                    logger.info(f"  📄 Page {start//self.PAGE_SIZE + 1} : {len(results)} offres (total: {total or '?'})")
                    return results, total

                elif resp.status_code == 429:
                    time.sleep(int(resp.headers.get("Retry-After", self.RETRY_DELAYS[attempt])))
                else:
                    logger.error(f"❌ HTTP {resp.status_code} pour '{keyword}'")
                    if attempt < 2:
                        time.sleep(self.RETRY_DELAYS[attempt])

            except Exception as e:
                logger.error(f"❌ Erreur tentative {attempt+1}: {e}")
                if attempt < 2:
                    time.sleep(self.RETRY_DELAYS[attempt])

        return [], 0

    # ══════════════════════════════════════════
    # NORMALISATION — FORMAT STANDARD PROJET
    # ══════════════════════════════════════════

    def _normalize(self, raw: Dict, keyword: str) -> Dict:
        description = raw.get("description", "")
        titre       = raw.get("intitule", "")
        sal_min, sal_max = self._extract_salary(raw.get("salaire", {}))

        competences_raw = [c.get("libelle", "") for c in raw.get("competences", [])]
        competences_nlp = self._extract_skills(f"{titre} {description}")

        return {
            "id":                  f"ft_{raw.get('id', '')}",
            "titre":               titre,
            "entreprise":          raw.get("entreprise", {}).get("nom", "Non spécifié"),
            "lieu":                raw.get("lieuTravail", {}).get("libelle", "France"),
            "description":         description,
            "description_snippet": description[:300],
            "competences":         list(set(competences_raw + competences_nlp)),
            "salaire_min":         sal_min,
            "salaire_max":         sal_max,
            "devise_salaire":      "EUR",
            "type_contrat":        self._extract_contract(raw.get("typeContrat", "")),
            "niveau_experience":   self._extract_experience(description),
            "date_publication":    raw.get("dateCreation", ""),
            "url":                 raw.get("origineOffre", {}).get("urlOrigine", ""),
            "source":              "france_travail",
            "date_scraping":       datetime.now().isoformat(),
            "statut":              "active",
            "query_recherche":     keyword,
        }

    # ══════════════════════════════════════════
    # PIPELINE PRINCIPAL
    # ══════════════════════════════════════════

    def scrape_all_data_jobs(self, max_offres_par_keyword: int = 500) -> List[Dict]:
        seen_ids         = set()
        seen_fingerprint = set()
        all_offers       = []

        for keyword in self.data_jobs_keywords:
            logger.info(f"\n📊 ══ {keyword} ══")
            kw_count = 0
            start    = 0

            while start < max_offres_par_keyword:
                raw_list, total_dispo = self._search_page(keyword, start)
                if not raw_list:
                    break

                for raw in raw_list:
                    try:
                        offer = self._normalize(raw, keyword)
                        self.stats["total_brut"] += 1

                        if offer["id"] in seen_ids:
                            self.stats["total_doublons"] += 1
                            continue
                        fp = f"{offer['titre'].lower()}|{offer['entreprise'].lower()}|{offer['lieu'].lower()}"
                        if fp in seen_fingerprint:
                            self.stats["total_doublons"] += 1
                            continue

                        seen_ids.add(offer["id"])
                        seen_fingerprint.add(fp)
                        all_offers.append(offer)
                        kw_count += 1

                    except Exception as e:
                        logger.error(f"Erreur normalisation: {e}")
                        self.stats["total_erreurs"] += 1

                if len(raw_list) < self.PAGE_SIZE:
                    break
                if start + self.PAGE_SIZE > self.MAX_PER_CALL:
                    logger.info(f"  ⚠️ Limite API atteinte pour '{keyword}'")
                    break

                start += self.PAGE_SIZE
                time.sleep(0.3)

            self.stats["offres_par_keyword"][keyword] = kw_count
            logger.info(f"  ✅ {kw_count} offres uniques pour '{keyword}'")
            time.sleep(1)

        logger.info(f"\n🎯 Total offres uniques : {len(all_offers)}")
        return all_offers

    # ══════════════════════════════════════════
    # SAUVEGARDE BRONZE — ✅ Structure Medallion
    # ══════════════════════════════════════════

    def save_to_json_bronze(self, offers: List[Dict]) -> str:
        """data_lake/bronze/france_travail/YYYY/MM/DD/offres_TIMESTAMP.json"""
        now       = datetime.now()
        date_path = now.strftime("%Y/%m/%d")
        timestamp = now.strftime("%Y%m%d_%H%M%S")

        output_dir = Path("data_lake/bronze/france_travail") / date_path
        output_dir.mkdir(parents=True, exist_ok=True)

        path = output_dir / f"offres_{timestamp}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(offers, f, ensure_ascii=False, indent=2)

        logger.info(f"💾 Bronze JSON : {path} ({len(offers)} offres)")
        return str(path)

    def save_to_minio_bronze(self, offers: List[Dict]):
        """MinIO : bronze/raw/france_travail/YYYY/MM/DD/offres_TIMESTAMP.json"""
        try:
            from minio import Minio
            client = Minio(
                os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", ""),
                access_key=os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
                secret_key=os.getenv("MINIO_SECRET_KEY", "minio_password"),
                secure=False,
            )
            now         = datetime.now()
            date_path   = now.strftime("%Y/%m/%d")
            timestamp   = now.strftime("%Y%m%d_%H%M%S")
            object_name = f"raw/france_travail/{date_path}/offres_{timestamp}.json"

            data = json.dumps(offers, ensure_ascii=False, indent=2).encode("utf-8")
            client.put_object(
                self.MINIO_BUCKET_BRONZE, object_name,
                io.BytesIO(data), len(data), "application/json"
            )
            logger.info(f"📦 MinIO Bronze : {self.MINIO_BUCKET_BRONZE}/{object_name}")
        except Exception as e:
            logger.warning(f"⚠️ MinIO Bronze : {e}")

    def save_to_kafka_bronze(self, offers: List[Dict]):
        """Kafka topic : bronze.france_travail.offres"""
        if not self.use_kafka or not self.producer:
            logger.warning("⚠️ Kafka non disponible")
            return
        ok = 0
        for o in offers:
            try:
                self.producer.send(self.KAFKA_TOPIC_BRONZE, value=o).get(timeout=10)
                ok += 1
            except Exception as e:
                logger.error(f"Kafka: {e}")
        self.producer.flush()
        logger.info(f"✅ {ok}/{len(offers)} → Kafka : {self.KAFKA_TOPIC_BRONZE}")

    # ══════════════════════════════════════════
    # RÉSUMÉ
    # ══════════════════════════════════════════

    def _print_summary(self, offers: List[Dict]):
        skills    = Counter()
        contracts = Counter()
        lieux     = Counter()
        for o in offers:
            skills.update(o["competences"])
            contracts[o["type_contrat"]] += 1
            lieu = o["lieu"].split(" - ")[-1] if " - " in o["lieu"] else o["lieu"]
            lieux[lieu] += 1

        print("\n" + "=" * 60)
        print("📊 RÉSUMÉ FRANCE-TRAVAIL v3")
        print("=" * 60)
        print(f"Total offres uniques  : {len(offers)}")
        print(f"\n📈 Offres par keyword :")
        for kw, count in self.stats["offres_par_keyword"].items():
            print(f"   - {kw}: {count}")
        print(f"\n🔧 Top 10 compétences :")
        for s, c in skills.most_common(10):
            print(f"   - {s}: {c}")
        print(f"\n📄 Types de contrat :")
        for ct, c in contracts.most_common():
            print(f"   - {ct}: {c}")
        print(f"\n📍 Top 5 localisations :")
        for lieu, c in lieux.most_common(5):
            print(f"   - {lieu}: {c}")
        salaries = [o for o in offers if o["salaire_min"]]
        print(f"\n💰 Offres avec salaire : {len(salaries)}/{len(offers)}")
        print(f"\n🔁 Doublons supprimés  : {self.stats['total_doublons']}")
        print(f"📡 Appels API total    : {self.stats['total_appels_api']}")
        print(f"\n📁 Sauvegardé dans :")
        print(f"   Local : data_lake/bronze/france_travail/YYYY/MM/DD/")
        print(f"   MinIO : bronze/raw/france_travail/YYYY/MM/DD/")
        print(f"   Kafka : {self.KAFKA_TOPIC_BRONZE}")
        print("=" * 60)

    # ══════════════════════════════════════════
    # POINT D'ENTRÉE
    # ══════════════════════════════════════════

    def run(self, max_offres_par_keyword: int = 500):
        logger.info("🚀 Démarrage France-Travail Scraper v3")
        t0 = time.time()

        offers = self.scrape_all_data_jobs(max_offres_par_keyword=max_offres_par_keyword)

        if not offers:
            logger.warning("⚠️ Aucune offre récupérée")
            logger.warning(f"   CLIENT_ID     : {self.client_id[:20] if self.client_id else 'MANQUANT'}...")
            logger.warning(f"   CLIENT_SECRET : {'✓ Présent' if self.client_secret else '✗ MANQUANT'}")
            return []

        # ── Sauvegarde Bronze (structure Medallion) ──
        self.save_to_json_bronze(offers)
        self.save_to_minio_bronze(offers)
        self.save_to_kafka_bronze(offers)

        logger.info(f"✨ Terminé en {time.time()-t0:.1f}s — {len(offers)} offres")
        self._print_summary(offers)
        return offers


if __name__ == "__main__":
    scraper = FranceTravailScraper(use_kafka=True)
    results = scraper.run(max_offres_par_keyword=500)

    if results:
        print("\n📋 Exemple d'offre normalisée :")
        print(json.dumps(results[0], ensure_ascii=False, indent=2))