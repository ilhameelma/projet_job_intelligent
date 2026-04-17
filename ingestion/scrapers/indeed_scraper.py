#!/usr/bin/env python3
"""
SCRAPER ADZUNA - API gratuite qui agrège Indeed + LinkedIn + autres
Inscription : https://developer.adzuna.com/ (gratuit, 1 minute)

ARCHITECTURE MEDALLION - BRONZE LAYER
Stockage : bronze/raw/adzuna/YYYY/MM/DD/offres_TIMESTAMP.json
"""

import json
import logging
import time
import os
import re
import io
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AdzunaScraper:

    BASE_URL = "https://api.adzuna.com/v1/api/jobs/fr/search"

    # ── Topics Kafka conformes à l'architecture Medallion ──
    KAFKA_TOPIC_BRONZE = "bronze.adzuna.offres"
    KAFKA_TOPIC_SILVER = "silver.adzuna.offres"

    # ── Buckets MinIO conformes ──
    MINIO_BUCKET_BRONZE = "bronze"
    MINIO_BUCKET_SILVER = "silver"

    def __init__(self, app_id: str = None, app_key: str = None, use_kafka: bool = True):
        # Credentials depuis variables d'env ou paramètres
        self.app_id = app_id or os.getenv("ADZUNA_APP_ID", "METS_TON_APP_ID")
        self.app_key = app_key or os.getenv("ADZUNA_APP_KEY", "METS_TON_APP_KEY")
        self.use_kafka = use_kafka
        self.producer = None

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
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                compression_type="gzip",
            )
            logger.info("✅ Kafka connecté")
        except Exception as e:
            logger.warning(f"⚠️ Kafka non disponible: {e}")
            self.use_kafka = False

    # ══════════════════════════════════════════
    # EXTRACTION MÉTADONNÉES
    # ══════════════════════════════════════════

    def _extract_skills(self, text: str) -> List[str]:
        skills = [
            "Python", "Java", "Scala", "R", "SQL", "NoSQL",
            "Spark", "Hadoop", "Kafka", "Flink", "Airflow",
            "TensorFlow", "PyTorch", "Scikit-learn", "XGBoost", "LightGBM",
            "Tableau", "Power BI", "Looker", "Qlik", "Superset",
            "AWS", "Azure", "GCP", "Databricks", "Snowflake", "BigQuery",
            "Docker", "Kubernetes", "Git", "Terraform",
            "PostgreSQL", "MySQL", "MongoDB", "Elasticsearch",
        ]
        t = text.lower()
        return list({s for s in skills if s.lower() in t})

    def _extract_contract(self, text: str) -> str:
        for c, p in {
            "CDI": r"\bCDI\b|\bpermanent\b",
            "CDD": r"\bCDD\b|\bcontract\b",
            "Freelance": r"\bfreelance\b|\bindépendant\b",
            "Stage": r"\bstage\b|\balternance\b|\binternship\b",
        }.items():
            if re.search(p, text, re.IGNORECASE):
                return c
        return "Non spécifié"

    def _extract_experience(self, text: str) -> str:
        for lv, p in {
            "Entry": r"\bjunior\b|\bdébutant\b|\bentry\b",
            "Mid": r"\bconfirmé\b|\b2[-–]5 ans\b|\bmid\b",
            "Senior": r"\bsenior\b|\b5\+\s*ans\b",
            "Lead": r"\blead\b|\bmanager\b|\bprincipal\b",
        }.items():
            if re.search(p, text, re.IGNORECASE):
                return lv
        return "Non spécifié"

    # ══════════════════════════════════════════
    # APPEL API ADZUNA
    # ══════════════════════════════════════════

    def search(self, keyword: str, page: int = 1, results_per_page: int = 50) -> List[Dict]:
        """Appelle l'API Adzuna pour un mot-clé donné."""
        url = f"{self.BASE_URL}/{page}"
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "what": keyword,
            "where": "France",
            "results_per_page": results_per_page,
            "sort_by": "date",
            "content-type": "application/json",
        }

        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            logger.info(f"  ✅ {len(results)} offres (page {page}) pour '{keyword}'")
            return results
        except requests.HTTPError as e:
            logger.error(f"❌ HTTP {resp.status_code} pour '{keyword}': {e}")
            if resp.status_code == 401:
                logger.error("   → Vérifie APP_ID et APP_KEY sur https://developer.adzuna.com/")
            return []
        except Exception as e:
            logger.error(f"❌ Erreur recherche '{keyword}': {e}")
            return []

    def _normalize(self, raw: Dict, keyword: str) -> Dict:
        """Transforme une offre brute Adzuna → format standard du projet."""
        description = raw.get("description", "")
        title = raw.get("title", "")
        company = raw.get("company", {}).get("display_name", "Non spécifié")
        location = raw.get("location", {}).get("display_name", "France")
        salary_min = raw.get("salary_min")
        salary_max = raw.get("salary_max")
        url = raw.get("redirect_url", "")
        date_pub = raw.get("created", "")

        return {
            "id": f"adzuna_{raw.get('id', '')}",
            "titre": title,
            "entreprise": company,
            "lieu": location,
            "description": description,
            "description_snippet": description[:300],
            "competences": self._extract_skills(f"{title} {description}"),
            "salaire_min": int(salary_min) if salary_min else None,
            "salaire_max": int(salary_max) if salary_max else None,
            "devise_salaire": "EUR",
            "type_contrat": self._extract_contract(description),
            "niveau_experience": self._extract_experience(description),
            "date_publication": date_pub,
            "url": url,
            "source": "adzuna",
            "date_scraping": datetime.now().isoformat(),
            "statut": "active",
            "query_recherche": keyword,
        }

    # ══════════════════════════════════════════
    # PIPELINE PRINCIPAL
    # ══════════════════════════════════════════

    def scrape_all_data_jobs(self, max_pages: int = 3) -> List[Dict]:
        all_offers = []

        for keyword in self.data_jobs_keywords:
            logger.info(f"\n📊 === {keyword} ===")
            for page in range(1, max_pages + 1):
                raw_list = self.search(keyword, page=page)
                if not raw_list:
                    break
                for raw in raw_list:
                    try:
                        all_offers.append(self._normalize(raw, keyword))
                    except Exception as e:
                        logger.error(f"Erreur normalisation: {e}")
                time.sleep(0.5)  # respecter le rate limit Adzuna
            time.sleep(1)

        # Dédoublonnage par ID
        unique = {o["id"]: o for o in all_offers}
        logger.info(f"\n🎯 Total offres uniques: {len(unique)}")
        return list(unique.values())

    # ══════════════════════════════════════════
    # SAUVEGARDE BRONZE (Format standardisé)
    # ══════════════════════════════════════════

    def save_to_json_bronze(self, offers: List[Dict]) -> str:
        """
        Sauvegarde locale au format Bronze
        data_lake/bronze/adzuna/YYYY/MM/DD/offres_TIMESTAMP.json
        """
        now = datetime.now()
        date_path = now.strftime("%Y/%m/%d")
        timestamp = now.strftime("%Y%m%d_%H%M%S")

        output_dir = Path("data_lake/bronze/adzuna") / date_path
        output_dir.mkdir(parents=True, exist_ok=True)

        path = output_dir / f"offres_{timestamp}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(offers, f, ensure_ascii=False, indent=2)

        logger.info(f"💾 Bronze JSON : {path} ({len(offers)} offres)")
        return str(path)

    def save_to_minio_bronze(self, offers: List[Dict]):
        """
        Sauvegarde MinIO au format Bronze standardisé
        bronze/raw/adzuna/YYYY/MM/DD/offres_TIMESTAMP.json
        """
        try:
            from minio import Minio
            client = Minio(
                os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", ""),
                access_key=os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
                secret_key=os.getenv("MINIO_SECRET_KEY", "minio_password"),
                secure=False,
            )
            now = datetime.now()
            date_path = now.strftime("%Y/%m/%d")
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            object_name = f"raw/adzuna/{date_path}/offres_{timestamp}.json"

            data = json.dumps(offers, ensure_ascii=False, indent=2).encode("utf-8")
            client.put_object(
                self.MINIO_BUCKET_BRONZE, object_name,
                io.BytesIO(data), len(data), "application/json"
            )
            logger.info(f"📦 MinIO Bronze : {self.MINIO_BUCKET_BRONZE}/{object_name}")
        except Exception as e:
            logger.warning(f"⚠️ MinIO Bronze : {e}")

    def save_to_kafka_bronze(self, offers: List[Dict]):
        """Kafka topic : bronze.adzuna.offres"""
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
        from collections import Counter
        skills = Counter()
        contracts = Counter()
        for o in offers:
            skills.update(o["competences"])
            contracts[o["type_contrat"]] += 1

        print("\n" + "=" * 60)
        print("📊 RÉSUMÉ ADZUNA (agrège Indeed + LinkedIn + autres)")
        print("=" * 60)
        print(f"Total offres uniques  : {len(offers)}")
        print(f"\n🔧 Top compétences :")
        for s, c in skills.most_common(10):
            print(f"   - {s}: {c}")
        print(f"\n📄 Types de contrat :")
        for ct, c in contracts.most_common():
            print(f"   - {ct}: {c}")
        salaries = [o for o in offers if o["salaire_min"]]
        print(f"\n💰 Offres avec salaire : {len(salaries)}/{len(offers)}")
        print(f"\n📁 Sauvegarde Bronze :")
        print(f"   Local : data_lake/bronze/adzuna/YYYY/MM/DD/")
        print(f"   MinIO : bronze/raw/adzuna/YYYY/MM/DD/")
        print(f"   Kafka : {self.KAFKA_TOPIC_BRONZE}")
        print("=" * 60)

    # ══════════════════════════════════════════
    # POINT D'ENTRÉE
    # ══════════════════════════════════════════

    def run(self, max_pages: int = 3):
        logger.info("🚀 Démarrage Adzuna Scraper")
        t0 = time.time()

        offers = self.scrape_all_data_jobs(max_pages=max_pages)

        if not offers:
            logger.warning("⚠️ Aucune offre — vérifie APP_ID/APP_KEY sur https://developer.adzuna.com/")
            return []

        # ── Sauvegarde Bronze (structure Medallion standardisée) ──
        self.save_to_json_bronze(offers)
        self.save_to_minio_bronze(offers)
        self.save_to_kafka_bronze(offers)

        logger.info(f"✨ Terminé en {time.time()-t0:.1f}s — {len(offers)} offres")
        self._print_summary(offers)
        return offers


if __name__ == "__main__":
    scraper = AdzunaScraper(
        app_id=os.getenv("ADZUNA_APP_ID", "METS_TON_APP_ID"),
        app_key=os.getenv("ADZUNA_APP_KEY", "METS_TON_APP_KEY"),
        use_kafka=True,
    )
    results = scraper.run(max_pages=3)

    if results:
        print("\n📋 Exemple d'offre normalisée :")
        print(json.dumps(results[0], ensure_ascii=False, indent=2))