#!/usr/bin/env python3
"""
SCRAPER INDEED - Version Selenium (contourne le 403)
Fonctionne depuis Docker avec Chromium headless
"""
 
import json
import logging
import time
import hashlib
import random
import os
import re
from datetime import datetime
from typing import List, Dict, Optional
 
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
 
 
class IndeedSeleniumScraper:
    """Scraper Indeed via Selenium — contourne le blocage 403"""
 
    BASE_URL = "https://fr.indeed.com"
    SEARCH_URL = "https://fr.indeed.com/jobs"
 
    def __init__(self, use_kafka: bool = True, headless: bool = True):
        self.use_kafka = use_kafka
        self.headless = headless
        self.driver = None
        self.kafka_topic = "indeed-offres"
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
 
    # ------------------------------------------------------------------
    # SELENIUM SETUP
    # ------------------------------------------------------------------
 
    def _build_driver(self) -> webdriver.Chrome:
        """
        Configure Chrome/Chromium en mode headless anti-détection.
        Compatible avec l'image Docker selenium/standalone-chrome
        ou avec chromium-driver installé dans airflow-worker.
        """
        options = Options()
 
        if self.headless:
            options.add_argument("--headless=new")          # nouveau mode headless Chrome 112+
 
        # --- Anti-détection ---
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--start-maximized")
        options.add_argument("--lang=fr-FR")
 
        # User-Agent d'un vrai Chrome Windows
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        )
 
        # Chercher chromedriver dans PATH ou chemin explicite
        chromedriver_path = os.getenv("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
        if os.path.exists(chromedriver_path):
            service = Service(chromedriver_path)
            driver = webdriver.Chrome(service=service, options=options)
        else:
            # Laisser Selenium trouver automatiquement
            driver = webdriver.Chrome(options=options)
 
        # Masquer le flag webdriver via JS
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        driver.execute_cdp_cmd(
            "Network.setUserAgentOverride",
            {
                "userAgent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                )
            },
        )
 
        return driver
 
    def _random_sleep(self, min_s: float = 1.5, max_s: float = 4.0):
        """Pause aléatoire pour imiter un humain."""
        time.sleep(random.uniform(min_s, max_s))
 
    def _scroll_page(self):
        """Scroll progressif pour déclencher le chargement lazy."""
        total_height = self.driver.execute_script("return document.body.scrollHeight")
        for i in range(0, total_height, 400):
            self.driver.execute_script(f"window.scrollTo(0, {i});")
            time.sleep(0.1)
 
    # ------------------------------------------------------------------
    # KAFKA
    # ------------------------------------------------------------------
 
    def _init_kafka_producer(self):
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                compression_type="gzip",
            )
            logger.info("✅ Producteur Kafka initialisé")
        except Exception as e:
            logger.warning(f"⚠️ Kafka non disponible: {e}")
            self.use_kafka = False
 
    # ------------------------------------------------------------------
    # EXTRACTION MÉTADONNÉES
    # ------------------------------------------------------------------
 
    def _extract_skills(self, title: str, description: str) -> List[str]:
        skills_map = {
            "Langages": ["Python", "Java", "Scala", "R", "SQL", "NoSQL", "Julia"],
            "Big Data": ["Spark", "Hadoop", "Hive", "Kafka", "Flink", "Airflow", "Beam"],
            "ML/AI": ["TensorFlow", "PyTorch", "Scikit-learn", "Keras", "XGBoost", "LightGBM"],
            "Data Viz": ["Tableau", "Power BI", "Looker", "Qlik", "Superset", "Metabase"],
            "Cloud": ["AWS", "Azure", "GCP", "Databricks", "Snowflake", "Redshift", "BigQuery"],
            "DevOps": ["Docker", "Kubernetes", "Jenkins", "Git", "Terraform"],
            "Databases": ["PostgreSQL", "MySQL", "MongoDB", "Cassandra", "Elasticsearch"],
        }
        found, text = [], f"{title} {description}".lower()
        for skills in skills_map.values():
            for s in skills:
                if s.lower() in text:
                    found.append(s)
        return list(set(found))
 
    def _extract_salary(self, text: str) -> Dict:
        patterns = [
            r"(\d{4,6})\s*[-–]\s*(\d{4,6})\s*€",
            r"(\d{4,6})\s*€\s*[-–]\s*(\d{4,6})\s*€",
            r"de\s+(\d{4,6})\s*€\s+à\s+(\d{4,6})\s*€",
            r"(\d{4,6})\s*€\s*(?:brut|annuel)",
        ]
        for p in patterns:
            m = re.search(p, text, re.IGNORECASE)
            if m:
                groups = m.groups()
                if len(groups) == 2:
                    return {"min": int(groups[0]), "max": int(groups[1]), "currency": "EUR"}
                return {"min": int(groups[0]), "max": int(groups[0]), "currency": "EUR"}
        return {"min": None, "max": None, "currency": "EUR"}
 
    def _extract_contract(self, text: str) -> str:
        for contract, pat in {
            "CDI": r"\bCDI\b",
            "CDD": r"\bCDD\b",
            "Freelance": r"\bfreelance\b|\bindépendant\b",
            "Stage": r"\bstage\b|\balternance\b",
            "Intérim": r"\bintérim\b",
        }.items():
            if re.search(pat, text, re.IGNORECASE):
                return contract
        return "Non spécifié"
 
    def _extract_experience(self, text: str) -> str:
        for level, pat in {
            "Entry": r"\bjunior\b|\bdébutant\b",
            "Mid": r"\bconfirmé\b|\b2[-–]5 ans\b",
            "Senior": r"\bsenior\b|\b5[-–]10 ans\b",
            "Lead": r"\blead\b|\bmanager\b",
        }.items():
            if re.search(pat, text, re.IGNORECASE):
                return level
        return "Non spécifié"
 
    # ------------------------------------------------------------------
    # SCRAPING
    # ------------------------------------------------------------------
 
    def _handle_cookies_popup(self):
        """Ferme le bandeau cookies s'il apparaît."""
        try:
            btn = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[contains(text(),'Accepter') or contains(text(),'Accept')]")
                )
            )
            btn.click()
            logger.info("🍪 Popup cookies fermé")
            self._random_sleep(0.5, 1.5)
        except TimeoutException:
            pass  # pas de popup, c'est OK
 
    def scrape_page(self, keyword: str, page: int = 0) -> List[Dict]:
        """Scrape une page de résultats Indeed avec Selenium."""
        url = (
            f"{self.SEARCH_URL}"
            f"?q={keyword.replace(' ', '+')}"
            f"&l=France"
            f"&start={page * 10}"
            f"&sort=date"
        )
        logger.info(f"🔍 {keyword} — page {page + 1} → {url}")
 
        self.driver.get(url)
        self._random_sleep(2, 4)
        self._handle_cookies_popup()
        self._scroll_page()
 
        jobs = []
 
        # --- Sélecteurs Indeed (plusieurs variantes selon la version A/B d'Indeed) ---
        card_selectors = [
            (By.CSS_SELECTOR, "div.job_seen_beacon"),
            (By.CSS_SELECTOR, "div[data-testid='job-card']"),
            (By.CSS_SELECTOR, "li.css-1ac2h1w"),          # variante récente
        ]
 
        cards = []
        for by, sel in card_selectors:
            cards = self.driver.find_elements(by, sel)
            if cards:
                break
 
        if not cards:
            logger.warning(f"⚠️ Aucune carte trouvée page {page + 1} — Indeed a peut-être changé son HTML")
            return []
 
        for card in cards:
            try:
                job = self._extract_from_card(card, keyword)
                if job:
                    jobs.append(job)
            except Exception as e:
                logger.debug(f"Erreur carte: {e}")
 
        logger.info(f"✅ {len(jobs)} offres extraites")
        return jobs
 
    def _safe_text(self, element, by: By, selector: str, default: str = "") -> str:
        """Extrait le texte d'un sous-élément sans lever d'exception."""
        try:
            return element.find_element(by, selector).text.strip()
        except NoSuchElementException:
            return default
 
    def _extract_from_card(self, card, keyword: str) -> Optional[Dict]:
        """Extrait les données d'une carte d'offre Indeed."""
 
        # Titre — plusieurs variantes
        title = ""
        for sel in ["h2.jobTitle", "a.jcs-JobTitle", "[data-testid='jobsearch-JobInfoHeader-title']"]:
            title = self._safe_text(card, By.CSS_SELECTOR, sel)
            if title:
                break
        if not title:
            return None
 
        # URL de l'offre
        url = ""
        try:
            link = card.find_element(By.CSS_SELECTOR, "h2.jobTitle a, a.jcs-JobTitle")
            href = link.get_attribute("href") or ""
            url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
        except NoSuchElementException:
            pass
 
        # Entreprise
        company = (
            self._safe_text(card, By.CSS_SELECTOR, "span[data-testid='company-name']")
            or self._safe_text(card, By.CSS_SELECTOR, "span.companyName")
            or "Non spécifié"
        )
 
        # Lieu
        location = (
            self._safe_text(card, By.CSS_SELECTOR, "div[data-testid='text-location']")
            or self._safe_text(card, By.CSS_SELECTOR, "div.companyLocation")
            or "France"
        )
 
        # Snippet de description
        snippet = (
            self._safe_text(card, By.CSS_SELECTOR, "div[data-testid='job-snippet']")
            or self._safe_text(card, By.CSS_SELECTOR, "div.job-snippet")
        )
 
        # Date de publication
        date_posted = (
            self._safe_text(card, By.CSS_SELECTOR, "span[data-testid='myJobsStateDate']")
            or self._safe_text(card, By.CSS_SELECTOR, "span.date")
        )
 
        # Description complète (optionnel — ralentit le scraping)
        full_description = self._get_full_description(url) if url else snippet
 
        job_id = hashlib.md5(f"{title}_{company}_{location}".encode()).hexdigest()
        salary = self._extract_salary(full_description)
 
        return {
            "id": f"indeed_{job_id}",
            "titre": title,
            "entreprise": company,
            "lieu": location,
            "description": full_description or snippet,
            "description_snippet": snippet,
            "competences": self._extract_skills(title, full_description),
            "salaire_min": salary["min"],
            "salaire_max": salary["max"],
            "devise_salaire": salary["currency"],
            "type_contrat": self._extract_contract(full_description),
            "niveau_experience": self._extract_experience(full_description),
            "date_publication": date_posted,
            "url": url,
            "source": "indeed",
            "date_scraping": datetime.now().isoformat(),
            "statut": "active",
            "query_recherche": keyword,
        }
 
    def _get_full_description(self, url: str) -> str:
        """Ouvre la page d'une offre et récupère sa description complète."""
        if not url:
            return ""
        try:
            # Ouvre dans un nouvel onglet pour garder la liste
            self.driver.execute_script("window.open(arguments[0]);", url)
            self.driver.switch_to.window(self.driver.window_handles[-1])
            self._random_sleep(1.5, 3)
 
            desc = ""
            for sel in [
                "#jobDescriptionText",
                "div.jobsearch-jobDescriptionText",
                "div[data-testid='jobDescriptionText']",
            ]:
                try:
                    elem = self.driver.find_element(By.CSS_SELECTOR, sel)
                    desc = elem.text.strip()
                    if desc:
                        break
                except NoSuchElementException:
                    continue
 
            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
            return desc
        except Exception as e:
            logger.debug(f"Erreur description complète: {e}")
            try:
                # S'assurer qu'on revient à l'onglet principal
                self.driver.switch_to.window(self.driver.window_handles[0])
            except Exception:
                pass
            return ""
 
    def scrape_all_data_jobs(self, max_pages: int = 3) -> List[Dict]:
        """Scrape tous les métiers Data sur Indeed."""
        all_jobs = []
 
        for keyword in self.data_jobs_keywords:
            logger.info(f"\n📊 === {keyword} ===")
            for page in range(max_pages):
                jobs = self.scrape_page(keyword, page)
                if not jobs:
                    break
                all_jobs.extend(jobs)
                self._random_sleep(2, 5)
            self._random_sleep(3, 6)
 
        # Dédoublonnage
        unique = {j["id"]: j for j in all_jobs}
        logger.info(f"\n🎯 Total offres uniques: {len(unique)}")
        return list(unique.values())
 
    # ------------------------------------------------------------------
    # SAUVEGARDE
    # ------------------------------------------------------------------
 
    def save_to_kafka(self, offers: List[Dict]):
        if not self.use_kafka or not self.producer:
            logger.warning("⚠️ Kafka non disponible")
            return
        ok = 0
        for o in offers:
            try:
                self.producer.send(self.kafka_topic, value=o).get(timeout=10)
                ok += 1
            except Exception as e:
                logger.error(f"Kafka error: {e}")
        self.producer.flush()
        logger.info(f"✅ {ok}/{len(offers)} → Kafka ({self.kafka_topic})")
 
    def save_to_json(self, offers: List[Dict], out_dir: str = "/opt/airflow/scrapers/data"):
        os.makedirs(out_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = f"{out_dir}/indeed_offres_{ts}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(offers, f, ensure_ascii=False, indent=2)
        logger.info(f"💾 JSON: {path}")
        return path
 
    def save_to_minio(self, offers: List[Dict]):
        try:
            from minio import Minio
            import io
            client = Minio(
                os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", ""),
                access_key=os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
                secret_key=os.getenv("MINIO_SECRET_KEY", "minio_password"),
                secure=False,
            )
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            data = json.dumps(offers, ensure_ascii=False, indent=2).encode("utf-8")
            client.put_object(
                "bronze", f"indeed/offres_{ts}.json",
                io.BytesIO(data), len(data), "application/json"
            )
            logger.info(f"📦 Minio: bronze/indeed/offres_{ts}.json")
        except Exception as e:
            logger.warning(f"⚠️ Minio: {e}")
 
    def _print_summary(self, offers: List[Dict]):
        from collections import Counter
        skills = Counter()
        for o in offers:
            skills.update(o["competences"])
        print("\n" + "=" * 60)
        print("📊 RÉSUMÉ INDEED (Selenium)")
        print("=" * 60)
        print(f"Total: {len(offers)} offres")
        print("\n🔧 Top compétences:")
        for s, c in skills.most_common(10):
            print(f"   - {s}: {c}")
        print("=" * 60)
 
    # ------------------------------------------------------------------
    # PIPELINE PRINCIPAL
    # ------------------------------------------------------------------
 
    def run(self, max_pages: int = 3):
        logger.info("🚀 Démarrage scraper Indeed (Selenium)")
        t0 = time.time()
 
        self.driver = self._build_driver()
        try:
            offers = self.scrape_all_data_jobs(max_pages=max_pages)
        finally:
            self.driver.quit()
            logger.info("🔒 Navigateur fermé")
 
        if not offers:
            logger.warning("⚠️ Aucune offre trouvée")
            return []
 
        self.save_to_kafka(offers)
        self.save_to_json(offers)
        self.save_to_minio(offers)
 
        logger.info(f"✨ Terminé en {time.time() - t0:.1f}s — {len(offers)} offres")
        self._print_summary(offers)
        return offers
 
 
# ------------------------------------------------------------------
# POINT D'ENTRÉE
# ------------------------------------------------------------------
if __name__ == "__main__":
    scraper = IndeedSeleniumScraper(use_kafka=True, headless=True)
    results = scraper.run(max_pages=3)
 
    if results:
        print("\n📋 Exemple d'offre:")
        print(json.dumps(results[0], ensure_ascii=False, indent=2))