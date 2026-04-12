#!/usr/bin/env python3
"""
SCRAPER WELCOMETOTHEJUNGLE v2 - Selenium
Sélecteurs CSS corrigés via debug du DOM réel WTTJ
Architecture : Bronze (raw) → Silver (clean) → Gold (enrichi)
Stockage     : MinIO (bronze/raw/welcometothejungle/YYYY/MM/DD/) + Kafka + JSON local
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

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WelcomeToTheJungleScraper:

    BASE_URL   = "https://www.welcometothejungle.com"
    SEARCH_URL = "https://www.welcometothejungle.com/fr/jobs"

    # ── Topics Kafka conformes à l'architecture Medallion ──
    KAFKA_TOPIC_BRONZE = "bronze.welcometothejungle.offres"
    KAFKA_TOPIC_SILVER = "silver.welcometothejungle.offres"

    # ── Buckets MinIO conformes ──
    MINIO_BUCKET_BRONZE = "bronze"
    MINIO_BUCKET_SILVER = "silver"

    # ── Timeouts & scroll ──
    PAGE_LOAD_TIMEOUT   = 20
    ELEMENT_TIMEOUT     = 10
    SCROLL_PAUSE        = 1.5
    SCROLL_MAX_ATTEMPTS = 3

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

    def __init__(self, use_kafka: bool = True, headless: bool = True):
        self.use_kafka = use_kafka
        self.headless  = headless
        self.producer  = None
        self.driver    = None

        self.stats = {
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
            "Analytics Engineer",
            "MLOps",
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
    # SELENIUM — DRIVER
    # ══════════════════════════════════════════

    def _init_driver(self):
        """Initialise Chrome headless avec options anti-détection"""
        from webdriver_manager.chrome import ChromeDriverManager

        options = Options()

        if self.headless:
            options.add_argument("--headless=new")

        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--lang=fr-FR")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )

        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        self.driver.set_page_load_timeout(self.PAGE_LOAD_TIMEOUT)
        self.driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        logger.info("✅ Chrome Selenium initialisé")

    def _close_driver(self):
        if self.driver:
            try:
                self.driver.quit()
                logger.info("🔒 Driver Chrome fermé")
            except Exception:
                pass
            self.driver = None

    # ══════════════════════════════════════════
    # COOKIES — ✅ JS click corrigé
    # ══════════════════════════════════════════

    def _accept_cookies(self):
        """Ferme le bandeau cookies — JS click pour contourner les overlays"""
        try:
            btn = WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((By.ID, "axeptio_btn_acceptAll"))
            )
            self.driver.execute_script("arguments[0].click();", btn)
            logger.info("🍪 Cookies acceptés")
            time.sleep(1.5)
        except TimeoutException:
            try:
                btns = self.driver.find_elements(By.TAG_NAME, "button")
                for b in btns:
                    txt = b.get_attribute("aria-label") or b.text
                    if "accepter" in txt.lower() or "accept" in txt.lower():
                        self.driver.execute_script("arguments[0].click();", b)
                        logger.info("🍪 Cookies acceptés (fallback)")
                        time.sleep(1.5)
                        return
            except Exception:
                pass
            logger.debug("Pas de bandeau cookies détecté")
        except Exception as e:
            logger.warning(f"⚠️ Cookies: {e}")

    # ══════════════════════════════════════════
    # NAVIGATION & SCROLL
    # ══════════════════════════════════════════

    def _build_search_url(self, keyword: str) -> str:
        import urllib.parse
        query = urllib.parse.quote(keyword)
        return f"{self.SEARCH_URL}?query={query}&page=1"

    def _scroll_to_load_all(self, max_offers: int = 100) -> List:
        """Scroll infini — WTTJ charge ~30 offres par page"""
        last_count    = 0
        stale_scrolls = 0

        while True:
            cards = self.driver.find_elements(
                By.CSS_SELECTOR,
                "li[data-testid='search-results-list-item-wrapper']"
            )
            current_count = len(cards)
            logger.info(f"  🔄 Cartes chargées: {current_count}")

            if current_count >= max_offers:
                logger.info(f"  ✅ Max offres atteint ({max_offers})")
                break

            if current_count == last_count:
                stale_scrolls += 1
                if stale_scrolls >= self.SCROLL_MAX_ATTEMPTS:
                    logger.info(f"  ✅ Fin de page (pas de nouvelles offres après {stale_scrolls} scrolls)")
                    break
            else:
                stale_scrolls = 0

            last_count = current_count
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(self.SCROLL_PAUSE)

        return self.driver.find_elements(
            By.CSS_SELECTOR,
            "li[data-testid='search-results-list-item-wrapper']"
        )

    # ══════════════════════════════════════════
    # EXTRACTION CARTE — ✅ CORRIGÉE
    # ══════════════════════════════════════════

    def _extract_card_data(self, card, keyword: str) -> Optional[Dict]:
        """
        Sélecteurs confirmés via debug DOM réel WTTJ :
        - Titre  : h2 > div[role='mark'] ou aria-label du <a>
        - URL    : <a href="/fr/companies/.../jobs/...">
        - Entrep.: alt de l'<img> logo
        - Lieu   : spans courts heuristiques
        """
        try:
            # ── TITRE ──────────────────────────────────────
            titre = ""

            try:
                titre = card.find_element(By.CSS_SELECTOR, "h2").text.strip()
            except NoSuchElementException:
                pass

            if not titre:
                try:
                    titre = card.find_element(
                        By.CSS_SELECTOR, "div[role='mark']"
                    ).text.strip()
                except NoSuchElementException:
                    pass

            if not titre:
                try:
                    lbl   = card.find_element(By.TAG_NAME, "a").get_attribute("aria-label") or ""
                    titre = re.sub(r"^Consultez l'offre\s*", "", lbl, flags=re.IGNORECASE).strip()
                except NoSuchElementException:
                    pass

            if not titre:
                try:
                    titre = self.driver.execute_script(
                        "return arguments[0].querySelector('h2,h3')?.textContent?.trim() || ''",
                        card
                    ) or ""
                except Exception:
                    pass

            if not titre:
                return None

            # ── URL ────────────────────────────────────────
            url      = ""
            offer_id = ""
            try:
                links = card.find_elements(By.TAG_NAME, "a")
                for link in links:
                    href = link.get_attribute("href") or ""
                    if "/jobs/" in href:
                        url = href if href.startswith("http") else self.BASE_URL + href
                        break
                if not url and links:
                    href = links[0].get_attribute("href") or ""
                    url  = href if href.startswith("http") else self.BASE_URL + href
            except NoSuchElementException:
                pass

            if url:
                match = re.search(r'/companies/([^/]+)/jobs/([^?#]+)', url)
                if match:
                    offer_id = f"{match.group(1)}__{match.group(2)}"
                else:
                    offer_id = re.sub(r'\W+', '_', url.split("/")[-1])
            if not offer_id:
                offer_id = re.sub(r'\W+', '_', f"{titre}_{keyword}").lower()

            # ── ENTREPRISE ─────────────────────────────────
            entreprise = "Non spécifié"
            try:
                imgs = card.find_elements(By.TAG_NAME, "img")
                for img in imgs:
                    alt = (img.get_attribute("alt") or "").strip()
                    if alt and alt.lower() not in ("cover", "") and len(alt) < 80:
                        entreprise = alt
                        break
            except Exception:
                pass

            if entreprise == "Non spécifié":
                try:
                    entreprise = self.driver.execute_script(
                        "return arguments[0].querySelector('img')?.alt?.trim() || 'Non spécifié'",
                        card
                    )
                except Exception:
                    pass

            # ── LIEU ───────────────────────────────────────
            lieu = "France"
            try:
                for sel in [
                    "[data-testid*='location']",
                    "[data-testid*='city']",
                    "[data-testid*='place']",
                    "[data-testid*='contract']",
                ]:
                    try:
                        val = card.find_element(By.CSS_SELECTOR, sel).text.strip()
                        if val and val not in (titre, entreprise):
                            lieu = val
                            break
                    except NoSuchElementException:
                        continue

                if lieu == "France":
                    spans = card.find_elements(By.TAG_NAME, "span")
                    for span in spans:
                        txt = span.text.strip()
                        if (2 < len(txt) < 35
                                and not txt.isdigit()
                                and "€" not in txt
                                and txt not in (titre, entreprise)
                                and not re.match(r'^\d', txt)):
                            lieu = txt
                            break
            except Exception:
                pass

            # ── TYPE DE CONTRAT ────────────────────────────
            type_contrat = "Non spécifié"
            try:
                card_text = self.driver.execute_script(
                    "return arguments[0].textContent || ''", card
                )
                for k, v in {
                    "CDI": "CDI", "CDD": "CDD", "Freelance": "Freelance",
                    "Stage": "Stage", "Alternance": "Alternance",
                    "Intérim": "Intérim", "VIE": "VIE",
                }.items():
                    if k in card_text:
                        type_contrat = v
                        break
            except Exception:
                pass

            return {
                "_titre":      titre,
                "_entreprise": entreprise,
                "_lieu":       lieu,
                "_url":        url,
                "_id":         offer_id,
                "_contrat":    type_contrat,
                "_keyword":    keyword,
            }

        except StaleElementReferenceException:
            return None
        except Exception as e:
            logger.debug(f"Erreur extraction carte: {e}")
            return None

    # ══════════════════════════════════════════
    # PAGE DÉTAIL
    # ══════════════════════════════════════════

    def _fetch_offer_detail(self, url: str) -> Dict:
        default = {
            "description":       "",
            "competences":       [],
            "salaire_min":       None,
            "salaire_max":       None,
            "niveau_experience": "Non spécifié",
        }
        if not url:
            return default

        try:
            self.driver.get(url)
            WebDriverWait(self.driver, self.ELEMENT_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "main"))
            )
            time.sleep(1)

            description = ""
            for sel in [
                "[data-testid='job-description']",
                "div.job-description",
                "section[data-testid*='description']",
                "div[class*='description']",
                "main article",
            ]:
                try:
                    description = self.driver.find_element(
                        By.CSS_SELECTOR, sel
                    ).text.strip()
                    if description:
                        break
                except NoSuchElementException:
                    continue

            if not description:
                try:
                    description = self.driver.find_element(
                        By.TAG_NAME, "main"
                    ).text[:3000]
                except Exception:
                    pass

            sal_min, sal_max = self._extract_salary_from_page()

            return {
                "description":       description,
                "competences":       self._extract_skills(description),
                "salaire_min":       sal_min,
                "salaire_max":       sal_max,
                "niveau_experience": self._extract_experience(description),
            }

        except TimeoutException:
            logger.warning(f"⏱️ Timeout page détail: {url[:60]}")
            return default
        except Exception as e:
            logger.debug(f"Erreur page détail: {e}")
            return default

    def _extract_salary_from_page(self) -> Tuple[Optional[int], Optional[int]]:
        try:
            page_text = self.driver.find_element(By.TAG_NAME, "main").text
            for p in [
                r'(\d[\d\s]+)\s*€?\s*[-–]\s*(\d[\d\s]+)\s*€',
                r'(\d+)[kK]\s*€?\s*[-–]\s*(\d+)[kK]',
                r'(\d[\d\s]+)\s*€\s*/\s*an',
            ]:
                m = re.search(p, page_text)
                if m:
                    n1 = int(re.sub(r'\s', '', m.group(1)))
                    n2 = int(re.sub(r'\s', '', m.group(2))) if len(m.groups()) > 1 else None
                    if n1 < 500:
                        n1 *= 1000
                    if n2 and n2 < 500:
                        n2 *= 1000
                    return n1, n2
        except Exception:
            pass
        return None, None

    # ══════════════════════════════════════════
    # MÉTADONNÉES
    # ══════════════════════════════════════════

    def _extract_skills(self, text: str) -> List[str]:
        return [skill for skill, pattern in self.SKILLS.items()
                if re.search(pattern, text, re.IGNORECASE)]

    def _extract_experience(self, text: str) -> str:
        for lv, p in {
            "Entry":  r"\bjunior\b|\bdébutant\b|\bentry\b|\bsans expérience\b|\b0[-–]2 ans\b",
            "Mid":    r"\bconfirmé\b|\b2[-–]5 ans\b|\bmid\b|\b2 ans\b|\b3 ans\b",
            "Senior": r"\bsenior\b|\b5\+\s*ans\b|\bexpérimenté\b|\b\+5 ans\b",
            "Lead":   r"\blead\b|\bmanager\b|\bprincipal\b|\bresponsable\b",
        }.items():
            if re.search(p, text, re.IGNORECASE):
                return lv
        return "Non spécifié"

    # ══════════════════════════════════════════
    # PIPELINE
    # ══════════════════════════════════════════

    def scrape_keyword(self, keyword: str, max_offers: int = 100,
                       fetch_details: bool = True) -> List[Dict]:
        logger.info(f"\n🌿 ══ {keyword} ══")
        url = self._build_search_url(keyword)

        try:
            self.driver.get(url)
            time.sleep(2)
            self._accept_cookies()
            WebDriverWait(self.driver, self.PAGE_LOAD_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "li"))
            )
        except TimeoutException:
            logger.error(f"❌ Timeout chargement page pour '{keyword}'")
            return []

        cards = self._scroll_to_load_all(max_offers=max_offers)
        logger.info(f"  📋 {len(cards)} cartes trouvées pour '{keyword}'")

        if not cards:
            logger.warning("  ⚠️ Aucune carte trouvée")
            return []

        cards_data = []
        for card in cards[:max_offers]:
            data = self._extract_card_data(card, keyword)
            if data:
                cards_data.append(data)

        logger.info(f"  ✅ {len(cards_data)} cartes parsées")

        offers = []
        for i, card in enumerate(cards_data, 1):
            try:
                if fetch_details and card["_url"]:
                    logger.info(f"  🔍 Détail {i}/{len(cards_data)}: {card['_titre'][:40]}...")
                    detail = self._fetch_offer_detail(card["_url"])
                    time.sleep(0.8)
                else:
                    detail = {
                        "description":       "",
                        "competences":       self._extract_skills(card["_titre"]),
                        "salaire_min":       None,
                        "salaire_max":       None,
                        "niveau_experience": "Non spécifié",
                    }
                offers.append(self._build_offer(card, detail))
                self.stats["total_brut"] += 1
            except Exception as e:
                logger.error(f"  ❌ Erreur offre {i}: {e}")
                self.stats["total_erreurs"] += 1

        return offers

    def _build_offer(self, card: Dict, detail: Dict) -> Dict:
        return {
            "id":                  f"jungle_{card['_id']}",
            "titre":               card["_titre"],
            "entreprise":          card["_entreprise"],
            "lieu":                card["_lieu"],
            "description":         detail.get("description", ""),
            "description_snippet": detail.get("description", "")[:300],
            "competences":         detail.get("competences", []),
            "salaire_min":         detail.get("salaire_min"),
            "salaire_max":         detail.get("salaire_max"),
            "devise_salaire":      "EUR",
            "type_contrat":        card["_contrat"],
            "niveau_experience":   detail.get("niveau_experience", "Non spécifié"),
            "date_publication":    "",
            "url":                 card["_url"],
            "source":              "welcometothejungle",
            "date_scraping":       datetime.now().isoformat(),
            "statut":              "active",
            "query_recherche":     card["_keyword"],
        }

    def scrape_all_data_jobs(self, max_offers_per_kw: int = 100,
                              fetch_details: bool = True) -> List[Dict]:
        seen_ids         = set()
        seen_fingerprint = set()
        all_offers       = []

        self._init_driver()

        try:
            for keyword in self.data_jobs_keywords:
                raw_offers = self.scrape_keyword(
                    keyword,
                    max_offers=max_offers_per_kw,
                    fetch_details=fetch_details
                )
                kw_count = 0
                for offer in raw_offers:
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

                self.stats["offres_par_keyword"][keyword] = kw_count
                logger.info(f"  ✅ {kw_count} offres uniques pour '{keyword}'")
                time.sleep(2)

        finally:
            self._close_driver()

        logger.info(f"\n🎯 Total offres uniques WTTJ: {len(all_offers)}")
        return all_offers

    # ══════════════════════════════════════════
    # SAUVEGARDE BRONZE — ✅ Structure Medallion
    # ══════════════════════════════════════════

    def save_to_json_bronze(self, offers: List[Dict]) -> str:
        """data_lake/bronze/welcometothejungle/YYYY/MM/DD/offres_TIMESTAMP.json"""
        now       = datetime.now()
        date_path = now.strftime("%Y/%m/%d")
        timestamp = now.strftime("%Y%m%d_%H%M%S")

        output_dir = Path("data_lake/bronze/welcometothejungle") / date_path
        output_dir.mkdir(parents=True, exist_ok=True)

        path = output_dir / f"offres_{timestamp}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(offers, f, ensure_ascii=False, indent=2)

        logger.info(f"💾 Bronze JSON : {path} ({len(offers)} offres)")
        return str(path)

    def save_to_minio_bronze(self, offers: List[Dict]):
        """MinIO : bronze/raw/welcometothejungle/YYYY/MM/DD/offres_TIMESTAMP.json"""
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
            object_name = f"raw/welcometothejungle/{date_path}/offres_{timestamp}.json"

            data = json.dumps(offers, ensure_ascii=False, indent=2).encode("utf-8")
            client.put_object(
                self.MINIO_BUCKET_BRONZE, object_name,
                io.BytesIO(data), len(data), "application/json"
            )
            logger.info(f"📦 MinIO Bronze : {self.MINIO_BUCKET_BRONZE}/{object_name}")
        except Exception as e:
            logger.warning(f"⚠️ MinIO Bronze : {e}")

    def save_to_kafka_bronze(self, offers: List[Dict]):
        """Kafka topic : bronze.welcometothejungle.offres"""
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
            lieux[o["lieu"]] += 1

        print("\n" + "=" * 60)
        print("🌿 RÉSUMÉ WELCOMETOTHEJUNGLE v2")
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
        print(f"❌ Erreurs             : {self.stats['total_erreurs']}")
        print(f"\n📁 Sauvegardé dans :")
        print(f"   Local : data_lake/bronze/welcometothejungle/YYYY/MM/DD/")
        print(f"   MinIO : bronze/raw/welcometothejungle/YYYY/MM/DD/")
        print(f"   Kafka : {self.KAFKA_TOPIC_BRONZE}")
        print("=" * 60)

    # ══════════════════════════════════════════
    # POINT D'ENTRÉE
    # ══════════════════════════════════════════

    def run(self, max_offers_per_kw: int = 100, fetch_details: bool = True):
        logger.info("🚀 Démarrage WelcomeToTheJungle Scraper v2")
        t0 = time.time()

        offers = self.scrape_all_data_jobs(
            max_offers_per_kw=max_offers_per_kw,
            fetch_details=fetch_details
        )

        if not offers:
            logger.warning("⚠️ Aucune offre récupérée")
            return []

        # ── Sauvegarde Bronze (structure Medallion) ──
        self.save_to_json_bronze(offers)
        self.save_to_minio_bronze(offers)
        self.save_to_kafka_bronze(offers)

        logger.info(f"✨ Terminé en {time.time()-t0:.1f}s — {len(offers)} offres")
        self._print_summary(offers)
        return offers


if __name__ == "__main__":
    scraper = WelcomeToTheJungleScraper(
        use_kafka=True,
        headless=True,
    )
    results = scraper.run(
        max_offers_per_kw=50,
        fetch_details=True  # True en prod pour avoir les descriptions complètes
    )

    if results:
        print("\n📋 Exemple d'offre normalisée :")
        print(json.dumps(results[0], ensure_ascii=False, indent=2))