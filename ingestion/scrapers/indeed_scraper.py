#!/usr/bin/env python3
"""
SCRAPER INDEED - Pour les offres Data en France
Intégration avec Kafka, Minio et Airflow
"""

import json
import logging
import time
import hashlib
import requests
from datetime import datetime
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote_plus
import re

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IndeedScraper:
    """Scraper pour Indeed France"""
    
    def __init__(self, use_kafka: bool = True):
        self.source_name = "indeed"
        self.kafka_topic = "indeed-offres"
        self.base_url = "https://fr.indeed.com"
        self.search_url = "https://fr.indeed.com/jobs"
        self.use_kafka = use_kafka
        
        # Headers pour éviter le blocage
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'fr,fr-FR;q=0.8,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # Mots-clés pour les métiers de la Data
        self.data_jobs_keywords = [
            "Data Scientist",
            "Data Engineer", 
            "Data Analyst",
            "Data Architect",
            "Machine Learning Engineer",
            "Business Intelligence",
            "Data Consultant",
            "Analytics Engineer",
            "Data Manager",
            "Dataops",
            "MLOps",
            "Data Product Manager"
        ]
        
        self.producer = None
        if use_kafka:
            self._init_kafka_producer()
    
    def _init_kafka_producer(self):
        """Initialise le producteur Kafka"""
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers='kafka:29092',
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                max_request_size=10485760,  # 10MB
                compression_type='gzip'
            )
            logger.info("✅ Producteur Kafka initialisé pour Indeed")
        except Exception as e:
            logger.warning(f"⚠️ Kafka non disponible: {e}")
            self.use_kafka = False
    
    def _extract_skills(self, title: str, description: str) -> List[str]:
        """Extrait les compétences techniques de l'offre"""
        skills_keywords = {
            "Langages": ["Python", "Java", "Scala", "R", "SQL", "NoSQL", "Julia", "C++"],
            "Big Data": ["Spark", "Hadoop", "Hive", "Kafka", "Flink", "Storm", "Airflow", "Beam"],
            "ML/AI": ["TensorFlow", "PyTorch", "Scikit-learn", "Keras", "XGBoost", "LightGBM"],
            "Data Viz": ["Tableau", "Power BI", "Looker", "Qlik", "Superset", "Metabase"],
            "Cloud": ["AWS", "Azure", "GCP", "Databricks", "Snowflake", "Redshift", "BigQuery"],
            "DevOps": ["Docker", "Kubernetes", "Jenkins", "Git", "CI/CD", "Terraform"],
            "Databases": ["PostgreSQL", "MySQL", "MongoDB", "Cassandra", "Elasticsearch"]
        }
        
        found_skills = []
        text = f"{title} {description}".lower()
        
        for category, skills in skills_keywords.items():
            for skill in skills:
                if skill.lower() in text:
                    found_skills.append(skill)
        
        return list(set(found_skills))  # Deduplication
    
    def _extract_salary(self, description: str) -> Dict:
        """Extrait les informations de salaire"""
        salary_patterns = [
            r'(\d{4,6})\s*[-–]\s*(\d{4,6})\s*€',
            r'(\d{4,6})\s*€\s*[-–]\s*(\d{4,6})\s*€',
            r'de\s+(\d{4,6})\s*€\s+à\s+(\d{4,6})\s*€',
            r'entre\s+(\d{4,6})\s*et\s+(\d{4,6})\s*€',
            r'(\d{4,6})\s*€\s*(?:brut|annuel)'
        ]
        
        for pattern in salary_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                if len(match.groups()) == 2:
                    return {
                        "min": int(match.group(1)),
                        "max": int(match.group(2)),
                        "currency": "EUR"
                    }
                elif len(match.groups()) == 1:
                    salary = int(match.group(1))
                    return {
                        "min": salary,
                        "max": salary,
                        "currency": "EUR"
                    }
        
        return {"min": None, "max": None, "currency": "EUR"}
    
    def _extract_contract_type(self, description: str) -> str:
        """Détermine le type de contrat"""
        contract_patterns = {
            "CDI": r'\bCDI\b|\bContrat à durée indéterminée\b',
            "CDD": r'\bCDD\b|\bContrat à durée déterminée\b',
            "Freelance": r'\bfreelance\b|\bindépendant\b|\bportage salarial\b',
            "Stage": r'\bstage\b|\balternance\b|\bapprentissage\b',
            "Intérim": r'\bintérim\b|\btemporary\b'
        }
        
        for contract, pattern in contract_patterns.items():
            if re.search(pattern, description, re.IGNORECASE):
                return contract
        
        return "Non spécifié"
    
    def _extract_experience_level(self, description: str) -> str:
        """Détermine le niveau d'expérience requis"""
        exp_patterns = {
            "Entry": r'\bjunior\b|\bmoins de 2 ans\b|\bdébutant\b',
            "Mid": r'\b2-5 ans\b|\bconfirmé\b|\b3-5 ans\b',
            "Senior": r'\bsenior\b|\b5-10 ans\b|\bexpérimenté\b',
            "Lead": r'\blead\b|\bmanager\b|\b10 ans\b|\bprincipal\b'
        }
        
        for level, pattern in exp_patterns.items():
            if re.search(pattern, description, re.IGNORECASE):
                return level
        
        return "Non spécifié"
    
    def scrape_search_page(self, query: str, location: str = "France", page: int = 0) -> List[Dict]:
        """Scrape une page de résultats Indeed"""
        params = {
            "q": query,
            "l": location,
            "start": page * 10,  # Indeed utilise offset de 10
            "sort": "date",  # Tri par date
            "filter": 0  # Pas de filtre automatique
        }
        
        logger.info(f"🔍 Recherche: {query} - Page {page + 1}")
        
        try:
            response = requests.get(
                self.search_url, 
                params=params, 
                headers=self.headers,
                timeout=15
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            job_cards = soup.find_all('div', class_='job_seen_beacon')
            
            if not job_cards:
                # Fallback selector pour Indeed
                job_cards = soup.find_all('div', {'data-testid': 'job-card'})
            
            jobs = []
            for card in job_cards:
                try:
                    job = self._extract_job_from_card(card, soup)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.error(f"Erreur extraction carte: {e}")
                    continue
            
            logger.info(f"✅ {len(jobs)} offres trouvées sur cette page")
            return jobs
            
        except requests.RequestException as e:
            logger.error(f"❌ Erreur requête: {e}")
            return []
    
    def _extract_job_from_card(self, card, soup) -> Optional[Dict]:
        """Extrait les données d'une carte d'offre"""
        try:
            # Titre
            title_elem = card.find('h2', class_='jobTitle') or card.find('a', class_='jcs-JobTitle')
            if not title_elem:
                return None
            
            title = title_elem.text.strip()
            
            # URL
            url_elem = title_elem.find('a')
            if url_elem and url_elem.get('href'):
                relative_url = url_elem['href']
                url = urljoin(self.base_url, relative_url)
            else:
                url = None
            
            # Entreprise
            company_elem = card.find('span', {'data-testid': 'company-name'}) or \
                          card.find('span', class_='companyName')
            company = company_elem.text.strip() if company_elem else "Non spécifié"
            
            # Lieu
            location_elem = card.find('div', {'data-testid': 'text-location'}) or \
                           card.find('div', class_='companyLocation')
            location = location_elem.text.strip() if location_elem else "France"
            
            # Date de publication
            date_elem = card.find('span', {'data-testid': 'myJobsStateDate'}) or \
                       card.find('span', class_='date')
            date_posted = date_elem.text.strip() if date_elem else None
            
            # Description (extrait)
            desc_elem = card.find('div', {'data-testid': 'job-snippet'}) or \
                       card.find('div', class_='job-snippet')
            description_snippet = desc_elem.text.strip() if desc_elem else ""
            
            # Récupérer la description complète si possible
            full_description = self._get_full_description(url) if url else description_snippet
            
            # Générer un ID unique
            job_id = hashlib.md5(f"{title}_{company}_{location}".encode()).hexdigest()
            
            # Extraire les métadonnées
            skills = self._extract_skills(title, full_description)
            salary = self._extract_salary(full_description)
            contract_type = self._extract_contract_type(full_description)
            experience_level = self._extract_experience_level(full_description)
            
            return {
                "id": f"indeed_{job_id}",
                "titre": title,
                "entreprise": company,
                "lieu": location,
                "description": full_description or description_snippet,
                "description_snippet": description_snippet,
                "competences": skills,
                "salaire_min": salary["min"],
                "salaire_max": salary["max"],
                "devise_salaire": salary["currency"],
                "type_contrat": contract_type,
                "niveau_experience": experience_level,
                "date_publication": date_posted,
                "url": url,
                "source": "indeed",
                "date_scraping": datetime.now().isoformat(),
                "statut": "active",
                "query_recherche": None
            }
            
        except Exception as e:
            logger.error(f"Erreur extraction: {e}")
            return None
    
    def _get_full_description(self, url: str) -> str:
        """Récupère la description complète d'une offre"""
        if not url:
            return ""
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Différents sélecteurs possibles pour la description
            desc_selectors = [
                ('div', {'id': 'jobDescriptionText'}),
                ('div', {'class': 'jobsearch-jobDescriptionText'}),
                ('div', {'data-testid': 'jobDescriptionText'}),
                ('div', {'class': 'description'})
            ]
            
            for tag, attrs in desc_selectors:
                desc_elem = soup.find(tag, attrs)
                if desc_elem:
                    # Nettoyer le texte
                    for unwanted in desc_elem.find_all(['script', 'style']):
                        unwanted.decompose()
                    return ' '.join(desc_elem.stripped_strings)
            
            return ""
            
        except Exception as e:
            logger.error(f"Erreur récupération description {url}: {e}")
            return ""
    
    def scrape_all_data_jobs(self, max_pages_per_query: int = 5) -> List[Dict]:
        """Scrape toutes les offres Data"""
        all_jobs = []
        
        for keyword in self.data_jobs_keywords:
            logger.info(f"📊 Scraping: {keyword}")
            
            for page in range(max_pages_per_query):
                jobs = self.scrape_search_page(keyword, page=page)
                
                if not jobs:
                    break
                
                # Ajouter le mot-clé utilisé
                for job in jobs:
                    job["query_recherche"] = keyword
                
                all_jobs.extend(jobs)
                
                # Pause pour éviter le blocage
                time.sleep(2)
            
            # Pause entre les requêtes
            time.sleep(3)
        
        # Supprimer les doublons basés sur le titre + entreprise
        unique_jobs = {}
        for job in all_jobs:
            key = f"{job['titre']}_{job['entreprise']}"
            if key not in unique_jobs:
                unique_jobs[key] = job
        
        logger.info(f"🎯 Total offres uniques: {len(unique_jobs)}")
        return list(unique_jobs.values())
    
    def save_to_kafka(self, offers: List[Dict]):
        """Envoie les offres vers Kafka"""
        if not self.use_kafka or not self.producer:
            logger.warning("⚠️ Kafka non disponible, sauvegarde locale uniquement")
            return
        
        success_count = 0
        for offer in offers:
            try:
                future = self.producer.send(self.kafka_topic, value=offer)
                future.get(timeout=10)
                success_count += 1
            except Exception as e:
                logger.error(f"❌ Erreur envoi Kafka: {e}")
        
        self.producer.flush()
        logger.info(f"✅ {success_count}/{len(offers)} offres envoyées à Kafka (topic: {self.kafka_topic})")
    
    def save_to_json(self, offers: List[Dict], output_dir: str = "/opt/airflow/scrapers/data"):
        """Sauvegarde en JSON local"""
        import os
        
        # Créer le dossier si nécessaire
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{output_dir}/indeed_offres_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(offers, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Sauvegarde JSON: {filename}")
        return filename
    
    def save_to_minio(self, offers: List[Dict]):
        """Sauvegarde directement dans Minio"""
        try:
            from minio import Minio
            
            client = Minio(
                'minio:9000',
                access_key='minio_admin',
                secret_key='minio_password',
                secure=False
            )
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"indeed_offres_{timestamp}.json"
            
            # Convertir en JSON
            data = json.dumps(offers, ensure_ascii=False, indent=2).encode('utf-8')
            
            # Envoyer vers Minio bucket bronze
            client.put_object(
                'bronze',
                f"indeed/{filename}",
                data=bytes(data),
                length=len(data),
                content_type='application/json'
            )
            
            logger.info(f"📦 Sauvegarde Minio: bronze/indeed/{filename}")
            
        except Exception as e:
            logger.warning(f"⚠️ Minio non disponible: {e}")
    
    def run(self):
        """Exécute le scraping complet"""
        logger.info("🚀 Démarrage du scraper Indeed")
        start_time = time.time()
        
        # Scraper les offres
        offers = self.scrape_all_data_jobs(max_pages_per_query=3)
        
        if not offers:
            logger.warning("⚠️ Aucune offre trouvée")
            return []
        
        # Envoyer vers Kafka
        self.save_to_kafka(offers)
        
        # Sauvegarde JSON locale
        json_file = self.save_to_json(offers)
        
        # Sauvegarde Minio
        self.save_to_minio(offers)
        
        elapsed_time = time.time() - start_time
        logger.info(f"✨ Scraping terminé en {elapsed_time:.2f} secondes")
        logger.info(f"📊 Statistiques: {len(offers)} offres uniques")
        
        # Afficher un résumé
        self._print_summary(offers)
        
        return offers
    
    def _print_summary(self, offers: List[Dict]):
        """Affiche un résumé des offres trouvées"""
        from collections import Counter
        
        companies = Counter([offer['entreprise'] for offer in offers])
        top_companies = companies.most_common(5)
        
        skills = Counter()
        for offer in offers:
            skills.update(offer['competences'])
        top_skills = skills.most_common(10)
        
        contract_types = Counter([offer['type_contrat'] for offer in offers])
        
        print("\n" + "="*60)
        print("📊 RÉSUMÉ DES OFFRES SCRAPÉES")
        print("="*60)
        print(f"\n🏢 Top 5 entreprises recruteuses:")
        for company, count in top_companies:
            print(f"   - {company}: {count} offres")
        
        print(f"\n🔧 Top 10 compétences demandées:")
        for skill, count in top_skills:
            print(f"   - {skill}: {count} offres")
        
        print(f"\n📄 Types de contrat:")
        for contract, count in contract_types.most_common():
            print(f"   - {contract}: {count} offres")
        
        print(f"\n💰 Offres avec salaire mentionné: {sum(1 for o in offers if o['salaire_min'])}")
        print("="*60)


# Point d'entrée pour exécution directe
if __name__ == "__main__":
    scraper = IndeedScraper(use_kafka=True)
    results = scraper.run()
    
    # Afficher un exemple d'offre
    if results:
        print("\n📋 Exemple d'offre:")
        print(json.dumps(results[0], ensure_ascii=False, indent=2))