# adzuna_scraper.py - Alternative plus fiable qu'Indeed
import requests
import json
from datetime import datetime

class AdzunaScraper:
    """Scraper utilisant l'API Adzuna (gratuite pour petits volumes)"""
    
    def __init__(self):
        self.app_id = "votre_app_id"  # Inscription sur https://developer.adzuna.com/
        self.app_key = "votre_app_key"
        self.base_url = "https://api.adzuna.com/v1/api/jobs/fr/search/1"
    
    def scrape_data_jobs(self):
        """Recherche les offres Data"""
        params = {
            'app_id': self.app_id,
            'app_key': self.app_key,
            'what': 'data scientist OR data engineer OR data analyst',
            'where': 'France',
            'results_per_page': 50,
            'content-type': 'application/json'
        }
        
        response = requests.get(self.base_url, params=params)
        data = response.json()
        
        offers = []
        for job in data.get('results', []):
            offers.append({
                "id": f"adzuna_{job.get('id')}",
                "titre": job.get('title'),
                "entreprise": job.get('company', {}).get('display_name'),
                "lieu": job.get('location', {}).get('display_name'),
                "description": job.get('description'),
                "competences": self._extract_skills(job.get('description', '')),
                "salaire_min": job.get('salary_min'),
                "salaire_max": job.get('salary_max'),
                "url": job.get('redirect_url'),
                "source": "adzuna",
                "date_scraping": datetime.now().isoformat()
            })
        
        return offers