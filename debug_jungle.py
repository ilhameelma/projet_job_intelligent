# debug_jungle.py  — à placer à la racine du projet
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time

options = Options()
# ❌ PAS headless — on veut voir ce qui se passe
options.add_argument("--window-size=1920,1080")
options.add_argument("--lang=fr-FR")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

try:
    url = "https://www.welcometothejungle.com/fr/jobs?query=Data+Scientist&page=1"
    driver.get(url)
    print("✅ Page chargée")

    # Accepter cookies si présent
    time.sleep(3)
    try:
        btns = driver.find_elements(By.TAG_NAME, "button")
        for b in btns:
            if "accept" in b.text.lower() or "accepter" in b.text.lower():
                b.click()
                print("🍪 Cookies acceptés")
                break
    except:
        pass

    time.sleep(3)

    # ══════════════════════════════════
    # TEST 1 : Trouver le bon sélecteur de liste
    # ══════════════════════════════════
    print("\n═══ TEST SÉLECTEURS LISTE ═══")
    selectors_to_test = [
        "li[data-testid='search-results-list-item-wrapper']",
        "article[data-testid*='job']",
        "div[data-testid*='job-card']",
        "li[role='listitem']",
        "ul[data-testid*='results'] li",
        "ul[data-testid*='list'] li",
        "div[data-testid*='search'] li",
        "li",  # fallback large
    ]

    for sel in selectors_to_test:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            print(f"  ✅ '{sel}' → {len(els)} éléments")
        else:
            print(f"  ❌ '{sel}' → 0")

    # ══════════════════════════════════
    # TEST 2 : Dump HTML de la 1ère carte
    # ══════════════════════════════════
    print("\n═══ HTML PREMIÈRE CARTE ═══")

    # Prendre le sélecteur qui a fonctionné
    cards = driver.find_elements(By.CSS_SELECTOR, "li[data-testid='search-results-list-item-wrapper']")
    if not cards:
        # Essai avec li générique filtré par taille
        all_li = driver.find_elements(By.TAG_NAME, "li")
        cards = [li for li in all_li if len(li.text) > 20]

    if cards:
        card = cards[0]
        print(f"Texte brut de la carte :\n{card.text}\n")
        print(f"HTML de la carte (premiers 2000 chars) :\n{card.get_attribute('outerHTML')[:2000]}")
    else:
        print("❌ Aucune carte trouvée — dump body entier :")
        print(driver.find_element(By.TAG_NAME, "body").get_attribute("innerHTML")[:3000])

    # ══════════════════════════════════
    # TEST 3 : Trouver titre et lien dans la carte
    # ══════════════════════════════════
    if cards:
        print("\n═══ SÉLECTEURS DANS LA CARTE ═══")
        card = cards[0]
        sub_selectors = ["h3", "h2", "h4", "a", "[data-testid]", "span", "p"]
        for sel in sub_selectors:
            els = card.find_elements(By.CSS_SELECTOR, sel)
            for el in els[:3]:
                txt  = el.text.strip()[:80]
                href = el.get_attribute("href") or ""
                dtid = el.get_attribute("data-testid") or ""
                cls  = el.get_attribute("class") or ""
                if txt or href:
                    print(f"  <{sel}> data-testid='{dtid}' class='{cls[:40]}' → '{txt}' | href='{href[:60]}'")

finally:
    input("\n⏸️  Appuie sur ENTRÉE pour fermer le navigateur...")
    driver.quit()