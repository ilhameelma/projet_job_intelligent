/**
 * JOB INTELLIGENT - JavaScript Principal
 * Recherche : redirection vers page offres
 * Compétences et offres : depuis API, sans fallback démo
 */

const API_BASE = 'http://localhost:8001';

// ============================================================
// NAVBAR - scroll effect
// ============================================================
window.addEventListener('scroll', () => {
    const navbar = document.getElementById('navbar');
    if (navbar) {
        navbar.classList.toggle('scrolled', window.scrollY > 50);
    }
});

// ============================================================
// RECHERCHE (redirection vers page offres)
// ============================================================
function searchOffers() {
    const q = document.getElementById('searchInput')?.value.trim();
    if (q) {
        window.location.href = `/offers?q=${encodeURIComponent(q)}`;
    }
}

function quickSearch(term) {
    const input = document.getElementById('searchInput');
    if (input) input.value = term;
    searchOffers();
}

document.getElementById('searchInput')?.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') searchOffers();
});

// ============================================================
// CHARGER LES OFFRES RÉCENTES (page accueil)
// ============================================================
async function loadRecentOffers() {
    const container = document.getElementById('offersList');
    if (!container) {
        console.error("Element #offersList introuvable !");
        return;
    }

    try {
        const res = await fetch(`${API_BASE}/api/offers/recent?limit=5`);
        console.log("Réponse API reçue, status", res.status);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        console.log("Données reçues :", data);

        if (!data.offers || data.offers.length === 0) {
            container.innerHTML = '<div class="loading">Aucune offre disponible</div>';
            return;
        }

        container.innerHTML = data.offers.map(offre => renderOfferCard(offre)).join('');
        if (data.total) {
            const totalEl = document.getElementById('totalOffres');
            if (totalEl) totalEl.innerText = formatNumber(data.total);
        }
    } catch (err) {
        console.error('Erreur chargement offres:', err);
        container.innerHTML = '<div class="loading">❌ Erreur API</div>';
    }
}

// ============================================================
// RENDER OFFER CARD
// ============================================================
function renderOfferCard(offre) {
    const initials = getInitials(offre.entreprise || 'JI');
    const score    = offre.score_match || Math.floor(Math.random() * 30 + 65);
    const tags     = (offre.competences || []).slice(0, 4);
    const salaire  = offre.salaire_mensuel ? `${formatNumber(offre.salaire_mensuel)}€ / mois` : 'Non renseigné';
    const source   = offre.source || 'adzuna';
    const url      = offre.url || '#';

    return `
    <a href="${url}" target="_blank" class="offer-card">
        <div class="offer-logo">${initials}</div>
        <div class="offer-info">
            <div class="offer-title">${escapeHtml(offre.titre_clean || offre.titre || 'Offre Data')}</div>
            <div class="offer-meta">
                ${escapeHtml(offre.entreprise || 'Entreprise')} · 
                ${escapeHtml(offre.ville || 'France')} · 
                ${escapeHtml(offre.type_contrat || 'CDI')}
            </div>
            <div class="offer-tags">
                ${tags.map(t => `<span class="offer-tag">${escapeHtml(t)}</span>`).join('')}
            </div>
        </div>
        <div class="offer-right">
            <div class="offer-score">${score}%</div>
            <div class="offer-score-bar">
                <div class="offer-score-fill" style="width:${score}%"></div>
            </div>
            <div class="offer-salary">${salaire}</div>
            <span class="offer-source">${source}</span>
        </div>
    </a>`;
}

// ============================================================
// CHARGER LES COMPÉTENCES DEPUIS L'API (UNIQUEMENT BASE)
// ============================================================
async function loadSkills() {
    const container = document.getElementById('skillsGrid');
    if (!container) return;

    try {
        const res = await fetch(`${API_BASE}/api/skills/top?limit=12`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        if (!data.skills || data.skills.length === 0) {
            container.innerHTML = '<div class="loading">Aucune compétence trouvée</div>';
            return;
        }

        const maxCount = Math.max(...data.skills.map(s => s.count));
        container.innerHTML = data.skills.map(skill => `
            <div class="skill-item">
                <div class="skill-name">${escapeHtml(skill.nom)}</div>
                <div class="skill-count">${skill.count} offres</div>
                <div class="skill-bar">
                    <div class="skill-bar-fill" style="width:${Math.round(skill.count / maxCount * 100)}%"></div>
                </div>
            </div>
        `).join('');
    } catch (err) {
        console.error('Erreur chargement compétences:', err);
        container.innerHTML = '<div class="loading">❌ Impossible de charger les compétences. Vérifiez l’API.</div>';
    }
}


// ============================================================
// UTILITAIRES
// ============================================================
function getInitials(name) {
    if (!name) return 'JI';
    return name.split(' ').slice(0, 2).map(w => w[0]).join('').toUpperCase();
}

function formatNumber(n) {
    return Number(n).toLocaleString('fr-FR');
}

function escapeHtml(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

function updateStat(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
}

// ============================================================
// INIT
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
    loadRecentOffers();
    loadSkills();
});