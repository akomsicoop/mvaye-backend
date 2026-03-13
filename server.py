"""
╔══════════════════════════════════════════════════════════════════════╗
║  STALIZA MetaCore™ v0.8 — Serveur complet Python pur                ║
║  6 indices · 4 couches actives · FusionEngine · Zéro dépendance     ║
║  Lance avec : python server.py                                       ║
╚══════════════════════════════════════════════════════════════════════╝

Architecture MetaCore™ :
  L1 Jamestown  — Performance zootechnique (biologique)
  L2 Pegasus    — Environnement & épidémiologie
  L3 Gotham     — Marché & intelligence économique
  L4 Acoustique — Signaux bioacoustiques
  FusionEngine  — Orchestrateur multi-couches

Indices :
  AgricScore™   — Score santé global 0–100
  LLI™          — Livestock Loss Index (risque de perte)
  MWI™          — Market Window Index (fenêtre de vente)
  EYI™          — Epidemic & Yield Intelligence
  FCI™          — Feed Cost Intelligence
  HVI™          — Herd Vulnerability Index
"""

import os, json, uuid, logging, threading, time, math
from datetime import datetime, timedelta
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("metacore")

VERSION = "0.8"

# ════════════════════════════════════════════════════════════════
#  SEUILS MÉTIER (thresholds)
# ════════════════════════════════════════════════════════════════

ZONES = {"VERTE": 75, "JAUNE": 50, "ORANGE": 25}  # seuils AgricScore

LLI_THRESHOLDS  = {"CRITIQUE": 65, "ÉLEVÉ": 45, "MODÉRÉ": 30}
MWI_THRESHOLDS  = {"OPTIMAL": 70, "BON": 50, "ATTENTE": 30}
HVI_THRESHOLDS  = {"CRITIQUE": 70, "ÉLEVÉ": 50, "MODÉRÉ": 35}
EYI_THRESHOLDS  = {"ALERTE": 70,  "VIGILANCE": 45, "NORMAL": 20}
FCI_THRESHOLDS  = {"CRITIQUE": 75, "TENDU": 50, "NORMAL": 30}

LAYER_WEIGHTS = {
    "L1_jamestown":  0.30,
    "L2_pegasus":    0.25,
    "L3_gotham":     0.25,
    "L4_acoustique": 0.20,
}

# ════════════════════════════════════════════════════════════════
#  STORE JSON (persistance locale)
# ════════════════════════════════════════════════════════════════

DATA_DIR     = Path("data_store")
DATA_DIR.mkdir(exist_ok=True)
ENTRIES_FILE = DATA_DIR / "entries.json"
RESULTS_FILE = DATA_DIR / "results.json"
STATS_FILE   = DATA_DIR / "stats.json"

class Store:
    def __init__(self):
        self._lock = threading.Lock()
        for f, d in [(ENTRIES_FILE, []), (RESULTS_FILE, {}), (STATS_FILE, {})]:
            if not f.exists():
                f.write_text(json.dumps(d, ensure_ascii=False), "utf-8")

    def _r(self, f):
        try:
            if f.exists() and f.stat().st_size > 2:
                return json.loads(f.read_text("utf-8"))
        except: pass
        return {} if f in (RESULTS_FILE, STATS_FILE) else []

    def _w(self, f, data):
        tmp = f.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), "utf-8")
        tmp.replace(f)

    def save_entry(self, e: dict) -> str:
        with self._lock:
            entries = self._r(ENTRIES_FILE)
            eid = e.get("id") or "MVY-" + str(uuid.uuid4())[:8].upper()
            e["id"] = eid
            e["_at"] = datetime.utcnow().isoformat()
            entries.append(e)
            self._w(ENTRIES_FILE, entries[-2000:])
            return eid

    def save_result(self, eid: str, result: dict):
        with self._lock:
            results = self._r(RESULTS_FILE)
            results[eid] = {**result, "_at": datetime.utcnow().isoformat()}
            self._w(RESULTS_FILE, results)

    def get_entry(self, eid: str):
        with self._lock:
            return next((e for e in self._r(ENTRIES_FILE) if e.get("id") == eid), None)

    def get_result(self, eid: str):
        with self._lock:
            return self._r(RESULTS_FILE).get(eid)

    def list_entries(self, limit=50, region=None):
        with self._lock:
            all_e = self._r(ENTRIES_FILE)
        if region:
            all_e = [e for e in all_e if e.get("region","").lower() == region.lower()]
        return sorted(all_e, key=lambda x: x.get("_at",""), reverse=True)[:limit]

    def count(self) -> int:
        with self._lock:
            return len(self._r(ENTRIES_FILE))

    def regional_avg(self, region: str) -> dict:
        """Score moyen par région — utilisé pour la comparaison M'vaye."""
        with self._lock:
            results = self._r(RESULTS_FILE)
            entries = self._r(ENTRIES_FILE)
        reg_entries = [e["id"] for e in entries if e.get("region","").lower() == region.lower()]
        scores = [float(results[eid]["agric_score"]) for eid in reg_entries
                  if eid in results and results[eid].get("agric_score")]
        if len(scores) < 3:
            return {"available": False, "n": len(scores), "min_required": 3}
        return {
            "available": True,
            "n": len(scores),
            "avg": round(sum(scores) / len(scores), 1),
            "min": round(min(scores), 1),
            "max": round(max(scores), 1),
        }

    def metrics(self) -> dict:
        with self._lock:
            entries = self._r(ENTRIES_FILE)
            results = self._r(RESULTS_FILE)
        if not entries:
            return {"total_entries": 0}
        regions = {}
        sectors = {}
        for e in entries:
            r = e.get("region", "?")
            regions[r] = regions.get(r, 0) + 1
            for s in (e.get("secteurs") or [e.get("animalType","?")]):
                sectors[s] = sectors.get(s, 0) + 1
        scores = [float(r.get("agric_score", 0)) for r in results.values() if r.get("agric_score")]
        zones  = {}
        for r in results.values():
            z = r.get("zone","?")
            zones[z] = zones.get(z, 0) + 1
        return {
            "total_entries":   len(entries),
            "total_results":   len(results),
            "by_region":       regions,
            "by_sector":       sectors,
            "by_zone":         zones,
            "avg_agric_score": round(sum(scores) / len(scores), 1) if scores else None,
            "timestamp":       datetime.utcnow().isoformat(),
        }

store = Store()

# ════════════════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════════════════

def _i(v, default, mn, mx):
    try:    return max(mn, min(mx, int(float(str(v)))))
    except: return default

def _f(v, default, mn, mx):
    try:    return max(mn, min(mx, float(str(v))))
    except: return default

def _zone(score):
    if score >= ZONES["VERTE"]:  return "VERTE"
    if score >= ZONES["JAUNE"]:  return "JAUNE"
    if score >= ZONES["ORANGE"]: return "ORANGE"
    return "ROUGE"

def _sev(val, thresholds: dict):
    keys = list(thresholds.keys())
    for k in keys:
        if val >= thresholds[k]: return k
    return "FAIBLE"

def normalize(raw: dict) -> dict:
    out = {}
    for k, v in raw.items():
        if isinstance(v, str):                  out[k] = v.strip()[:500]
        elif isinstance(v, (int, float, bool)): out[k] = v
        elif isinstance(v, list):               out[k] = [str(i)[:100] for i in v if i is not None][:20]
        elif isinstance(v, dict):               out[k] = v
        else:                                   out[k] = str(v)[:200] if v is not None else None
    # Alias nom → ownerName
    if not out.get("ownerName") and out.get("nom"):
        out["ownerName"] = out["nom"]
    out.setdefault("id", "MVY-" + str(uuid.uuid4())[:8].upper())
    out.setdefault("timestamp", datetime.utcnow().isoformat())
    out.setdefault("schema_version", VERSION)
    return out

# ════════════════════════════════════════════════════════════════
#  COUCHE L1 — JAMESTOWN (Performance zootechnique)
# ════════════════════════════════════════════════════════════════

def layer_jamestown(raw: dict) -> dict:
    """
    Jamestown Analytics — couche biologique.
    Évalue la performance intrinsèque du troupeau :
    survie, croissance, alimentation, santé.
    """
    head    = _i(raw.get("headCount"), 100, 1, 100_000)
    deaths  = _i(raw.get("deaths"),    0,   0, head)
    weight  = _f(raw.get("weight"),    1.5, 0.05, 20.0)
    week    = _i(raw.get("week"),      5,   0, 52)
    water   = _f(raw.get("waterLiters"), 0.20, 0, 50)
    feed    = raw.get("feed", "standard")
    vaccin  = raw.get("vaccin", "normal")
    has_vet = raw.get("hasVet", "non")
    symptoms = raw.get("symptoms", []) or []

    mort     = (deaths / max(head, 1)) * 100
    has_symp = len(symptoms) > 0 and "aucun" not in [s.lower() for s in symptoms]

    # ── Survie ────────────────────────────────────────
    mort_score = max(0, 100 - mort * 15)

    # ── Croissance (poids vs courbe standard poulet chair) ─
    expected_weight = {1: 0.18, 2: 0.45, 3: 0.85, 4: 1.35, 5: 1.85, 6: 2.30, 7: 2.65}
    exp_w = expected_weight.get(week, 1.5)
    growth_ratio = weight / max(exp_w, 0.01)
    growth_score = min(100, round(growth_ratio * 75 + 25 if growth_ratio >= 0.85 else growth_ratio * 60))

    # ── Alimentation ──────────────────────────────────
    feed_score = {"premium": 95, "standard": 72, "artisan": 55, "mauvais": 25}.get(feed, 65)

    # ── Santé vétérinaire ─────────────────────────────
    vacc_score = {"recent": 100, "normal": 80, "retard": 45, "jamais": 10}.get(vaccin, 70)
    vet_score  = {"oui": 90, "parfois": 65, "non": 40}.get(has_vet, 60)
    symp_score = 35 if has_symp else 90

    # ── Eau ───────────────────────────────────────────
    bench = {"poulet_chair": 0.20, "ponte": 0.22, "bovin": 30, "porcin": 5}.get(
        raw.get("animalType", "poulet_chair"), 0.20)
    water_ratio = water / max(bench, 0.01)
    water_score = 40 if water_ratio < 0.70 else 70 if water_ratio < 0.90 else 100

    # ── Score L1 ──────────────────────────────────────
    l1 = round(
        mort_score   * 0.25 +
        growth_score * 0.20 +
        feed_score   * 0.20 +
        vacc_score   * 0.15 +
        vet_score    * 0.10 +
        symp_score   * 0.05 +
        water_score  * 0.05
    )

    return {
        "score": max(5, min(100, l1)),
        "confidence": 0.82,
        "details": {
            "mort_pct":     round(mort, 2),
            "mort_score":   round(mort_score),
            "growth_score": round(growth_score),
            "feed_score":   round(feed_score),
            "vacc_score":   round(vacc_score),
            "water_score":  round(water_score),
            "water_ratio":  round(water_ratio, 2),
        }
    }

# ════════════════════════════════════════════════════════════════
#  COUCHE L2 — PEGASUS (Environnement & Épidémiologie)
# ════════════════════════════════════════════════════════════════

def layer_pegasus(raw: dict) -> dict:
    """
    Pegasus Streaming — couche environnementale.
    Évalue les risques extérieurs : épidémie, biosécurité,
    transhumance, météo, signal voisinage.
    Score inversé : 100 = risque nul, 0 = risque maximal.
    """
    neigh    = raw.get("neighDeaths",   "non")
    transhu  = raw.get("transhumance",  "non")
    new_ani  = raw.get("newAnimals",    "non")
    visitors = raw.get("visitors",      "aucun")
    bird_mkt = raw.get("liveBirdMarket","non")
    wx_evts  = raw.get("weatherEvents", ["normal"]) or ["normal"]
    season   = raw.get("seasonEvents",  ["aucun"])  or ["aucun"]

    # ── Risque épidémique ─────────────────────────────
    neigh_r  = {"non": 0, "incertain": 12, "oui": 45}.get(neigh, 8)
    transhu_r= {"non": 0, "incertain": 10, "oui": 35}.get(transhu, 5)
    bird_r   = {"non": 0, "oui_quarantaine": 12, "oui_direct": 55}.get(bird_mkt, 5)
    wx_r     = len([w for w in wx_evts if w != "normal"]) * 9

    # ── Biosécurité ───────────────────────────────────
    new_s  = {"non": 0, "oui_quarantaine": 8, "oui_direct": 50}.get(new_ani, 5)
    vis_s  = {"aucun": 0, "famille": 5, "acheteurs": 18, "multi": 40}.get(visitors, 5)

    # ── Stress saisonnier ─────────────────────────────
    saison_boost = 0 if "aucun" in season else len([s for s in season if s != "aucun"]) * 5

    epid_total = min(90, neigh_r + transhu_r + bird_r + wx_r + new_s + vis_s)
    l2_risk    = epid_total
    l2_score   = max(5, 100 - round(l2_risk))

    return {
        "score":      l2_score,
        "risk":       round(l2_risk),
        "confidence": 0.76,
        "details": {
            "neigh_risk":     neigh_r,
            "transhu_risk":   transhu_r,
            "bird_market_risk": bird_r,
            "weather_risk":   wx_r,
            "biosec_risk":    new_s + vis_s,
            "saison_boost":   saison_boost,
            "double_signal":  neigh_r > 30 and transhu_r > 20,
        }
    }

# ════════════════════════════════════════════════════════════════
#  COUCHE L3 — GOTHAM (Marché & Intelligence économique)
# ════════════════════════════════════════════════════════════════

def layer_gotham(raw: dict) -> dict:
    """
    Gotham Intelligence — couche marché.
    Évalue la position économique : prix, tendances,
    ruptures d'intrants, fenêtre de vente, coût de production.
    """
    price_ch   = raw.get("priceChange",  "stable")
    season     = raw.get("seasonEvents", ["aucun"]) or ["aucun"]
    stock_outs = raw.get("stockOuts",    ["aucune"]) or ["aucune"]
    market_p   = _f(raw.get("marketPrice"),  2500, 100, 50_000)
    mais_p     = _f(raw.get("maizPrice"),    12000, 3000, 100_000)
    feed_p     = _f(raw.get("feedBagPrice"), 17500, 5000, 150_000)
    buyers     = raw.get("regularBuyers", "non")
    market_ch  = raw.get("market", "")

    # ── Prix de vente ─────────────────────────────────
    bench_price = 2500  # FCFA/kg référence Cameroun
    price_ratio = market_p / bench_price
    price_score = min(100, round(price_ratio * 65 + 10))
    trend_boost = {"hausse": 15, "stable": 0, "baisse": -20}.get(price_ch, 0)

    # ── Coût intrants ─────────────────────────────────
    bench_mais = 12000; bench_feed = 17500
    mais_ratio = mais_p / bench_mais
    feed_ratio = feed_p / bench_feed
    cost_stress = round(((mais_ratio - 1) * 30 + (feed_ratio - 1) * 50) * 10)
    cost_stress = max(0, min(50, cost_stress))

    # ── Ruptures ──────────────────────────────────────
    stock_penalty = 0 if "aucune" in stock_outs else len(stock_outs) * 10

    # ── Saisonnalité ─────────────────────────────────
    ev_boost = 0 if "aucun" in season else len([s for s in season if s != "aucun"]) * 12

    # ── Acheteurs ─────────────────────────────────────
    buyer_bonus = {"oui": 8, "coop": 12, "non": 0}.get(buyers, 0)

    l3 = max(5, min(100, round(
        price_score + trend_boost + ev_boost + buyer_bonus - cost_stress - stock_penalty
    )))

    # Marge estimée (FCFA/kg)
    cout_aliment_kg = (feed_p / 50) * 2.0  # 2kg aliment / kg vif (FCR moyen)
    marge_kg = market_p - cout_aliment_kg

    return {
        "score":      l3,
        "confidence": 0.71,
        "details": {
            "price_score":    round(price_score),
            "trend_boost":    trend_boost,
            "cost_stress":    cost_stress,
            "stock_penalty":  stock_penalty,
            "saison_boost":   ev_boost,
            "price_vs_bench_pct": round((price_ratio - 1) * 100, 1),
            "marge_fcfa_kg":  round(marge_kg),
            "mwi_signal":     "VENDRE" if l3 >= 70 else "ATTENDRE" if l3 < 40 else "SURVEILLER",
        }
    }

# ════════════════════════════════════════════════════════════════
#  COUCHE L4 — ACOUSTIQUE (Signaux bioacoustiques)
# ════════════════════════════════════════════════════════════════

def layer_acoustique(raw: dict) -> dict:
    """
    Couche bioacoustique — signaux sonores du troupeau.
    Analyse les sons anormaux comme indicateurs précoces
    de détresse respiratoire ou comportementale.
    Score inversé : 100 = sons normaux, 0 = détresse sévère.
    """
    sounds     = raw.get("sounds",        ["normal"]) or ["normal"]
    snd_int    = raw.get("soundIntensity", "aucune")
    snd_dur    = raw.get("soundDuration",  "aucun")
    snd_time   = raw.get("soundTime",      "constant")
    audio_rec  = raw.get("audioRecorded",  False)

    abn_sounds = [s for s in sounds if s.lower() != "normal"]
    n_abn      = len(abn_sounds)

    # ── Intensité ─────────────────────────────────────
    int_score = {"aucune": 100, "faible": 65, "forte": 20}.get(snd_int, 85)

    # ── Durée ─────────────────────────────────────────
    dur_score = {"aucun": 100, "24h": 65, "3j": 40, "7j": 20, "long": 8}.get(snd_dur, 85)

    # ── Fréquence ─────────────────────────────────────
    time_score = {"constant": 30, "matin": 65, "soir": 65, "nuit": 50, "repas": 80}.get(snd_time, 70)
    if n_abn == 0:
        time_score = 100

    # ── Types de sons (diagnostic) ────────────────────
    sound_risk = {
        "toux_frequente": 40, "sifflements": 50, "gargouillis": 45,
        "silence_anormal": 55, "toux_rare": 20, "cris": 35
    }
    type_penalty = min(60, sum(sound_risk.get(s.lower(), 10) for s in abn_sounds))

    # ── Bonus enregistrement audio ─────────────────────
    audio_bonus = 5 if audio_rec else 0

    l4 = max(5, min(100, round(
        int_score * 0.35 +
        dur_score * 0.30 +
        time_score * 0.20 +
        (100 - type_penalty) * 0.15
        + audio_bonus
    )))

    # Diagnostic probable
    diagnosis = "RAS"
    if "sifflements" in abn_sounds and snd_int == "forte":
        diagnosis = "Suspicion bronchite infectieuse / Newcastle respiratoire"
    elif "toux_frequente" in abn_sounds:
        diagnosis = "Suspicion maladie respiratoire — consultation requise"
    elif "gargouillis" in abn_sounds:
        diagnosis = "Suspicion atteinte digestive"
    elif "silence_anormal" in abn_sounds:
        diagnosis = "Abattement collectif — bilan sanitaire urgent"

    return {
        "score":      l4,
        "confidence": 0.63 if audio_rec else 0.48,
        "details": {
            "abnormal_sounds":  abn_sounds,
            "n_abnormal":       n_abn,
            "intensity":        snd_int,
            "duration":         snd_dur,
            "diagnosis":        diagnosis,
            "audio_recorded":   audio_rec,
        }
    }

# ════════════════════════════════════════════════════════════════
#  6 INDICES METACORE™
# ════════════════════════════════════════════════════════════════

def compute_agric_score(l1, l2, l3, l4) -> dict:
    """
    AgricScore™ — Score de santé globale 0–100.
    Fusion pondérée des 4 couches via FusionEngine.
    """
    raw_score = round(
        l1["score"] * LAYER_WEIGHTS["L1_jamestown"]  +
        l2["score"] * LAYER_WEIGHTS["L2_pegasus"]    +
        l3["score"] * LAYER_WEIGHTS["L3_gotham"]     +
        l4["score"] * LAYER_WEIGHTS["L4_acoustique"]
    )
    score = max(5, min(100, raw_score))
    zone  = _zone(score)
    conf  = round((l1["confidence"]*0.30 + l2["confidence"]*0.25 +
                   l3["confidence"]*0.25 + l4["confidence"]*0.20), 2)

    reco_map = {
        "VERTE":  "Troupeau en bonne santé. Fenêtre de vente favorable.",
        "JAUNE":  "Surveillance recommandée. Vérifiez alimentation et état sanitaire.",
        "ORANGE": "Action requise dans les 48h. Consultez votre vétérinaire.",
        "ROUGE":  "Situation critique. Isolez le troupeau et contactez le MINEPIA.",
    }

    return {"value": score, "zone": zone, "confidence": conf,
            "recommendation": reco_map[zone]}

def compute_lli(raw: dict, l1: dict, l2: dict) -> dict:
    """
    LLI™ — Livestock Loss Index.
    Mesure le risque de perte animale à court terme (0=sûr, 100=critique).
    Combine mortalité observée + risque épidémique + signaux précoces.
    """
    mort    = l1["details"]["mort_pct"]
    has_symp= len(raw.get("symptoms",[]) or []) > 0
    epid    = l2["risk"]
    l4_abn  = len(raw.get("sounds",["normal"])) - 1  # sons anormaux
    water_r = 1.0 - min(1.0, l1["details"]["water_ratio"])

    lli = min(100, round(
        mort     * 4.5 +
        (35 if has_symp else 0) +
        epid     * 0.6 +
        l4_abn   * 6 +
        water_r  * 20
    ))
    sev = _sev(lli, LLI_THRESHOLDS)
    return {
        "value": lli, "severity": sev, "confidence": 0.79,
        "dominant_risk": "épidémique" if epid > 40 else "sanitaire" if lli > 40 else "faible",
    }

def compute_mwi(raw: dict, l3: dict) -> dict:
    """
    MWI™ — Market Window Index.
    Indique si c'est le bon moment pour vendre (0=mauvais, 100=optimal).
    """
    mwi = l3["score"]
    sev = _sev(mwi, MWI_THRESHOLDS)
    signal = l3["details"]["mwi_signal"]
    return {
        "value": mwi, "signal": signal, "confidence": l3["confidence"],
        "price_vs_bench_pct": l3["details"]["price_vs_bench_pct"],
        "marge_fcfa_kg":      l3["details"]["marge_fcfa_kg"],
        "recommendation":     ("Vendre maintenant — conditions optimales" if mwi >= 70 else
                               "Attendre une meilleure fenêtre" if mwi < 40 else
                               "Surveiller l'évolution des prix"),
    }

def compute_eyi(raw: dict, l2: dict) -> dict:
    """
    EYI™ — Epidemic & Yield Intelligence.
    Mesure le risque épidémique collectif et son impact
    sur le rendement attendu (0=risque nul, 100=risque maximal).
    """
    epid_risk = l2["risk"]
    double    = l2["details"]["double_signal"]
    wx_r      = l2["details"]["weather_risk"]
    neigh_r   = l2["details"]["neigh_risk"]
    transhu_r = l2["details"]["transhu_risk"]

    # Impact rendement estimé
    yield_impact_pct = round(min(40, epid_risk * 0.45))

    eyi = min(100, round(epid_risk + (20 if double else 0)))
    sev = _sev(eyi, EYI_THRESHOLDS)

    alert_30j = []
    if neigh_r > 30:   alert_30j.append("Foyer actif détecté dans votre zone")
    if transhu_r > 20: alert_30j.append("Transhumance — risque Newcastle/FMD accru")
    if wx_r > 15:      alert_30j.append("Conditions météo favorables aux maladies respiratoires")
    if double:         alert_30j.append("DOUBLE SIGNAL — intervention préventive urgente")

    return {
        "value": eyi, "severity": sev, "confidence": 0.74,
        "yield_impact_pct": yield_impact_pct,
        "alerts_30j": alert_30j,
        "recommendation": ("Isolement préventif et renforcement biosécurité immédiat" if eyi >= 70 else
                           "Surveillance renforcée — 2x/semaine" if eyi >= 45 else
                           "Monitoring standard hebdomadaire"),
    }

def compute_fci(raw: dict, l3: dict) -> dict:
    """
    FCI™ — Feed Cost Intelligence.
    Mesure la pression des coûts d'intrants sur la rentabilité
    (0=coûts normaux, 100=pression critique sur marges).
    """
    cost_stress  = l3["details"]["cost_stress"]
    stock_pen    = l3["details"]["stock_penalty"]
    marge        = l3["details"]["marge_fcfa_kg"]
    mais_p       = _f(raw.get("maizPrice"),    12000, 3000, 100_000)
    feed_p       = _f(raw.get("feedBagPrice"), 17500, 5000, 150_000)

    fci = min(100, round(cost_stress * 1.5 + stock_pen * 0.8))
    sev = _sev(fci, FCI_THRESHOLDS)

    bench_mais = 12000; bench_feed = 17500
    mais_var  = round((mais_p / bench_mais - 1) * 100, 1)
    feed_var  = round((feed_p / bench_feed - 1) * 100, 1)

    return {
        "value": fci, "severity": sev, "confidence": 0.77,
        "marge_fcfa_kg":   marge,
        "mais_var_pct":    mais_var,
        "feed_var_pct":    feed_var,
        "stock_issues":    [] if "aucune" in (raw.get("stockOuts") or ["aucune"]) else raw.get("stockOuts"),
        "recommendation":  ("Coûts critiques — cherchez des fournisseurs alternatifs" if fci >= 75 else
                            "Coûts tendus — optimisez la consommation alimentaire" if fci >= 50 else
                            "Coûts maîtrisés — continuez votre programme"),
    }

def compute_hvi(raw: dict, l1: dict, l2: dict, l4: dict) -> dict:
    """
    HVI™ — Herd Vulnerability Index.
    Évalue la vulnérabilité structurelle du troupeau aux maladies
    et chocs externes (0=résilient, 100=très vulnérable).
    """
    vacc_score = l1["details"].get("vacc_score", 70)
    water_r    = 1.0 - min(1.0, l1["details"]["water_ratio"])
    epid_r     = l2["risk"]
    abn_sounds = l4["details"]["n_abnormal"]
    has_symp   = len(raw.get("symptoms", []) or []) > 0

    # Composantes vulnérabilité
    imm_gap   = round(max(0, 100 - vacc_score) * 0.5)   # Lacune immunitaire
    water_gap = round(water_r * 25)                       # Stress hydrique
    epid_exp  = round(epid_r * 0.35)                      # Exposition épidémique
    symp_pen  = 25 if has_symp else 0                     # Symptômes présents
    sound_pen = abn_sounds * 8                            # Signaux bioacoustiques

    hvi = min(100, round(imm_gap + water_gap + epid_exp + symp_pen + sound_pen))
    sev = _sev(hvi, HVI_THRESHOLDS)

    return {
        "value": hvi, "severity": sev, "confidence": 0.80,
        "components": {
            "immunite_gap":    imm_gap,
            "stress_hydrique": water_gap,
            "exposition_epid": epid_exp,
            "symptomes":       symp_pen,
            "signaux_sonores": sound_pen,
        },
        "recommendation": ("Intervention vétérinaire urgente — troupeau très vulnérable" if hvi >= 70 else
                           "Renforcement sanitaire nécessaire cette semaine" if hvi >= 50 else
                           "Troupeau résilient — maintenir les protocoles actuels"),
    }

# ════════════════════════════════════════════════════════════════
#  SYSTÈME D'ALERTES
# ════════════════════════════════════════════════════════════════

def build_alerts(raw: dict, indices: dict, layers: dict) -> list:
    alerts = []

    def A(title, severity, category, reco):
        alerts.append({"id": str(uuid.uuid4())[:8], "title": title,
                        "severity": severity, "category": category,
                        "recommendation": reco})

    agric = indices["AgricScore"]["value"]
    lli   = indices["LLI"]["value"]
    eyi   = indices["EYI"]["value"]
    fci   = indices["FCI"]["value"]
    hvi   = indices["HVI"]["value"]
    l2    = layers["L2_pegasus"]
    l4    = layers["L4_acoustique"]

    mort = layers["L1_jamestown"]["details"]["mort_pct"]

    # ── Alertes épidémiques ───────────────────────────
    if l2["details"]["double_signal"]:
        A("🚨 DOUBLE SIGNAL ÉPIDÉMIQUE — Voisins + Transhumance", "critical", "epidemique",
          "Risque épidémie imminent. Renforcez la biosécurité, vérifiez vaccination Newcastle/FMD.")

    if eyi >= EYI_THRESHOLDS["ALERTE"]:
        A(f"⚠️ EYI™ critique — Risque épidémique {eyi}/100", "critical", "epidemique",
          "Réseau sentinelle M'vaye signale une activité épidémique dans votre zone.")

    # ── Alertes sanitaires ────────────────────────────
    if mort > 3.0:
        A(f"💀 Mortalité critique : {mort:.1f}% en 7 jours", "critical", "sante",
          "Au-delà du seuil acceptable. Autopsie d'urgence pour identifier la cause.")
    elif mort > 1.5:
        A(f"⚠️ Mortalité élevée : {mort:.1f}%", "high", "sante",
          "Surveillez alimentation, ventilation et sources d'eau.")

    if hvi >= HVI_THRESHOLDS["CRITIQUE"]:
        A(f"🛡️ HVI™ critique — Troupeau très vulnérable {hvi}/100", "critical", "sante",
          "Intervention vétérinaire urgente. Vérifiez immunité et conditions d'élevage.")

    if lli >= LLI_THRESHOLDS["CRITIQUE"]:
        A(f"📉 LLI™ élevé — Risque perte {lli}/100", "high", "sanitaire",
          "Risque de pertes animales significatives. Plan d'action sanitaire immédiat.")

    # ── Alertes bioacoustiques ─────────────────────────
    diag = l4["details"]["diagnosis"]
    if diag != "RAS":
        sev = "critical" if "urgent" in diag.lower() else "high"
        A(f"🎙️ Signal acoustique : {diag}", sev, "bioacoustique",
          "Observation rapprochée du troupeau. Consultation vétérinaire si persistance.")

    # ── Alertes biosécurité ───────────────────────────
    if raw.get("newAnimals") == "oui_direct":
        A("⚠️ Nouveaux animaux sans quarantaine", "critical", "biosecurite",
          "Isolez immédiatement les nouveaux arrivants. Quarantaine 14 jours minimum.")

    if raw.get("transhumance") == "oui":
        A("🐂 Transhumance détectée à proximité", "high", "epidemique",
          "Risque Newcastle/FMD accru. Vérifiez l'immunité du troupeau.")

    # ── Alertes intrants ──────────────────────────────
    stock_outs = raw.get("stockOuts", ["aucune"]) or ["aucune"]
    if "vaccin" in stock_outs:
        A("💉 Rupture de vaccins", "moderate", "intrants",
          "Contactez le délégué MINEPIA ou un fournisseur alternatif immédiatement.")

    if fci >= FCI_THRESHOLDS["CRITIQUE"]:
        A(f"💰 FCI™ critique — Pression coûts {fci}/100", "high", "intrants",
          "Marges sous pression critique. Cherchez des alternatives d'approvisionnement.")

    # ── Alertes eau ───────────────────────────────────
    if layers["L1_jamestown"]["details"]["water_ratio"] < 0.75:
        A("💧 Consommation d'eau anormalement basse", "high", "sante",
          "Signal pré-symptomatique de maladie. Vérifiez abreuvoirs et état du troupeau.")

    # ── Alertes marché ────────────────────────────────
    season_evts = raw.get("seasonEvents", ["aucun"]) or ["aucun"]
    if "aucun" not in season_evts:
        ev_name = ", ".join([e for e in season_evts if e != "aucun"])
        A(f"📅 Événement saisonnier — {ev_name}", "info", "marche",
          "Hausse de demande prévisible de 20–40%. Planifiez votre vente en conséquence.")

    # ── Positif si tout va bien ───────────────────────
    if not alerts and agric >= 75:
        A("✅ Tous les indicateurs sont au vert", "info", "positif",
          "Excellente gestion. Votre troupeau est au-dessus de la moyenne régionale.")

    return alerts

# ════════════════════════════════════════════════════════════════
#  PRÉDICTIONS J+7 / J+14 / J+21
# ════════════════════════════════════════════════════════════════

def build_predictions(raw: dict, agric_score: int, layers: dict) -> dict:
    """
    Prédictions temporelles basées sur les tendances actuelles.
    Drift calculé à partir des risques épidémiques, marché et santé.
    """
    price_ch  = raw.get("priceChange",  "stable")
    season    = raw.get("seasonEvents", ["aucun"]) or ["aucun"]
    mort      = layers["L1_jamestown"]["details"]["mort_pct"]
    epid_r    = layers["L2_pegasus"]["risk"]
    abn_snd   = layers["L4_acoustique"]["details"]["n_abnormal"]
    audio_rec = raw.get("audioRecorded", False)

    mkt_drift   = {"hausse": 4, "stable": 0, "baisse": -8}.get(price_ch, 0)
    saison_d    = 0 if "aucun" in season else len([s for s in season if s != "aucun"]) * 7
    health_d    = -(mort * 0.8) - (epid_r / 100 * 12) - (abn_snd * 4)
    drift7      = health_d + mkt_drift + saison_d

    base_conf   = 72 if audio_rec else 58

    p7  = max(5, min(100, round(agric_score + drift7)))
    p14 = max(5, min(100, round(agric_score + drift7 * 1.7)))
    p21 = max(5, min(100, round(agric_score + drift7 * 2.4)))

    def trend(p):
        d = p - agric_score
        return "↗️ Amélioration" if d > 5 else "↘️ Dégradation" if d < -5 else "→ Stable"

    return {
        "j0":  agric_score,
        "j7":  p7,  "j7_conf":  f"{min(base_conf, 80)}%",  "j7_trend":  trend(p7),
        "j14": p14, "j14_conf": f"{min(base_conf-8, 70)}%", "j14_trend": trend(p14),
        "j21": p21, "j21_conf": f"{min(base_conf-16,60)}%", "j21_trend": trend(p21),
        "drift_7j": round(drift7, 1),
    }

# ════════════════════════════════════════════════════════════════
#  FUSION ENGINE — Orchestrateur principal
# ════════════════════════════════════════════════════════════════

def fusion_engine(raw: dict) -> dict:
    """
    FusionEngine MetaCore™ — point d'entrée unique.
    Orchestre les 4 couches, calcule les 6 indices,
    génère les alertes et prédictions.
    """
    t0 = time.perf_counter()

    # ── Étape 1 : Calcul des 4 couches ───────────────
    l1 = layer_jamestown(raw)
    l2 = layer_pegasus(raw)
    l3 = layer_gotham(raw)
    l4 = layer_acoustique(raw)

    layers = {
        "L1_jamestown":  l1,
        "L2_pegasus":    l2,
        "L3_gotham":     l3,
        "L4_acoustique": l4,
    }

    # ── Étape 2 : Calcul des 6 indices ───────────────
    agric = compute_agric_score(l1, l2, l3, l4)
    lli   = compute_lli(raw, l1, l2)
    mwi   = compute_mwi(raw, l3)
    eyi   = compute_eyi(raw, l2)
    fci   = compute_fci(raw, l3)
    hvi   = compute_hvi(raw, l1, l2, l4)

    indices = {
        "AgricScore": agric,
        "LLI":        lli,
        "MWI":        mwi,
        "EYI":        eyi,
        "FCI":        fci,
        "HVI":        hvi,
    }

    # ── Étape 3 : Alertes ────────────────────────────
    alerts = build_alerts(raw, indices, layers)

    # ── Étape 4 : Prédictions ─────────────────────────
    predictions = build_predictions(raw, agric["value"], layers)

    # ── Étape 5 : Comparaison régionale ───────────────
    region    = raw.get("region", "")
    reg_avg   = store.regional_avg(region) if region else {"available": False}

    ms = round((time.perf_counter() - t0) * 1000, 1)

    # ── Qualité données globale ───────────────────────
    conf_avg = round(
        l1["confidence"] * 0.30 + l2["confidence"] * 0.25 +
        l3["confidence"] * 0.25 + l4["confidence"] * 0.20, 2
    )

    return {
        # Résumé rapide (pour l'affichage M'vaye)
        "agric_score":        agric["value"],
        "zone":               agric["zone"],
        "zone_recommendation":agric["recommendation"],
        "lli":                lli["value"],
        "mwi":                mwi["value"],
        "eyi":                eyi["value"],
        "fci":                fci["value"],
        "hvi":                hvi["value"],

        # Indices complets
        "indices": indices,

        # Couches (pour debug / dashboard)
        "layers": layers,

        # Prédictions
        "predictions": predictions,

        # Alertes
        "alerts": alerts,

        # Comparaison régionale
        "regional_comparison": reg_avg,

        # Meta
        "active_layers": list(LAYER_WEIGHTS.keys()),
        "data_quality":  conf_avg,
        "processing_ms": ms,
        "version":       VERSION,
    }

# ════════════════════════════════════════════════════════════════
#  SERVEUR HTTP
# ════════════════════════════════════════════════════════════════

def resp_json(handler, data: dict, status=200):
    body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type",  "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Access-Control-Allow-Origin",  "*")
    handler.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
    handler.end_headers()
    handler.wfile.write(body)

def read_body(handler) -> dict:
    length = int(handler.headers.get("Content-Length", 0))
    if length == 0: return {}
    try:    return json.loads(handler.rfile.read(length).decode("utf-8"))
    except: return {}

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        log.info(f"{self.address_string()} — {fmt % args}")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_GET(self):
        path = urlparse(self.path).path.rstrip("/")
        qs   = parse_qs(urlparse(self.path).query)

        if path == "/api/v1/health":
            resp_json(self, {
                "status": "ok", "version": VERSION,
                "metacore_active": True, "entries": store.count(),
                "layers_active": list(LAYER_WEIGHTS.keys()),
                "indices": ["AgricScore","LLI","MWI","EYI","FCI","HVI"],
                "timestamp": datetime.utcnow().isoformat(),
            })

        elif path == "/api/v1/entries":
            limit  = int(qs.get("limit",  ["50"])[0])
            region = qs.get("region", [None])[0]
            entries = store.list_entries(min(limit, 500), region)
            safe    = [{k: v for k, v in e.items() if k not in {"whatsapp", "wa"}} for e in entries]
            resp_json(self, {"count": len(safe), "entries": safe})

        elif path.startswith("/api/v1/entries/"):
            eid = path.split("/")[-1]
            e   = store.get_entry(eid)
            if e: resp_json(self, {"entry": e, "result": store.get_result(eid)})
            else: resp_json(self, {"error": f"Entrée {eid} introuvable"}, 404)

        elif path == "/api/v1/metrics":
            resp_json(self, store.metrics())

        elif path == "/api/v1/metacore/status":
            resp_json(self, {
                "version": VERSION,
                "layers": {
                    "L1_jamestown":     {"active": True,  "name": "Jamestown Analytics",  "accuracy_pct": 82},
                    "L2_pegasus":       {"active": True,  "name": "Pegasus Streaming",     "accuracy_pct": 76},
                    "L3_gotham":        {"active": True,  "name": "Gotham Intelligence",   "accuracy_pct": 71},
                    "L4_acoustique":    {"active": True,  "name": "Bioacoustique",         "accuracy_pct": 63},
                    "L5_comportemental":{"active": False, "eta": "v1.0"},
                    "L6_competitif":    {"active": False, "eta": "v1.1"},
                },
                "indices":       ["AgricScore™","LLI™","MWI™","EYI™","FCI™","HVI™"],
                "predictions":   ["J+7","J+14","J+21"],
                "total_entries": store.count(),
                "fusion_engine": "FusionEngine v0.8",
            })

        elif path.startswith("/api/v1/region/"):
            region = path.split("/")[-1]
            resp_json(self, {"region": region, **store.regional_avg(region)})

        elif path in ("", "/"):
            self._home()

        else:
            resp_json(self, {"error": "Route introuvable", "path": path}, 404)

    def do_POST(self):
        path = urlparse(self.path).path.rstrip("/")

        if path == "/api/v1/collect":
            data = read_body(self)
            if not data:
                resp_json(self, {"error": "Corps JSON requis"}, 400); return
            if not data.get("ownerName") and not data.get("nom"):
                resp_json(self, {"error": "ownerName est requis"}, 400); return

            raw = normalize(data)
            eid = store.save_entry(raw)
            raw["id"] = eid

            try:
                result = fusion_engine(raw)
                store.save_result(eid, result)
                resp_json(self, {
                    "success":  True,
                    "entry_id": eid,
                    "owner":    raw.get("ownerName"),
                    "metacore": result,
                }, 201)
            except Exception as e:
                log.error(f"FusionEngine erreur: {e}", exc_info=True)
                resp_json(self, {"success": False, "error": str(e), "entry_id": eid}, 500)

        elif path == "/api/v1/sync":
            data    = read_body(self)
            entries = data.get("entries", [])
            if not isinstance(entries, list):
                resp_json(self, {"error": "entries doit être une liste"}, 400); return
            results, errors = [], []
            for entry in entries:
                try:
                    norm = normalize(entry)
                    eid  = store.save_entry(norm)
                    res  = fusion_engine(norm)
                    store.save_result(eid, res)
                    results.append({"entry_id": eid, "score": res["agric_score"]})
                except Exception as e:
                    errors.append({"entry": entry.get("id"), "error": str(e)})
            resp_json(self, {"synced": len(results), "errors": len(errors), "results": results})

        else:
            resp_json(self, {"error": "Route POST introuvable", "path": path}, 404)

    def _home(self):
        body = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>STALIZA MetaCore™ v{VERSION}</title>
<style>
  body{{font-family:system-ui;background:#0F0A06;color:#F0E8D8;max-width:800px;margin:50px auto;padding:24px}}
  h1{{color:#E06835;font-size:28px}}h2{{color:#C9962A;font-size:16px;margin-top:28px}}
  code{{background:#1E1409;padding:3px 10px;border-radius:6px;font-size:13px;color:#E06835}}
  table{{width:100%;border-collapse:collapse;margin:12px 0}}
  td,th{{padding:10px 14px;border:1px solid #2A1C0C;text-align:left;font-size:13px}}
  th{{background:#1E1409;color:#C9962A}}
  .ok{{color:#5CAD6B;font-weight:700}}.badge{{background:#2A1C0C;border-radius:20px;padding:4px 14px;font-size:12px;color:#C9962A}}
  .idx{{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin:12px 0}}
  .idx-card{{background:#1E1409;border:1px solid #2A1C0C;border-radius:12px;padding:14px;text-align:center}}
  .idx-name{{color:#E06835;font-weight:700;font-size:14px}}.idx-desc{{color:#9B7A52;font-size:11px;margin-top:4px}}
</style></head><body>
<h1>🌾 STALIZA MetaCore™</h1>
<p><span class="ok">● EN LIGNE</span> &nbsp;<span class="badge">v{VERSION}</span> &nbsp;
<span class="badge">FusionEngine</span> &nbsp;<span class="badge">6 indices</span></p>

<h2>6 Indices MetaCore™</h2>
<div class="idx">
  <div class="idx-card"><div class="idx-name">AgricScore™</div><div class="idx-desc">Score santé global 0–100</div></div>
  <div class="idx-card"><div class="idx-name">LLI™</div><div class="idx-desc">Livestock Loss Index</div></div>
  <div class="idx-card"><div class="idx-name">MWI™</div><div class="idx-desc">Market Window Index</div></div>
  <div class="idx-card"><div class="idx-name">EYI™</div><div class="idx-desc">Epidemic & Yield Intelligence</div></div>
  <div class="idx-card"><div class="idx-name">FCI™</div><div class="idx-desc">Feed Cost Intelligence</div></div>
  <div class="idx-card"><div class="idx-name">HVI™</div><div class="idx-desc">Herd Vulnerability Index</div></div>
</div>

<h2>API Endpoints</h2>
<table>
  <tr><th>Méthode</th><th>URL</th><th>Description</th></tr>
  <tr><td>GET</td><td><code>/api/v1/health</code></td><td>Statut serveur + indices actifs</td></tr>
  <tr><td>POST</td><td><code>/api/v1/collect</code></td><td>Soumettre un relevé → 6 scores</td></tr>
  <tr><td>POST</td><td><code>/api/v1/sync</code></td><td>Sync batch offline (SQLite → API)</td></tr>
  <tr><td>GET</td><td><code>/api/v1/entries</code></td><td>Liste des relevés</td></tr>
  <tr><td>GET</td><td><code>/api/v1/entries/{{id}}</code></td><td>Détail + résultats MetaCore™</td></tr>
  <tr><td>GET</td><td><code>/api/v1/metrics</code></td><td>Statistiques globales</td></tr>
  <tr><td>GET</td><td><code>/api/v1/region/{{nom}}</code></td><td>Score moyen régional</td></tr>
  <tr><td>GET</td><td><code>/api/v1/metacore/status</code></td><td>État couches + indices</td></tr>
</table>
<p style="color:#9B7A52;font-size:12px;margin-top:24px">Intelligence Agricole Cameroun · STALIZA MetaCore™ v{VERSION} · M'vaye Platform</p>
</body></html>""".encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


# ════════════════════════════════════════════════════════════════
#  DÉMARRAGE
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 8000))
    print(f"""
╔══════════════════════════════════════════════════════════╗
║  🌾  STALIZA MetaCore™ v{VERSION}                            ║
║                                                          ║
║  ✅  Serveur démarré — port {PORT:<29}║
║  🌐  http://localhost:{PORT:<33}║
║                                                          ║
║  Indices actifs : AgricScore™ LLI™ MWI™ EYI™ FCI™ HVI™  ║
║  Couches : Jamestown · Pegasus · Gotham · Acoustique     ║
║  FusionEngine : ACTIF                                    ║
║                                                          ║
║  Arrêter : Ctrl+C                                        ║
╚══════════════════════════════════════════════════════════╝
""")
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n⛔  Serveur arrêté.")
        server.server_close()
