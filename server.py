"""
╔══════════════════════════════════════════════════════════════╗
║  STALIZA MetaCore™ v0.7 — Serveur Python pur                ║
║  Aucune installation requise. Lance avec : python server.py  ║
╚══════════════════════════════════════════════════════════════╝
"""

import os, sys, json, uuid, logging, threading, time
from datetime import datetime
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("metacore")

# ════════════════════════════════════════════════════════════════
#  1. STORE JSON (sauvegarde sur disque)
# ════════════════════════════════════════════════════════════════

DATA_DIR     = Path("data_store")
DATA_DIR.mkdir(exist_ok=True)
ENTRIES_FILE = DATA_DIR / "entries.json"
RESULTS_FILE = DATA_DIR / "results.json"

class Store:
    def __init__(self):
        self._lock = threading.Lock()
        for f, d in [(ENTRIES_FILE, []), (RESULTS_FILE, {})]:
            if not f.exists():
                f.write_text(json.dumps(d, ensure_ascii=False), "utf-8")

    def _r(self, f):
        try:
            if f.exists() and f.stat().st_size > 2:
                return json.loads(f.read_text("utf-8"))
        except:
            pass
        return {} if f == RESULTS_FILE else []

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
            self._w(ENTRIES_FILE, entries[-1000:])
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

    def list_entries(self, limit=50):
        with self._lock:
            all_e = self._r(ENTRIES_FILE)
        return sorted(all_e, key=lambda x: x.get("_at", ""), reverse=True)[:limit]

    def count(self) -> int:
        with self._lock:
            return len(self._r(ENTRIES_FILE))

    def metrics(self) -> dict:
        with self._lock:
            entries = self._r(ENTRIES_FILE)
            results = self._r(RESULTS_FILE)
        if not entries:
            return {"total_entries": 0}
        regions = {}
        for e in entries:
            r = e.get("region", "?")
            regions[r] = regions.get(r, 0) + 1
        scores = [float(r.get("agric_score", 0)) for r in results.values() if r.get("agric_score")]
        return {
            "total_entries":   len(entries),
            "total_results":   len(results),
            "by_region":       regions,
            "avg_agric_score": round(sum(scores) / len(scores), 1) if scores else None,
        }

store = Store()

# ════════════════════════════════════════════════════════════════
#  2. HELPERS
# ════════════════════════════════════════════════════════════════

def _i(v, default, mn, mx):
    try:    return max(mn, min(mx, int(float(str(v)))))
    except: return default

def _f(v, default, mn, mx):
    try:    return max(mn, min(mx, float(str(v))))
    except: return default

def normalize(raw: dict) -> dict:
    out = {}
    for k, v in raw.items():
        if isinstance(v, str):           out[k] = v.strip()[:500]
        elif isinstance(v, (int, float, bool)): out[k] = v
        elif isinstance(v, list):        out[k] = [str(i)[:100] for i in v if i is not None][:20]
        elif isinstance(v, dict):        out[k] = v
        else:                            out[k] = str(v)[:200] if v is not None else None
    out.setdefault("id", "MVY-" + str(uuid.uuid4())[:8].upper())
    out.setdefault("timestamp", datetime.utcnow().isoformat())
    out.setdefault("schema_version", "0.7")
    return out

# ════════════════════════════════════════════════════════════════
#  3. MOTEUR METACORE™
# ════════════════════════════════════════════════════════════════

def compute(raw: dict) -> dict:
    t0 = time.perf_counter()

    head    = _i(raw.get("headCount"), 100, 1, 100_000)
    deaths  = _i(raw.get("deaths"),    0,   0, head)
    mort    = (deaths / max(head, 1)) * 100
    water   = _f(raw.get("waterLiters"), 0.20, 0, 50)
    weight  = _f(raw.get("weight"),      1.5, 0.05, 20)

    vaccin  = raw.get("vaccin",       "normal")
    has_vet = raw.get("hasVet",       "non")
    feed    = raw.get("feed",         "standard")
    price_ch= raw.get("priceChange",  "stable")
    neigh   = raw.get("neighDeaths",  "non")
    transhu = raw.get("transhumance", "non")
    new_ani = raw.get("newAnimals",   "non")
    visitors= raw.get("visitors",     "aucun")
    bird_mkt= raw.get("liveBirdMarket","non")
    snd_int = raw.get("soundIntensity","aucune")
    snd_dur = raw.get("soundDuration", "aucun")

    symptoms    = raw.get("symptoms",     []) or []
    sounds      = raw.get("sounds",       ["normal"]) or ["normal"]
    wx_events   = raw.get("weatherEvents",["normal"]) or ["normal"]
    season_evts = raw.get("seasonEvents", ["aucun"])  or ["aucun"]
    stock_outs  = raw.get("stockOuts",    ["aucune"]) or ["aucune"]

    has_symp  = len(symptoms) > 0 and "aucun" not in symptoms
    abn_sounds= [s for s in sounds if s != "normal"]
    wx_stress = [w for w in wx_events if w != "normal"]

    # ── SANTÉ (25%) ───────────────────────────────────
    vacc_s = {"recent": 100, "normal": 80, "retard": 45, "jamais": 10}.get(vaccin, 70)
    vet_s  = {"oui": 90, "parfois": 65, "non": 40}.get(has_vet, 60)
    symp_s = 40 if has_symp else 90
    mort_s = max(0, 100 - mort * 15)
    sante  = vacc_s*0.30 + vet_s*0.20 + symp_s*0.30 + mort_s*0.20

    # ── BIOSÉCURITÉ (15%) ─────────────────────────────
    new_s  = {"non": 100, "oui_quarantaine": 70, "oui_direct": 20}.get(new_ani, 80)
    vis_s  = {"aucun": 100, "famille": 85, "acheteurs": 60, "multi": 30}.get(visitors, 80)
    bird_s = {"non": 100, "oui_quarantaine": 70, "oui_direct": 15}.get(bird_mkt, 90)
    biosec = new_s*0.40 + vis_s*0.30 + bird_s*0.30

    # ── BIOACOUSTIQUE (15%) ───────────────────────────
    int_s  = {"aucune": 100, "faible": 65, "forte": 25}.get(snd_int, 85)
    dur_s  = {"aucun": 100, "24h": 65, "3j": 40, "7j": 25, "long": 10}.get(snd_dur, 85)
    acoust = max(0, min(100, int_s*0.40 + dur_s*0.40 + (100 - len(abn_sounds)*15)*0.20))

    # ── ALIMENTATION (20%) ────────────────────────────
    feed_s = {"premium": 95, "standard": 72, "artisan": 52, "mauvais": 25}.get(feed, 65)

    # ── MARCHÉ (15%) ─────────────────────────────────
    price_s  = {"hausse": 90, "stable": 65, "baisse": 35}.get(price_ch, 55)
    ev_boost = 0 if "aucun" in season_evts else 12
    stock_r  = 0 if "aucune" in stock_outs else len(stock_outs) * 8
    marche   = max(0, min(100, price_s + ev_boost - stock_r))

    # ── EAU (10%) ─────────────────────────────────────
    bench_water = {"poulet_chair": 0.20, "ponte": 0.22, "bovin": 30, "porcin": 5}.get(
        raw.get("animalType", "poulet_chair"), 0.20)
    ratio   = water / max(bench_water, 0.01)
    water_s = 40 if ratio < 0.7 else 70 if ratio < 0.9 else 100

    # ── RISQUE ÉPIDÉMIQUE (pénalité) ──────────────────
    neigh_r  = {"non": 0, "incertain": 10, "oui": 40}.get(neigh, 5)
    transhu_r= {"non": 0, "incertain": 8,  "oui": 30}.get(transhu, 5)
    wx_r     = len(wx_stress) * 8
    epid_r   = min(60, neigh_r + transhu_r + wx_r + (10 if abn_sounds else 0))

    # ── SCORE FINAL ───────────────────────────────────
    score = round(
        sante   * 0.25 +
        biosec  * 0.15 +
        acoust  * 0.15 +
        feed_s  * 0.20 +
        marche  * 0.15 +
        water_s * 0.10
        - epid_r * 0.30
    )
    score = max(5, min(100, score))

    zone = ("VERTE" if score >= 75 else
            "JAUNE"  if score >= 50 else
            "ORANGE" if score >= 25 else "ROUGE")

    zone_reco = {
        "VERTE":  "Troupeau en bonne santé. Fenêtre de vente favorable.",
        "JAUNE":  "Surveillance recommandée. Vérifiez alimentation et état sanitaire.",
        "ORANGE": "Action requise dans les 48h. Consultez votre vétérinaire.",
        "ROUGE":  "Situation critique. Isolez le troupeau et contactez le MINEPIA.",
    }

    # ── PRÉDICTIONS J+7 / J+14 / J+21 ────────────────
    mkt_drift = {"hausse": 4, "stable": 0, "baisse": -7}.get(price_ch, 0)
    ev_drift  = 0 if "aucun" in season_evts else 8
    drift7    = -(mort*0.6) - (epid_r/100*10) - (len(abn_sounds)*3) + mkt_drift + ev_drift
    base_conf = 68 if raw.get("audioRecorded") else 55

    # ── LLI (Livestock Loss Index) ────────────────────
    lli = min(100, round(
        mort * 4 +
        (30 if has_symp else 0) +
        neigh_r + transhu_r +
        ({"non": 0, "oui_quarantaine": 10, "oui_direct": 50}.get(bird_mkt, 5)) +
        ({"aucune": 0, "faible": 20, "forte": 60}.get(snd_int, 0)) +
        wx_r +
        len(abn_sounds) * 5
    ))
    lli_sev = ("CRITIQUE" if lli > 75 else
               "ÉLEVÉ"    if lli > 50 else
               "MODÉRÉ"   if lli > 25 else "FAIBLE")

    # ── MWI (Market Watch Index) ──────────────────────
    mkt_price = _f(raw.get("marketPrice"), 2500, 100, 50_000)
    buyers    = raw.get("regularBuyers", "non")
    buy_b     = {"oui": 5, "coop": 8, "non": 0}.get(buyers, 0)
    mwi       = max(0, min(100, round(
        {"hausse": 80, "stable": 55, "baisse": 30}.get(price_ch, 50) +
        ev_boost - stock_r + buy_b
    )))

    # ── ALERTES ───────────────────────────────────────
    alerts = []

    def alert(title, severity, category, reco):
        alerts.append({"id": str(uuid.uuid4())[:8], "title": title,
                        "severity": severity, "category": category, "recommendation": reco})

    if neigh == "oui" and transhu == "oui":
        alert("🚨 Double signal épidémique — voisins + transhumance", "critical", "epidemique",
              "Risque épidémie imminent. Renforcez la biosécurité, vérifiez vaccination Newcastle.")
    elif lli > 60:
        alert(f"⚠️ LLI élevé — Risque sanitaire {lli}/100", "high", "sanitaire",
              "Surveillance quotidienne des mortalités. Vétérinaire sous 48h.")

    if abn_sounds and snd_int == "forte":
        alert("🎙️ Sons anormaux intenses dans le troupeau", "critical", "bioacoustique",
              "Suspicion maladie respiratoire. Isolez les individus affectés immédiatement.")
    elif abn_sounds:
        alert("🔊 Sons respiratoires anormaux", "high", "bioacoustique",
              "Observation rapprochée recommandée. Consultation vétérinaire si aggravation.")

    if mort > 3.0:
        alert(f"💀 Mortalité critique : {mort:.1f}% en 7 jours", "critical", "sante",
              "Au-delà du seuil acceptable. Autopsie d'urgence pour identifier la cause.")
    elif mort > 1.5:
        alert(f"⚠️ Mortalité élevée : {mort:.1f}%", "high", "sante",
              "Surveillez alimentation, ventilation et sources d'eau.")

    if new_ani == "oui_direct":
        alert("⚠️ Nouveaux animaux sans quarantaine", "critical", "biosecurite",
              "Isolez immédiatement les nouveaux arrivants. Quarantaine 14 jours minimum.")

    if transhu == "oui":
        alert("🐂 Transhumance détectée à proximité", "high", "epidemique",
              "Risque Newcastle accru. Vérifiez l'immunité du troupeau.")

    if "vaccin" in stock_outs:
        alert("💉 Rupture de vaccins chez votre fournisseur", "moderate", "intrants",
              "Contactez le délégué MINEPIA ou un fournisseur alternatif.")

    if ratio < 0.75:
        alert("💧 Consommation d'eau anormalement basse", "high", "sante",
              "Signal pré-symptomatique de maladie. Vérifiez abreuvoirs.")

    if "aucun" not in season_evts:
        ev_name = ", ".join([e for e in season_evts if e != "aucun"])
        alert(f"📅 Événement saisonnier à venir — {ev_name}", "info", "marche",
              "Hausse de demande probable de 20–40%. Planifiez votre vente.")

    if not alerts and score >= 75:
        alert("✅ Troupeau en excellente condition", "info", "positif",
              "Tous les indicateurs sont au vert. Continuez votre programme actuel.")

    ms = round((time.perf_counter() - t0) * 1000, 1)

    return {
        "agric_score": score,
        "lli": lli,
        "mwi": mwi,
        "zone": zone,
        "zone_recommendation": zone_reco[zone],
        "indices": {
            "AgricScore": {
                "value": score, "confidence": 0.74, "zone": zone,
                "components": {
                    "sante": round(sante), "biosec": round(biosec),
                    "acoustique": round(acoust), "alimentation": round(feed_s),
                    "marche": round(marche), "eau": round(water_s),
                    "risque_epidemique": round(epid_r),
                },
                "predictions": {
                    "j0":  score,
                    "j7":  max(5, min(100, round(score + drift7))),
                    "j7_conf":  f"{min(base_conf, 80)}%",
                    "j14": max(5, min(100, round(score + drift7 * 1.6))),
                    "j14_conf": f"{min(base_conf - 8, 70)}%",
                    "j21": max(5, min(100, round(score + drift7 * 2.2))),
                    "j21_conf": f"{min(base_conf - 15, 60)}%",
                },
            },
            "LLI": {"value": lli, "severity": lli_sev, "confidence": 0.79},
            "MWI": {
                "value": mwi, "confidence": 0.68,
                "price_vs_benchmark_pct": round((mkt_price / 2500 - 1) * 100, 1),
                "recommendation": ("Vendre maintenant" if mwi > 70 else
                                   "Attendre" if mwi < 40 else "Surveiller"),
            },
        },
        "alerts": alerts,
        "active_layers": ["L1_biologique", "L2_epidemique", "L3_marche", "L4_bioacoustique"],
        "processing_ms": ms,
    }

# ════════════════════════════════════════════════════════════════
#  4. SERVEUR HTTP
# ════════════════════════════════════════════════════════════════

def resp_json(handler, data: dict, status=200):
    body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Access-Control-Allow-Origin",  "*")
    handler.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
    handler.end_headers()
    handler.wfile.write(body)

def read_body(handler) -> dict:
    length = int(handler.headers.get("Content-Length", 0))
    if length == 0:
        return {}
    raw = handler.rfile.read(length)
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as e:
        log.error(f"JSON parse error: {e}")
        return {}

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
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip("/")
        qs     = parse_qs(parsed.query)

        # ── GET /api/v1/health ──────────────────────
        if path == "/api/v1/health":
            resp_json(self, {
                "status":          "ok",
                "version":         "0.7",
                "metacore_active": True,
                "entries":         store.count(),
                "timestamp":       datetime.utcnow().isoformat(),
            })

        # ── GET /api/v1/entries ─────────────────────
        elif path == "/api/v1/entries":
            limit = int(qs.get("limit", ["50"])[0])
            limit = max(1, min(limit, 500))
            entries = store.list_entries(limit)
            safe    = [{k: v for k, v in e.items() if k not in {"whatsapp"}} for e in entries]
            resp_json(self, {"count": len(safe), "entries": safe})

        # ── GET /api/v1/entries/{eid} ───────────────
        elif path.startswith("/api/v1/entries/"):
            eid = path.split("/")[-1]
            e   = store.get_entry(eid)
            if e:
                resp_json(self, {"entry": e, "result": store.get_result(eid)})
            else:
                resp_json(self, {"error": f"Entrée {eid} introuvable"}, 404)

        # ── GET /api/v1/metrics ─────────────────────
        elif path == "/api/v1/metrics":
            resp_json(self, store.metrics())

        # ── GET /api/v1/metacore/status ─────────────
        elif path == "/api/v1/metacore/status":
            resp_json(self, {
                "version": "0.7",
                "layers": {
                    "L1_biologique":     {"active": True,  "accuracy_pct": 78},
                    "L2_epidemique":     {"active": True,  "accuracy_pct": 72},
                    "L3_marche":         {"active": True,  "accuracy_pct": 68},
                    "L4_bioacoustique":  {"active": True,  "accuracy_pct": 65},
                    "L5_comportemental": {"active": False, "eta": "v1.0"},
                },
                "total_entries": store.count(),
                "indices":       ["AgricScore", "LLI", "MWI"],
                "predictions":   ["J+7", "J+14", "J+21"],
            })

        # ── Page d'accueil ──────────────────────────
        elif path in ("", "/"):
            body = """<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>STALIZA MetaCore™ v0.7</title>
<style>
  body{font-family:system-ui;background:#0F0A06;color:#F0E8D8;max-width:700px;margin:50px auto;padding:20px}
  h1{color:#E06835}code{background:#1E1409;padding:3px 10px;border-radius:6px;font-size:13px}
  table{width:100%;border-collapse:collapse;margin:16px 0}
  td,th{padding:10px;border:1px solid #2A1C0C;text-align:left;font-size:13px}
  th{background:#1E1409;color:#E06835}
  .ok{color:#5CAD6B;font-weight:700}.badge{background:#2A1C0C;border-radius:20px;padding:4px 12px;font-size:12px}
</style>
</head><body>
<h1>🌾 STALIZA MetaCore™</h1>
<p><span class="ok">● EN LIGNE</span> &nbsp;<span class="badge">v0.7</span></p>
<h2>Endpoints disponibles</h2>
<table>
  <tr><th>Méthode</th><th>URL</th><th>Description</th></tr>
  <tr><td>GET</td><td><code>/api/v1/health</code></td><td>Statut du serveur</td></tr>
  <tr><td>POST</td><td><code>/api/v1/collect</code></td><td>Soumettre un relevé → score</td></tr>
  <tr><td>POST</td><td><code>/api/v1/sync</code></td><td>Sync batch offline</td></tr>
  <tr><td>GET</td><td><code>/api/v1/entries</code></td><td>Liste des relevés</td></tr>
  <tr><td>GET</td><td><code>/api/v1/entries/{id}</code></td><td>Détail d'un relevé</td></tr>
  <tr><td>GET</td><td><code>/api/v1/metrics</code></td><td>Statistiques globales</td></tr>
  <tr><td>GET</td><td><code>/api/v1/metacore/status</code></td><td>État du moteur</td></tr>
</table>
<p style="color:#9B7A52;font-size:12px">Intelligence Agricole Cameroun · STALIZA MetaCore™</p>
</body></html>""".encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        else:
            resp_json(self, {"error": "Route introuvable", "path": path}, 404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip("/")

        # ── POST /api/v1/collect ────────────────────
        if path == "/api/v1/collect":
            data = read_body(self)
            if not data:
                resp_json(self, {"error": "Corps JSON requis"}, 400)
                return
            if not data.get("ownerName") and not data.get("nom"):
                resp_json(self, {"error": "ownerName est requis"}, 400)
                return
            # Accepter 'nom' comme alias de ownerName (compatibilité frontend)
            if not data.get("ownerName") and data.get("nom"):
                data["ownerName"] = data["nom"]

            raw = normalize(data)
            eid = store.save_entry(raw)
            raw["id"] = eid

            try:
                result = compute(raw)
                store.save_result(eid, result)
                resp_json(self, {
                    "success":   True,
                    "entry_id":  eid,
                    "owner":     raw.get("ownerName"),
                    "metacore":  result,
                }, 201)
            except Exception as e:
                log.error(f"Erreur MetaCore: {e}", exc_info=True)
                resp_json(self, {"success": False, "error": str(e), "entry_id": eid}, 500)

        # ── POST /api/v1/sync ───────────────────────
        elif path == "/api/v1/sync":
            data    = read_body(self)
            entries = data.get("entries", [])
            if not isinstance(entries, list):
                resp_json(self, {"error": "entries doit être une liste"}, 400)
                return
            results, errors = [], []
            for entry in entries:
                try:
                    norm = normalize(entry)
                    eid  = store.save_entry(norm)
                    res  = compute(norm)
                    store.save_result(eid, res)
                    results.append({"entry_id": eid, "score": res["agric_score"]})
                except Exception as e:
                    errors.append({"entry": entry.get("id"), "error": str(e)})
            resp_json(self, {"synced": len(results), "errors": len(errors), "results": results})

        else:
            resp_json(self, {"error": "Route POST introuvable", "path": path}, 404)


# ════════════════════════════════════════════════════════════════
#  5. DÉMARRAGE
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 8000))

    print(f"""
╔══════════════════════════════════════════════════╗
║  🌾  STALIZA MetaCore™ v0.7                      ║
║                                                  ║
║  ✅  Serveur démarré sur le port {PORT:<17}║
║  🌐  http://localhost:{PORT:<26}║
║  🔗  http://localhost:{PORT}/api/v1/health{' '*(6-len(str(PORT)))} ║
║                                                  ║
║  Arrêter : Ctrl+C                                ║
╚══════════════════════════════════════════════════╝
""")

    server = HTTPServer(("0.0.0.0", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n⛔  Serveur arrêté.")
        server.server_close()
