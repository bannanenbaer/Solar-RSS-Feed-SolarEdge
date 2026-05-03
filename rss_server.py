#!/usr/bin/env python3
"""Solar RSS Feed – SolarEdge monitoring with energy forecast and UV recommendation."""

import logging
import os
import sqlite3
import threading
import time
from datetime import date, datetime, timedelta
from statistics import mean

import pytz
import requests
from flask import Flask, Response, jsonify
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Config ──────────────────────────────────────────────────────────────────────────────
SOLAREDGE_API_KEY = os.environ.get("SOLAREDGE_API_KEY", "")
SOLAREDGE_SITE_ID = os.environ.get("SOLAREDGE_SITE_ID", "")
SOLAR_LAT = float(os.environ.get("SOLAR_LAT", "52.2762744"))
SOLAR_LON = float(os.environ.get("SOLAR_LON", "9.5671846"))
SOLAR_DB_PATH = os.environ.get("SOLAR_DB_PATH", "/data/solar.db")
SOLAR_PEAK_KW = float(os.environ.get("SOLAR_PEAK_KW", "10.0"))
SOLAR_BATTERY_KWH = float(os.environ.get("SOLAR_BATTERY_KWH", "10.0"))

TZ = pytz.timezone("Europe/Berlin")
SE_BASE = "https://monitoringapi.solaredge.com"
OM_BASE = "https://api.open-meteo.com/v1/forecast"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── HTTP session ───────────────────────────────────────────────────────────────────────

_http = requests.Session()
_retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
_http.mount("https://", HTTPAdapter(max_retries=_retry))

# ── In-memory caches ─────────────────────────────────────────────────────────────────────

_POWER_CACHE: dict = {}    # keys: data, ts
_ENERGY_CACHE: dict = {}   # keys: data, ts
_WEATHER_CACHE: dict = {}  # keys: data, ts
_cache_lock = threading.Lock()
_db_lock = threading.Lock()
_feed_lock = threading.Lock()
_feed_xml: str = ""

app = Flask(__name__)


# ── Database ─────────────────────────────────────────────────────────────────────────────

def _init_db() -> None:
    os.makedirs(os.path.dirname(SOLAR_DB_PATH), exist_ok=True)
    with _db_lock:
        con = sqlite3.connect(SOLAR_DB_PATH)
        con.executescript("""
            CREATE TABLE IF NOT EXISTS daily_energy (
                date             TEXT PRIMARY KEY,
                day_of_year      INTEGER,
                month            INTEGER,
                year             INTEGER,
                production_kwh   REAL DEFAULT 0,
                consumption_kwh  REAL DEFAULT 0,
                grid_import_kwh  REAL DEFAULT 0,
                grid_export_kwh  REAL DEFAULT 0,
                battery_charge_kwh    REAL DEFAULT 0,
                battery_discharge_kwh REAL DEFAULT 0,
                peak_power_kw    REAL DEFAULT 0,
                updated_at       TEXT
            );
            CREATE TABLE IF NOT EXISTS weather_actuals (
                date                    TEXT PRIMARY KEY,
                shortwave_radiation_sum REAL,
                direct_radiation_sum    REAL,
                sunshine_duration       REAL,
                precipitation_sum       REAL,
                temperature_max         REAL,
                weather_code            INTEGER,
                uv_index_max            REAL
            );
            CREATE INDEX IF NOT EXISTS idx_daily_doy ON daily_energy(day_of_year);
        """)
        con.commit()
        con.close()
    log.info("DB ready at %s", SOLAR_DB_PATH)


def _db_query(sql: str, params: tuple = ()) -> list:
    with _db_lock:
        con = sqlite3.connect(SOLAR_DB_PATH)
        try:
            rows = con.execute(sql, params).fetchall()
        finally:
            con.close()
    return rows


def _db_execute(sql: str, params: tuple = ()) -> None:
    with _db_lock:
        con = sqlite3.connect(SOLAR_DB_PATH)
        try:
            con.execute(sql, params)
            con.commit()
        finally:
            con.close()


def _upsert_daily_energy(data: dict) -> None:
    today = datetime.now(TZ).date()
    _db_execute(
        """
        INSERT INTO daily_energy
            (date, day_of_year, month, year, production_kwh, consumption_kwh,
             grid_import_kwh, grid_export_kwh, battery_charge_kwh,
             battery_discharge_kwh, peak_power_kw, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(date) DO UPDATE SET
            production_kwh        = excluded.production_kwh,
            consumption_kwh       = excluded.consumption_kwh,
            grid_import_kwh       = excluded.grid_import_kwh,
            grid_export_kwh       = excluded.grid_export_kwh,
            battery_charge_kwh    = excluded.battery_charge_kwh,
            battery_discharge_kwh = excluded.battery_discharge_kwh,
            peak_power_kw         = MAX(peak_power_kw, excluded.peak_power_kw),
            updated_at            = excluded.updated_at
        """,
        (
            today.isoformat(),
            today.timetuple().tm_yday,
            today.month,
            today.year,
            data.get("production_kwh", 0),
            data.get("consumption_kwh", 0),
            data.get("grid_import_kwh", 0),
            data.get("grid_export_kwh", 0),
            data.get("battery_charge_kwh", 0),
            data.get("battery_discharge_kwh", 0),
            data.get("peak_power_kw", 0),
            datetime.now(TZ).isoformat(),
        ),
    )


def _upsert_weather(day: dict) -> None:
    _db_execute(
        """
        INSERT INTO weather_actuals
            (date, shortwave_radiation_sum, direct_radiation_sum,
             sunshine_duration, precipitation_sum, temperature_max,
             weather_code, uv_index_max)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(date) DO UPDATE SET
            shortwave_radiation_sum = excluded.shortwave_radiation_sum,
            direct_radiation_sum    = excluded.direct_radiation_sum,
            sunshine_duration       = excluded.sunshine_duration,
            precipitation_sum       = excluded.precipitation_sum,
            temperature_max         = excluded.temperature_max,
            weather_code            = excluded.weather_code,
            uv_index_max            = excluded.uv_index_max
        """,
        (
            day.get("date"),
            day.get("shortwave_radiation_sum"),
            day.get("direct_radiation_sum"),
            day.get("sunshine_duration"),
            day.get("precipitation_sum"),
            day.get("temperature_max"),
            day.get("weather_code"),
            day.get("uv_index_max"),
        ),
    )


# ── SolarEdge API ──────────────────────────────────────────────────────────────────────────

def _fetch_current_power_flow() -> dict | None:
    """Live power in kW: PV, grid, battery, load."""
    with _cache_lock:
        if _POWER_CACHE.get("ts", 0) and time.time() - _POWER_CACHE["ts"] < 60:
            return _POWER_CACHE.get("data")

    if not SOLAREDGE_API_KEY or not SOLAREDGE_SITE_ID:
        return None
    try:
        url = f"{SE_BASE}/site/{SOLAREDGE_SITE_ID}/currentPowerFlow"
        r = _http.get(url, params={"api_key": SOLAREDGE_API_KEY}, timeout=10)
        r.raise_for_status()
        raw = r.json().get("siteCurrentPowerFlow", {})

        pv_kw = raw.get("PV", {}).get("currentPower", 0.0)
        load_kw = raw.get("LOAD", {}).get("currentPower", 0.0)
        grid_kw = raw.get("GRID", {}).get("currentPower", 0.0)
        storage = raw.get("STORAGE", {})
        battery_kw = storage.get("currentPower", 0.0)
        battery_soc = storage.get("chargeLevel")  # None when no battery
        has_battery = bool(storage)

        conns = {
            (c.get("from", "").lower(), c.get("to", "").lower())
            for c in raw.get("connections", [])
        }
        grid_importing = ("grid", "load") in conns or ("grid", "storage") in conns
        battery_discharging = ("storage", "load") in conns or ("storage", "grid") in conns

        data = {
            "pv_kw": round(pv_kw, 2),
            "load_kw": round(load_kw, 2),
            "grid_kw": round(grid_kw, 2),
            "battery_kw": round(battery_kw, 2),
            "battery_soc": battery_soc,
            "has_battery": has_battery,
            "grid_importing": grid_importing,
            "battery_discharging": battery_discharging,
        }
        with _cache_lock:
            _POWER_CACHE["data"] = data
            _POWER_CACHE["ts"] = time.time()
        return data
    except Exception as exc:
        log.warning("currentPowerFlow: %s", exc)
        with _cache_lock:
            return _POWER_CACHE.get("data")


def _fetch_today_energy() -> dict | None:
    """Accumulated kWh totals for today from energyDetails."""
    with _cache_lock:
        if _ENERGY_CACHE.get("ts", 0) and time.time() - _ENERGY_CACHE["ts"] < 300:
            return _ENERGY_CACHE.get("data")

    if not SOLAREDGE_API_KEY or not SOLAREDGE_SITE_ID:
        return None
    try:
        today_str = datetime.now(TZ).strftime("%Y-%m-%d")
        url = f"{SE_BASE}/site/{SOLAREDGE_SITE_ID}/energyDetails"
        r = _http.get(
            url,
            params={
                "api_key": SOLAREDGE_API_KEY,
                "meters": "Production,Consumption,SelfConsumption,FeedIn,Purchased",
                "timeUnit": "DAY",
                "startTime": f"{today_str} 00:00:00",
                "endTime": f"{today_str} 23:59:59",
            },
            timeout=10,
        )
        r.raise_for_status()
        meters = r.json().get("energyDetails", {}).get("meters", [])

        vals: dict = {}
        for m in meters:
            mv = m.get("values", [])
            v = mv[0].get("value") or 0 if mv else 0
            vals[m.get("type", "")] = v / 1000  # Wh → kWh

        production = vals.get("Production", 0)
        consumption = vals.get("Consumption", 0)
        grid_import = vals.get("Purchased", 0)
        grid_export = vals.get("FeedIn", 0)
        self_cons = vals.get("SelfConsumption", 0)

        # Estimate battery flows from energy balance
        battery_discharge = max(0.0, consumption - grid_import - self_cons)
        battery_charge = max(0.0, production - self_cons - grid_export)

        data = {
            "production_kwh": round(production, 2),
            "consumption_kwh": round(consumption, 2),
            "grid_import_kwh": round(grid_import, 2),
            "grid_export_kwh": round(grid_export, 2),
            "battery_charge_kwh": round(battery_charge, 2),
            "battery_discharge_kwh": round(battery_discharge, 2),
        }
        with _cache_lock:
            _ENERGY_CACHE["data"] = data
            _ENERGY_CACHE["ts"] = time.time()
        _upsert_daily_energy(data)
        return data
    except Exception as exc:
        log.warning("energyDetails: %s", exc)
        with _cache_lock:
            return _ENERGY_CACHE.get("data")


# ── Open-Meteo ─────────────────────────────────────────────────────────────────────────────

_WMO_DE: dict[int, str] = {
    0: "Klar",
    1: "Überwiegend klar",
    2: "Teilweise bewölkt",
    3: "Bewölkt",
    45: "Nebel",
    48: "Gefrierender Nebel",
    51: "Leichter Nieselregen",
    53: "Nieselregen",
    55: "Starker Nieselregen",
    61: "Leichter Regen",
    63: "Regen",
    65: "Starker Regen",
    71: "Leichter Schnee",
    73: "Schnee",
    75: "Starker Schnee",
    77: "Schneekörner",
    80: "Leichte Regenschauer",
    81: "Regenschauer",
    82: "Starke Regenschauer",
    85: "Schneeschauer",
    86: "Starke Schneeschauer",
    95: "Gewitter",
    96: "Gewitter mit Hagel",
    99: "Gewitter mit starkem Hagel",
}


def _wmo(code) -> str:
    return _WMO_DE.get(int(code), "Unbekannt") if code is not None else "k.A."


def _fetch_weather() -> dict | None:
    """Current conditions + 4-day forecast from Open-Meteo."""
    with _cache_lock:
        if _WEATHER_CACHE.get("ts", 0) and time.time() - _WEATHER_CACHE["ts"] < 3600:
            return _WEATHER_CACHE.get("data")

    try:
        r = _http.get(
            OM_BASE,
            params={
                "latitude": SOLAR_LAT,
                "longitude": SOLAR_LON,
                "current": "temperature_2m,cloudcover,uv_index,weathercode",
                "daily": (
                    "shortwave_radiation_sum,direct_radiation_sum,precipitation_sum,"
                    "sunshine_duration,temperature_2m_max,weathercode,uv_index_max"
                ),
                "timezone": "Europe/Berlin",
                "forecast_days": 4,
            },
            timeout=10,
        )
        r.raise_for_status()
        raw = r.json()

        cur = raw.get("current", {})
        daily = raw.get("daily", {})
        times = daily.get("time", [])

        def _dget(key, i):
            lst = daily.get(key, [])
            return lst[i] if i < len(lst) else None

        days = [
            {
                "date": times[i],
                "shortwave_radiation_sum": _dget("shortwave_radiation_sum", i),
                "direct_radiation_sum": _dget("direct_radiation_sum", i),
                "sunshine_duration": _dget("sunshine_duration", i),
                "precipitation_sum": _dget("precipitation_sum", i),
                "temperature_max": _dget("temperature_2m_max", i),
                "weather_code": _dget("weathercode", i),
                "uv_index_max": _dget("uv_index_max", i),
            }
            for i in range(len(times))
        ]

        data = {
            "current": {
                "temperature": cur.get("temperature_2m"),
                "cloudcover": cur.get("cloudcover"),
                "uv_index": cur.get("uv_index"),
                "weather_code": cur.get("weathercode"),
            },
            "days": days,
        }
        with _cache_lock:
            _WEATHER_CACHE["data"] = data
            _WEATHER_CACHE["ts"] = time.time()

        if days:
            _upsert_weather(days[0])

        return data
    except Exception as exc:
        log.warning("Open-Meteo: %s", exc)
        with _cache_lock:
            return _WEATHER_CACHE.get("data")


# ── Forecast algorithm ───────────────────────────────────────────────────────────────────

def _forecast_kwh(target: date, weather_day: dict) -> float | None:
    """
    Estimate daily production for target date.
    Blends seasonal historical baseline with weather-corrected estimate.
    Returns None when fewer than 7 historical data points exist.
    """
    doy = target.timetuple().tm_yday

    hist = _db_query(
        """
        SELECT production_kwh FROM daily_energy
        WHERE ABS(day_of_year - ?) <= 21
          AND production_kwh > 0
          AND date < ?
        ORDER BY date DESC
        """,
        (doy, target.isoformat()),
    )
    if not hist:
        return None
    baseline = mean(r[0] for r in hist)

    radiation = weather_day.get("shortwave_radiation_sum")
    if radiation and radiation > 0:
        corr = _db_query(
            """
            SELECT d.production_kwh, w.shortwave_radiation_sum
            FROM daily_energy d
            JOIN weather_actuals w ON d.date = w.date
            WHERE w.shortwave_radiation_sum > 0
              AND d.production_kwh > 0
            ORDER BY d.date DESC LIMIT 90
            """,
        )
        if len(corr) >= 7:
            ratio = mean(p / r for p, r in corr)
            weather_est = ratio * radiation
            return round(max(0, 0.6 * weather_est + 0.4 * baseline), 1)

    return round(max(0, baseline), 1)


# ── UV recommendation ──────────────────────────────────────────────────────────────────────

def _uv_title(effective_uv: float) -> str:
    if effective_uv < 3:
        return "Sonnenschutz nicht nötig"
    elif effective_uv < 6:
        return "Sonnenschutz wäre nicht schlecht"
    elif effective_uv < 8:
        return "Sonnenschutz nötig"
    elif effective_uv < 11:
        return "Sonnenschutz auf jeden Fall"
    return "Sonnenschutz sonst wirst du zum Bratshähnchen"


def _uv_data(weather: dict, pv_kw: float) -> dict:
    cur = weather.get("current", {})
    today_w = weather.get("days", [{}])[0]

    current_uv = cur.get("uv_index") or 0.0
    max_uv = today_w.get("uv_index_max") or 0.0
    temperature = cur.get("temperature") or 0.0
    cloudcover = cur.get("cloudcover") or 0.0

    pv_ratio = min(1.0, pv_kw / SOLAR_PEAK_KW) if SOLAR_PEAK_KW > 0 else 0.0
    uv_from_pv = pv_ratio * 8.0
    ref_uv = max(current_uv, max_uv)
    effective_uv = 0.6 * ref_uv + 0.4 * uv_from_pv

    return {
        "title": _uv_title(effective_uv),
        "temperature": temperature,
        "cloudcover": cloudcover,
        "current_uv": current_uv,
        "max_uv": max_uv,
        "effective_uv": effective_uv,
    }


# ── Formatting helpers ───────────────────────────────────────────────────────────────────

def _fmt_kwh(v: float) -> str:
    return f"{v:.2f} kWh"


def _fmt_kw(v: float) -> str:
    return f"{v:.2f} kW"


def _signed_kwh(production: float, consumption: float) -> str:
    """Daily net balance with +/-/~ prefix."""
    balance = production - consumption
    if balance > 0.05:
        return f"+{balance:.2f} kWh"
    elif balance < -0.05:
        return f"-{abs(balance):.2f} kWh"
    return f"~{abs(balance):.2f} kWh"


def _live_signed(power: dict) -> str:
    """Current live power with +/-/~ prefix based on dominant energy source."""
    if power.get("grid_importing"):
        return f"-{power.get('grid_kw', 0):.2f} kW"
    if power.get("battery_discharging") and power.get("pv_kw", 0) < 0.1:
        return f"~{power.get('battery_kw', 0):.2f} kW"
    return f"+{power.get('pv_kw', 0):.2f} kW"


def _battery_hours_str(
    soc: float, battery_kw: float, consumption_kwh: float, hours_elapsed: float
) -> str:
    available = SOLAR_BATTERY_KWH * soc / 100
    if battery_kw > 0.1:
        rate = battery_kw
    elif hours_elapsed > 0.1 and consumption_kwh > 0:
        rate = consumption_kwh / hours_elapsed
    else:
        rate = 1.5
    hours = int(available / rate) if rate > 0 else 0
    return f"{hours} Std."


def _day_label(i: int) -> str:
    if i == 0:
        return "Heute"
    if i == 1:
        return "Morgen"
    if i == 2:
        return "Übermorgen"
    return "Der Tag danach"


# ── RSS feed builder ─────────────────────────────────────────────────────────────────────

def _xe(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _item(title: str, desc: str, guid: str) -> str:
    pub = datetime.now(TZ).strftime("%a, %d %b %Y %H:%M:%S %z")
    return (
        f"    <item>\n"
        f"      <title>{_xe(title)}</title>\n"
        f"      <description><![CDATA[{desc}]]></description>\n"
        f"      <pubDate>{pub}</pubDate>\n"
        f"      <guid isPermaLink='false'>{_xe(guid)}</guid>\n"
        f"    </item>\n"
    )


def _build_feed() -> str:
    now = datetime.now(TZ)
    today = now.date()

    power = _fetch_current_power_flow() or {}
    energy = _fetch_today_energy() or {}
    weather = _fetch_weather() or {}

    production = energy.get("production_kwh", 0.0)
    consumption = energy.get("consumption_kwh", 0.0)
    grid_import = energy.get("grid_import_kwh", 0.0)
    bat_discharge = energy.get("battery_discharge_kwh", 0.0)
    pv_kw = power.get("pv_kw", 0.0)
    battery_soc = power.get("battery_soc")
    has_battery = power.get("has_battery", False)
    weather_days = weather.get("days", [])

    # —— Item 1: Aktuell ———————————————————————————————————————————————
    daily_balance = _signed_kwh(production, consumption)
    title_aktuell = f"Aktuell | {daily_balance}"

    live = _live_signed(power) if power else "k.A."
    desc_lines = [
        f"Aktuell | {live}",
        f"Netzbezug | {_fmt_kwh(grid_import)}",
        f"Solarproduktion | {_fmt_kwh(production)}",
    ]
    if has_battery and battery_soc is not None:
        hours_elapsed = now.hour + now.minute / 60 or 1.0
        bat_h = _battery_hours_str(
            battery_soc, power.get("battery_kw", 0.0), consumption, hours_elapsed
        )
        desc_lines.append(
            f"Akku | {_fmt_kwh(bat_discharge)} > {battery_soc:.0f}% > {bat_h}"
        )
    desc_lines.append(f"Gesamtverbrauch | {daily_balance}")
    desc_lines.append("+ = Produktion | - = Netzbezug | ~ = Akku/Ausgeglichen")
    item_aktuell = _item(title_aktuell, "\n".join(desc_lines), "solar-aktuell")

    # —— Item 2: Vorhersage ——————————————————————————————————————————————
    today_fc = _forecast_kwh(today, weather_days[0]) if weather_days else None
    if today_fc is not None:
        title_fc = f"Vorhersage | {production:.1f} kWh von {today_fc} kWh"
    else:
        title_fc = "Vorhersage | Daten werden gesammelt…"

    fc_lines = []
    for i in range(4):
        target = today + timedelta(days=i)
        label = _day_label(i)
        wd = weather_days[i] if i < len(weather_days) else {}
        est = _forecast_kwh(target, wd)
        val = f"{est} kWh" if est is not None else "k.A."
        fc_lines.append(f"{label} | {val}")
    item_fc = _item(title_fc, "\n".join(fc_lines), "solar-vorhersage")

    # —— Item 3: Sonnenschutz ———————————————————————————————————————————
    uv = (
        _uv_data(weather, pv_kw)
        if weather
        else {"title": "Sonnenschutz", "temperature": 0, "cloudcover": 0, "current_uv": 0, "max_uv": 0}
    )
    uv_desc = [
        f"Temperatur | {uv['temperature']:.0f}°C",
        f"Bewölkung | {uv['cloudcover']:.0f}%",
        f"UV Index | {uv['current_uv']:.1f} | max {uv['max_uv']:.1f}",
    ]
    item_uv = _item(uv["title"], "\n".join(uv_desc), "solar-sonnenschutz")

    # —— Channel ——————————————————————————————————————————————————————
    now_rfc = now.strftime("%a, %d %b %Y %H:%M:%S %z")
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0">\n'
        '  <channel>\n'
        '    <title>Solar – SolarEdge</title>\n'
        '    <link>https://monitoringapi.solaredge.com</link>\n'
        '    <description>SolarEdge Solaranlage Monitoring</description>\n'
        '    <language>de-de</language>\n'
        f'    <lastBuildDate>{now_rfc}</lastBuildDate>\n'
        '    <ttl>15</ttl>\n'
        + item_aktuell
        + item_fc
        + item_uv
        + "  </channel>\n</rss>"
    )


# ── Background threads ───────────────────────────────────────────────────────────────────

def _refresh_feed() -> None:
    global _feed_xml
    try:
        xml = _build_feed()
        with _feed_lock:
            _feed_xml = xml
        log.info("Feed refreshed")
    except Exception as exc:
        log.error("Feed build error: %s", exc)


def _collector_loop() -> None:
    while True:
        try:
            _fetch_current_power_flow()
            _fetch_today_energy()
            _fetch_weather()
        except Exception as exc:
            log.error("Collector: %s", exc)
        time.sleep(15 * 60)


def _feed_loop() -> None:
    time.sleep(5)
    while True:
        _refresh_feed()
        time.sleep(15 * 60)


# ── Flask routes ───────────────────────────────────────────────────────────────────────────


@app.route("/feed.rss")
@app.route("/feed")
def feed():
    with _feed_lock:
        xml = _feed_xml
    if not xml:
        xml = _build_feed()
        with _feed_lock:
            _feed_xml = xml
    return Response(xml, mimetype="application/rss+xml; charset=utf-8")


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/stats")
def stats():
    row = _db_query(
        "SELECT COUNT(*), MIN(date), MAX(date), AVG(production_kwh) FROM daily_energy"
    )
    count, first, last, avg_prod = row[0] if row else (0, None, None, None)
    w_row = _db_query("SELECT COUNT(*) FROM weather_actuals")
    w_count = w_row[0][0] if w_row else 0
    return jsonify(
        {
            "daily_records": count,
            "first_date": first,
            "last_date": last,
            "avg_production_kwh": round(avg_prod, 2) if avg_prod else None,
            "weather_records": w_count,
            "config": {
                "peak_kw": SOLAR_PEAK_KW,
                "battery_kwh": SOLAR_BATTERY_KWH,
                "lat": SOLAR_LAT,
                "lon": SOLAR_LON,
            },
        }
    )


@app.route("/")
def index():
    return (
        "<h1>Solar RSS Feed – SolarEdge</h1>"
        "<p><a href='/feed.rss'>Feed</a> &bull; "
        "<a href='/health'>Health</a> &bull; "
        "<a href='/stats'>Stats</a></p>"
    )


# ── Startup ──────────────────────────────────────────────────────────────────────────────

_init_db()
threading.Thread(target=_collector_loop, daemon=True, name="collector").start()
threading.Thread(target=_feed_loop, daemon=True, name="feed-builder").start()
log.info("Solar RSS Feed started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
