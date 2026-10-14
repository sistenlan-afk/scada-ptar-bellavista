# -*- coding: utf-8 -*-
"""
SCADA PTAR BELLAVISTA · Servidor central de sincronización

Sirve la app móvil (index.html) y recibe/entrega los registros del celular y del
programa de escritorio. Habla el mismo protocolo que usa el escritorio:

    GET  /api/health                      -> estado del servidor
    POST /api/sync                        -> recibe registros {device_id, registros:[...]}
    GET  /api/registros?limit=2000        -> entrega registros (opcional: &desde_id=N)
    POST /api/datos                       -> envío "rápido" antiguo (se guarda aparte)

Ejecutar en local:   python app.py            (http://127.0.0.1:8000)
En Render:           gunicorn app:app --workers 1 --threads 4 --timeout 120

Base de datos:
  - Por defecto SQLite en ./data/scada_central.db  (carpeta DATA_DIR)
  - Si defines DATABASE_URL (PostgreSQL) usa esa base, que SÍ conserva los datos
    entre reinicios (necesita:  pip install psycopg2-binary)
"""
import os
import json
import uuid
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

from flask import Flask, request, jsonify, send_from_directory, abort, make_response

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
USE_PG = DATABASE_URL.startswith(("postgres://", "postgresql://"))

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 40 * 1024 * 1024   # fotos incluidas

# Archivos que se pueden descargar (lista blanca: nunca se expone el código ni la base)
ARCHIVOS_PUBLICOS = {
    "index.html": "text/html; charset=utf-8",
    "sw.js": "application/javascript; charset=utf-8",
    "manifest.webmanifest": "application/manifest+json",
    "icon-192.png": "image/png",
    "icon-512.png": "image/png",
}

_init_lock = threading.Lock()


# ------------------------------------------------------------------ base de datos
class _Cur:
    """Envuelve el cursor para escribir SQL con '?' en SQLite y PostgreSQL."""
    def __init__(self, cur):
        self.cur = cur

    def execute(self, sql, params=()):
        self.cur.execute(sql.replace("?", "%s") if USE_PG else sql, params)
        return self

    def fetchone(self):
        r = self.cur.fetchone()
        return dict(r) if r else None

    def fetchall(self):
        return [dict(r) for r in self.cur.fetchall()]


@contextmanager
def tx():
    if USE_PG:
        import psycopg2
        import psycopg2.extras
        con = psycopg2.connect(DATABASE_URL)
        cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        os.makedirs(DATA_DIR, exist_ok=True)
        con = sqlite3.connect(os.path.join(DATA_DIR, "scada_central.db"), timeout=20)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
    try:
        yield _Cur(cur)
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_db():
    pk = "BIGSERIAL PRIMARY KEY" if USE_PG else "INTEGER PRIMARY KEY AUTOINCREMENT"
    with tx() as c:
        c.execute(f"""CREATE TABLE IF NOT EXISTS registros (
            id {pk},
            sync_uuid TEXT NOT NULL UNIQUE,
            tabla TEXT NOT NULL,
            device_id TEXT,
            operador TEXT,
            fecha_hora TEXT,
            datos TEXT NOT NULL,
            recibido_en TEXT NOT NULL)""")
        c.execute(f"""CREATE TABLE IF NOT EXISTS legacy_datos (
            id {pk},
            tabla TEXT,
            payload TEXT,
            recibido_en TEXT)""")
        c.execute("CREATE TABLE IF NOT EXISTS meta (clave TEXT PRIMARY KEY, valor TEXT NOT NULL)")
        c.execute("INSERT INTO meta(clave, valor) VALUES(?, ?) ON CONFLICT(clave) DO NOTHING",
                  ("instancia", uuid.uuid4().hex))


def _db_existe():
    return USE_PG or os.path.exists(os.path.join(DATA_DIR, "scada_central.db"))


def _asegurar_db():
    """Crea las tablas la primera vez y también si el archivo de la base desapareció
    (por ejemplo, tras un reinicio en un servidor con disco temporal)."""
    if not getattr(app, "_db_lista", False) or not _db_existe():
        with _init_lock:
            if not getattr(app, "_db_lista", False) or not _db_existe():
                init_db()
                app._db_lista = True


def instancia_id():
    with tx() as c:
        r = c.execute("SELECT valor FROM meta WHERE clave=?", ("instancia",)).fetchone()
    return r["valor"] if r else ""


def ahora():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ------------------------------------------------------------------ utilidades HTTP
@app.before_request
def _antes():
    if request.path.startswith("/api/"):
        _asegurar_db()


@app.after_request
def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    if request.path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/api/<path:_ruta>", methods=["OPTIONS"])
def _preflight(_ruta):
    return make_response("", 204)


def _fecha_hora(datos):
    f = str(datos.get("fecha") or "").strip()
    h = str(datos.get("hora") or "").strip()
    return (f + " " + h).strip()


# ------------------------------------------------------------------ API
@app.route("/api/health")
def health():
    with tx() as c:
        total = c.execute("SELECT COUNT(*) AS n FROM registros").fetchone()["n"]
        filas = c.execute("SELECT tabla, COUNT(*) AS n FROM registros GROUP BY tabla").fetchall()
    return jsonify({
        "status": "OK",
        "instancia": instancia_id(),
        "registros": total,
        "por_tabla": {f["tabla"]: f["n"] for f in filas},
        "motor": "postgresql" if USE_PG else "sqlite",
        "hora_servidor": ahora(),
    })


@app.route("/api/sync", methods=["POST"])
def sync():
    cuerpo = request.get_json(silent=True)
    if not isinstance(cuerpo, dict) or not isinstance(cuerpo.get("registros"), list):
        return jsonify({"ok": False, "error": "Se esperaba {device_id, registros:[...]}"}), 400
    dispositivo = str(cuerpo.get("device_id") or "")[:80]
    confirmados = []
    rechazados = 0
    with tx() as c:
        for r in cuerpo["registros"]:
            if not isinstance(r, dict):
                rechazados += 1
                continue
            uid = str(r.get("sync_uuid") or "").strip()
            tabla = str(r.get("tabla") or "").strip()[:60]
            datos = r.get("datos") if isinstance(r.get("datos"), dict) else {}
            if not uid or not tabla:
                rechazados += 1
                continue
            c.execute(
                """INSERT INTO registros(sync_uuid, tabla, device_id, operador, fecha_hora, datos, recibido_en)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(sync_uuid) DO UPDATE SET
                       tabla=excluded.tabla, device_id=excluded.device_id,
                       operador=excluded.operador, fecha_hora=excluded.fecha_hora,
                       datos=excluded.datos""",
                (uid, tabla, str(r.get("device_id") or dispositivo)[:80],
                 str(r.get("operador") or "")[:120], _fecha_hora(datos),
                 json.dumps(datos, ensure_ascii=False), ahora()))
            confirmados.append(uid)
    return jsonify({"ok": True, "recibidos": len(cuerpo["registros"]),
                    "confirmados": confirmados, "rechazados": rechazados,
                    "instancia": instancia_id()})


@app.route("/api/registros")
def registros():
    try:
        limite = max(1, min(int(request.args.get("limit", 2000)), 5000))
        desde_id = max(0, int(request.args.get("desde_id", 0)))
    except ValueError:
        return jsonify({"ok": False, "error": "limit/desde_id inválidos"}), 400
    con_fotos = request.args.get("fotos") == "1"
    with tx() as c:
        if desde_id > 0:
            filas = c.execute("SELECT * FROM registros WHERE id > ? ORDER BY id ASC LIMIT ?",
                              (desde_id, limite)).fetchall()
        else:
            filas = c.execute("SELECT * FROM (SELECT * FROM registros ORDER BY id DESC LIMIT ?) AS t ORDER BY id ASC",
                              (limite,)).fetchall()
        total = c.execute("SELECT COUNT(*) AS n FROM registros").fetchone()["n"]
    salida = []
    for f in filas:
        try:
            datos = json.loads(f["datos"])
        except ValueError:
            datos = {}
        if not con_fotos and isinstance(datos.get("fotos"), list):
            datos["n_fotos"] = len(datos.pop("fotos"))
        salida.append({"id": f["id"], "sync_uuid": f["sync_uuid"], "tabla": f["tabla"],
                       "device_id": f["device_id"], "operador": f["operador"],
                       "fecha_hora": f["fecha_hora"], "datos": datos})
    return jsonify({"ok": True, "registros": salida, "total": total,
                    "ultimo_id": salida[-1]["id"] if salida else desde_id,
                    "instancia": instancia_id()})


@app.route("/api/datos", methods=["POST"])
def datos_legacy():
    """Envío rápido antiguo del escritorio. Se guarda aparte y NO se reparte a los demás
    dispositivos: si se repartiera, el escritorio lo volvería a descargar y quedaría duplicado
    (el registro de verdad llega por /api/sync con su identificador único)."""
    cuerpo = request.get_json(silent=True) or {}
    with tx() as c:
        c.execute("INSERT INTO legacy_datos(tabla, payload, recibido_en) VALUES(?,?,?)",
                  (str(cuerpo.get("tabla") or "")[:60], json.dumps(cuerpo, ensure_ascii=False), ahora()))
    return jsonify({"ok": True})


# ------------------------------------------------------------------ app móvil (archivos estáticos)
def _servir(nombre):
    resp = send_from_directory(BASE_DIR, nombre, mimetype=ARCHIVOS_PUBLICOS[nombre].split(";")[0])
    if nombre in ("index.html", "sw.js", "manifest.webmanifest"):
        resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.route("/")
def inicio():
    return _servir("index.html")


@app.route("/<path:nombre>")
def archivos(nombre):
    if nombre in ARCHIVOS_PUBLICOS:
        return _servir(nombre)
    abort(404)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), debug=False)
