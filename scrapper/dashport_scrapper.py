#!/usr/bin/env python3
"""Scrapper de resultados para eventos en Dashport.

Uso:
    python scrapper/dashport_scrapper.py --evento 1056 --out resultados_1056.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from html import unescape
from html.parser import HTMLParser
from typing import Any, Iterable
from urllib.error import HTTPError
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
from urllib.request import ProxyHandler, Request, build_opener

BASE_URL = "https://dashport.run/live/evento_{evento}"
FIREBASE_TOKEN_CACHE: dict[str, str] = {}

POSITION_KEYS = {
    "puesto",
    "posicion",
    "posición",
    "position",
    "rank",
    "place",
    "overallrank",
    "overallposition",
    "overall",
    "general",
    "posgral",
    "posiciongeneral",
}
BIB_KEYS = {"dorsal", "bib", "bibnumber", "numero", "número", "runnernumber", "number"}
NAME_KEYS = {
    "nombre",
    "atleta",
    "runner",
    "competidor",
    "fullname",
    "athlete",
    "name",
    "participantname",
    "participante",
    "corredor",
    "runnername",
}
CATEGORY_KEYS = {"categoria", "categoría", "category", "categoryname", "division", "cat", "rama", "gender"}
TIME_KEYS = {
    "tiempo",
    "time",
    "nettime",
    "chiptime",
    "officialtime",
    "guntime",
    "resulttime",
    "finishtime",
    "timefinal",
    "totaltime",
    "elapsed",
    "marca",
}


@dataclass
class Resultado:
    posicion: str
    dorsal: str
    atleta: str
    categoria: str
    tiempo: str


class TableTextParser(HTMLParser):
    """Extrae texto de filas en tablas HTML."""

    def __init__(self) -> None:
        super().__init__()
        self.in_tr = False
        self.in_td = False
        self.current_cell: list[str] = []
        self.current_row: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self.in_tr = True
            self.current_row = []
        elif self.in_tr and tag in ("td", "th"):
            self.in_td = True
            self.current_cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th") and self.in_tr and self.in_td:
            cell = unescape("".join(self.current_cell)).strip()
            self.current_row.append(re.sub(r"\s+", " ", cell))
            self.in_td = False
        elif tag == "tr" and self.in_tr:
            self.in_tr = False
            if any(c.strip() for c in self.current_row):
                self.rows.append(self.current_row)

    def handle_data(self, data: str) -> None:
        if self.in_td:
            self.current_cell.append(data)


def _build_request(url: str) -> Request:
    return Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
        },
    )


def fetch_text(url: str, timeout: int = 30, disable_proxy: bool = False) -> str:
    opener = build_opener(ProxyHandler({}) if disable_proxy else ProxyHandler())
    with opener.open(_build_request(url), timeout=timeout) as response:
        return response.read().decode("utf-8", "ignore")


def extract_next_data(html: str) -> dict[str, Any] | None:
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        flags=re.DOTALL,
    )
    return json.loads(match.group(1)) if match else None


def extract_next_data_json_url(html: str, page_url: str) -> str | None:
    match = re.search(r'"(/_next/data/[^"]*?/live/evento_\d+\.json[^"]*)"', html)
    if not match:
        return None
    return urljoin(page_url, match.group(1).encode("utf-8").decode("unicode_escape"))


def extract_script_json_blocks(html: str) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for body in re.findall(r"<script[^>]*>(.*?)</script>", html, flags=re.DOTALL | re.IGNORECASE):
        body = body.strip()
        if not body:
            continue

        # Caso: script es JSON puro
        if body.startswith("{") or body.startswith("["):
            try:
                parsed = json.loads(body)
                if isinstance(parsed, dict):
                    blocks.append(parsed)
                continue
            except json.JSONDecodeError:
                pass

        # Caso: asignaciones JS con objeto JSON (window.__DATA__ = {...})
        for m in re.findall(r"=\s*(\{.*?\}|\[.*?\])\s*;", body, flags=re.DOTALL):
            try:
                parsed = json.loads(m)
                if isinstance(parsed, dict):
                    blocks.append(parsed)
            except json.JSONDecodeError:
                continue
    return blocks


def discover_json_urls(html: str, page_url: str, evento: str) -> list[str]:
    candidates = set()
    banned_hosts = {"fonts.googleapis.com", "fonts.gstatic.com"}

    for m in re.findall(r'https?://[^"\'\s]+', html):
        low = m.lower()
        parsed = urlparse(m)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        if host in banned_hosts:
            continue
        if (
            path.endswith(".json")
            or "/api/" in path
            or "firebaseio.com" in host
            or "firebasedatabase.app" in host
            or f"evento_{evento}" in low
            or any(token in path for token in ("result", "clasif", "rank"))
        ):
            candidates.add(m)

    for m in re.findall(r'/[^"\'\s]+', html):
        low = m.lower()
        if (
            low.endswith(".json")
            or "/api/" in low
            or f"evento_{evento}" in low
            or any(token in low for token in ("result", "clasif", "rank"))
        ):
            candidates.add(urljoin(page_url, m))

    return sorted(candidates)


def extract_firebase_config(html: str) -> dict[str, str]:
    cfg: dict[str, str] = {}
    patterns = {
        "apiKey": r'["\']apiKey["\']\s*:\s*["\']([^"\']+)["\']',
        "databaseURL": r'["\']databaseURL["\']\s*:\s*["\'](https://[^"\']+)["\']',
    }
    for key, pattern in patterns.items():
        m = re.search(pattern, html)
        if m:
            cfg[key] = m.group(1).rstrip("/")
    return cfg


def discover_firebase_urls(html: str, evento: str, database_url: str | None = None) -> list[str]:
    bases: set[str] = set()
    if database_url:
        bases.add(database_url.rstrip("/"))

    for m in re.findall(r'https://[a-z0-9.-]+(?:firebaseio\.com|firebasedatabase\.app)', html, flags=re.IGNORECASE):
        bases.add(m.rstrip("/"))

    if not bases:
        # Base conocida usada históricamente por Dashport/Kmetas.
        bases.add("https://kmetasfirebase.firebaseio.com")

    suffixes = [
        f"/evento_{evento}.json",
        f"/evento{evento}.json",
        f"/{evento}.json",
        f"/evento_{evento}/resultados.json",
        f"/evento_{evento}/results.json",
        f"/eventos/evento_{evento}.json",
        f"/eventos/{evento}.json",
        f"/eventos/evento_{evento}/resultados.json",
        f"/resultados/evento_{evento}.json",
        f"/resultados/{evento}.json",
        f"/resultados/evento{evento}.json",
        f"/resultadosEvento/{evento}.json",
        f"/resultadosEventos/{evento}.json",
        f"/resultados_evento/{evento}.json",
        f"/clasificacion/evento_{evento}.json",
        f"/clasificacion/{evento}.json",
        f"/clasificaciones/evento_{evento}.json",
        f"/clasificaciones/{evento}.json",
        f"/ranking/evento_{evento}.json",
        f"/ranking/{evento}.json",
        f"/rankings/{evento}.json",
        f"/tiempos/evento_{evento}.json",
        f"/tiempos/{evento}.json",
        f"/eventosLista/evento_{evento}.json",
    ]
    out: set[str] = set()
    for base in bases:
        for suffix in suffixes:
            out.add(f"{base}{suffix}")
    return sorted(out)


def append_query_params(url: str, params: dict[str, str]) -> str:
    parsed = urlparse(url)
    current = dict(parse_qsl(parsed.query, keep_blank_values=True))
    current.update(params)
    return urlunparse(parsed._replace(query=urlencode(current)))


def firebase_base_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    if not ("firebaseio.com" in host or "firebasedatabase.app" in host):
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


def discover_firebase_nodes(base_url: str, evento: str, disable_proxy: bool, firebase_api_key: str | None) -> list[str]:
    url = append_query_params(f"{base_url}/.json", {"shallow": "true"})
    data = fetch_json_with_optional_firebase_auth(
        url,
        disable_proxy=disable_proxy,
        firebase_api_key=firebase_api_key,
    )
    if not isinstance(data, dict):
        return []

    keys = [str(k) for k in data.keys()]
    evento_tokens = {evento, f"evento_{evento}", f"evento{evento}"}
    interesting = []
    for key in keys:
        low = key.lower()
        if any(tok in low for tok in ("result", "clasif", "rank", "tiempo", "evento")) or any(
            tok in low for tok in evento_tokens
        ):
            interesting.append(key)

    out: set[str] = set()
    for key in interesting[:120]:
        out.add(f"{base_url}/{key}.json")
        out.add(f"{base_url}/{key}/evento_{evento}.json")
        out.add(f"{base_url}/{key}/evento{evento}.json")
        out.add(f"{base_url}/{key}/{evento}.json")
        out.add(f"{base_url}/{key}/evento_{evento}/resultados.json")
        out.add(f"{base_url}/{key}/evento_{evento}/results.json")
    return sorted(out)


def firebase_anonymous_token(api_key: str, disable_proxy: bool = False, timeout: int = 30) -> str | None:
    if api_key in FIREBASE_TOKEN_CACHE:
        return FIREBASE_TOKEN_CACHE[api_key]

    endpoint = f"https://identitytoolkit.googleapis.com/v1/accounts:signUp?key={api_key}"
    payload = json.dumps({"returnSecureToken": True}).encode("utf-8")
    opener = build_opener(ProxyHandler({}) if disable_proxy else ProxyHandler())
    req = Request(
        endpoint,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        method="POST",
    )
    try:
        with opener.open(req, timeout=timeout) as response:
            data = json.loads(response.read().decode("utf-8", "ignore"))
            token = data.get("idToken")
            if token:
                FIREBASE_TOKEN_CACHE[api_key] = token
                return token
    except Exception:
        return None
    return None


def fetch_json_with_optional_firebase_auth(
    url: str,
    disable_proxy: bool = False,
    firebase_api_key: str | None = None,
) -> Any | None:
    try:
        return json.loads(fetch_text(url, disable_proxy=disable_proxy))
    except HTTPError as exc:
        if exc.code != 401 or not firebase_api_key:
            return None
        token = firebase_anonymous_token(firebase_api_key, disable_proxy=disable_proxy)
        if not token:
            return None
        authed_url = append_query_params(url, {"auth": token})
        try:
            return json.loads(fetch_text(authed_url, disable_proxy=disable_proxy))
        except Exception:
            return None
    except Exception:
        return None


def walk_objects(data: Any) -> Iterable[dict[str, Any]]:
    if isinstance(data, dict):
        yield data
        for value in data.values():
            yield from walk_objects(value)
    elif isinstance(data, list):
        for item in data:
            yield from walk_objects(item)


def _pick(k: dict[str, Any], keys: set[str]) -> Any:
    for key in keys:
        if key in k and k[key] not in (None, ""):
            return k[key]
    return None


def _str_or_empty(value: Any) -> str:
    return "" if value is None else str(value)


def map_resultado(obj: dict[str, Any]) -> Resultado | None:
    normalized = {re.sub(r"[^a-z0-9áéíóúñ]", "", key.lower()): value for key, value in obj.items()}

    posicion = _pick(normalized, POSITION_KEYS)
    dorsal = _pick(normalized, BIB_KEYS)
    atleta = _pick(normalized, NAME_KEYS)
    categoria = _pick(normalized, CATEGORY_KEYS)
    tiempo = _pick(normalized, TIME_KEYS)

    if not atleta:
        first = _pick(normalized, {"firstname", "nombre"})
        last = _pick(normalized, {"lastname", "apellido"})
        if first or last:
            atleta = " ".join(str(x).strip() for x in (first, last) if x)

    # Acepta registros que tengan al menos nombre + tiempo o posición + tiempo.
    # Algunos JSON de proveedor no incluyen dorsal/categoría.
    if not ((atleta and tiempo) or (posicion and tiempo) or (atleta and posicion)):
        return None

    return Resultado(
        posicion=_str_or_empty(posicion),
        dorsal=_str_or_empty(dorsal),
        atleta=_str_or_empty(atleta),
        categoria=_str_or_empty(categoria),
        tiempo=_str_or_empty(tiempo),
    )


def resultados_from_json_data(data: Any) -> list[Resultado]:
    unique: dict[tuple[str, str, str, str, str], Resultado] = {}
    for obj in walk_objects(data):
        resultado = map_resultado(obj)
        if not resultado:
            continue
        key = (resultado.posicion, resultado.dorsal, resultado.atleta, resultado.categoria, resultado.tiempo)
        unique[key] = resultado
    return list(unique.values())


def merge_resultados(*groups: list[Resultado]) -> list[Resultado]:
    unique: dict[tuple[str, str, str, str, str], Resultado] = {}
    for group in groups:
        for r in group:
            key = (r.posicion, r.dorsal, r.atleta, r.categoria, r.tiempo)
            unique[key] = r
    return list(unique.values())


def resultados_from_tables(html: str) -> list[Resultado]:
    parser = TableTextParser()
    parser.feed(html)
    resultados: list[Resultado] = []

    for row in parser.rows:
        if len(row) < 4:
            continue
        first = row[0].strip().lower()
        if first in {"pos", "posición", "puesto", "position", "rank"}:
            continue
        valores = row + [""] * (5 - len(row))
        resultados.append(Resultado(*valores[:5]))

    return resultados


def _extract_from_sources(
    html: str,
    page_url: str,
    evento: str,
    disable_proxy: bool,
    debug: bool = False,
) -> list[Resultado]:
    firebase_cfg = extract_firebase_config(html)
    firebase_api_key = firebase_cfg.get("apiKey")
    firebase_db_url = firebase_cfg.get("databaseURL")
    if debug and firebase_cfg:
        print(
            "[debug] config Firebase detectada: "
            f"apiKey={'si' if firebase_api_key else 'no'}, "
            f"databaseURL={firebase_db_url or 'n/a'}",
            file=sys.stderr,
        )
    elif debug:
        print("[debug] no se detectó config Firebase en HTML", file=sys.stderr)

    # 1) __NEXT_DATA__
    next_data = extract_next_data(html)
    if next_data:
        r = resultados_from_json_data(next_data)
        if r:
            if debug:
                print(f"[debug] resultados desde __NEXT_DATA__: {len(r)}", file=sys.stderr)
            return r

    # 2) JSON de /_next/data
    json_url = extract_next_data_json_url(html, page_url)
    if json_url:
        try:
            if debug:
                print(f"[debug] intentando _next/data: {json_url}", file=sys.stderr)
            preloaded = json.loads(fetch_text(json_url, disable_proxy=disable_proxy))
            r = resultados_from_json_data(preloaded)
            if r:
                if debug:
                    print(f"[debug] resultados desde _next/data: {len(r)}", file=sys.stderr)
                return r
        except Exception as exc:
            if debug:
                print(f"[debug] fallo _next/data: {exc}", file=sys.stderr)

    # 3) Bloques JSON inline en scripts
    for block in extract_script_json_blocks(html):
        r = resultados_from_json_data(block)
        if r:
            if debug:
                print(f"[debug] resultados desde script inline: {len(r)}", file=sys.stderr)
            return r

    # 4) URLs API/JSON descubiertas en la página
    discovered = discover_json_urls(html, page_url, evento)
    if debug:
        print(f"[debug] URLs JSON descubiertas: {len(discovered)}", file=sys.stderr)
    for candidate in discovered:
        try:
            if debug:
                print(f"[debug] intentando URL descubierta: {candidate}", file=sys.stderr)
            parsed = fetch_json_with_optional_firebase_auth(
                candidate,
                disable_proxy=disable_proxy,
                firebase_api_key=firebase_api_key,
            )
            if isinstance(parsed, (dict, list)):
                r = resultados_from_json_data(parsed)
                if r:
                    if debug:
                        print(f"[debug] resultados desde URL descubierta: {len(r)}", file=sys.stderr)
                    return r
            elif debug:
                print(f"[debug] URL descubierta sin JSON útil: {candidate}", file=sys.stderr)
        except Exception as exc:
            if debug:
                print(f"[debug] fallo URL descubierta {candidate}: {exc}", file=sys.stderr)
            continue

    # 5) Rutas típicas de Firebase (fallback robusto para Dashport/Kmetas)
    firebase_candidates = discover_firebase_urls(html, evento, database_url=firebase_db_url)
    if debug:
        print(f"[debug] URLs Firebase candidatas: {len(firebase_candidates)}", file=sys.stderr)
    for candidate in firebase_candidates:
        try:
            if debug:
                print(f"[debug] intentando Firebase: {candidate}", file=sys.stderr)
            parsed = fetch_json_with_optional_firebase_auth(
                candidate,
                disable_proxy=disable_proxy,
                firebase_api_key=firebase_api_key,
            )
            if isinstance(parsed, (dict, list)):
                r = resultados_from_json_data(parsed)
                if r:
                    if debug:
                        print(f"[debug] resultados desde Firebase: {len(r)}", file=sys.stderr)
                    return r
                if debug:
                    # Útil para distinguir 401/empty de "sí hay JSON pero es metadata".
                    if isinstance(parsed, dict):
                        print(
                            f"[debug] Firebase JSON sin resultados ({candidate}) keys: {list(parsed.keys())[:8]}",
                            file=sys.stderr,
                        )
            elif debug:
                print(f"[debug] Firebase sin JSON útil: {candidate}", file=sys.stderr)
        except Exception as exc:
            if debug:
                print(f"[debug] fallo Firebase {candidate}: {exc}", file=sys.stderr)
            continue

    # 6) Descubrimiento de nodos Firebase (shallow) + intento por evento
    firebase_bases = set()
    for candidate in firebase_candidates:
        base = firebase_base_from_url(candidate)
        if base:
            firebase_bases.add(base)
    if firebase_db_url:
        firebase_bases.add(firebase_db_url.rstrip("/"))

    if debug:
        print(f"[debug] bases Firebase para discovery: {len(firebase_bases)}", file=sys.stderr)

    discovered_nodes: set[str] = set()
    for base in sorted(firebase_bases):
        nodes = discover_firebase_nodes(
            base,
            evento,
            disable_proxy=disable_proxy,
            firebase_api_key=firebase_api_key,
        )
        if debug:
            print(f"[debug] nodos Firebase descubiertos en {base}: {len(nodes)}", file=sys.stderr)
        discovered_nodes.update(nodes)

    for candidate in sorted(discovered_nodes):
        try:
            if debug:
                print(f"[debug] intentando Firebase discovery: {candidate}", file=sys.stderr)
            parsed = fetch_json_with_optional_firebase_auth(
                candidate,
                disable_proxy=disable_proxy,
                firebase_api_key=firebase_api_key,
            )
            if isinstance(parsed, (dict, list)):
                r = resultados_from_json_data(parsed)
                if r:
                    if debug:
                        print(f"[debug] resultados desde Firebase discovery: {len(r)}", file=sys.stderr)
                    return r
        except Exception as exc:
            if debug:
                print(f"[debug] fallo Firebase discovery {candidate}: {exc}", file=sys.stderr)
            continue

    # 7) Tablas HTML
    tables = resultados_from_tables(html)
    if debug:
        print(f"[debug] resultados desde tablas HTML: {len(tables)}", file=sys.stderr)
    return tables


def _extract_with_playwright(page_url: str, evento: str, debug: bool = False) -> list[Resultado]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception:
        if debug:
            print("[debug] Playwright no está disponible en el entorno Python", file=sys.stderr)
        return []

    captured_payloads: list[Any] = []
    rendered_html = ""

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            def on_response(response: Any) -> None:
                url = response.url.lower()
                ctype = (response.headers or {}).get("content-type", "").lower()
                looks_json = "json" in ctype or any(
                    token in url
                    for token in (
                        "firebaseio.com",
                        "firebasedatabase.app",
                        "googleapis.com",
                        "result",
                        "clasif",
                        "rank",
                        f"evento_{evento}",
                    )
                )
                if not looks_json:
                    return
                try:
                    text = response.text()
                    if text and text[:1] in "{[":
                        captured_payloads.append(json.loads(text))
                except Exception:
                    return

            page.on("response", on_response)
            try:
                page.goto(page_url, wait_until="domcontentloaded", timeout=120000)
                page.wait_for_timeout(12000)
            except PlaywrightTimeoutError:
                pass

            rendered_html = page.content()
            browser.close()
    except Exception as exc:
        if debug:
            print(f"[debug] Playwright fallback falló: {exc}", file=sys.stderr)
        return []

    from_payloads: list[Resultado] = []
    for payload in captured_payloads:
        from_payloads.extend(resultados_from_json_data(payload))

    from_tables = resultados_from_tables(rendered_html) if rendered_html else []
    merged = merge_resultados(from_payloads, from_tables)
    if debug:
        print(
            f"[debug] Playwright capturó payloads: {len(captured_payloads)}, "
            f"resultados JSON: {len(from_payloads)}, tablas render: {len(from_tables)}, "
            f"únicos: {len(merged)}",
            file=sys.stderr,
        )
    return merged


def obtener_resultados(
    evento: str,
    disable_proxy: bool = False,
    debug: bool = False,
    page_url: str | None = None,
    use_playwright: bool = True,
) -> list[Resultado]:
    page_url = page_url or BASE_URL.format(evento=evento)
    if debug:
        print(f"[debug] cargando página principal: {page_url}", file=sys.stderr)
    html = fetch_text(page_url, disable_proxy=disable_proxy)
    resultados = _extract_from_sources(html, page_url, evento, disable_proxy, debug=debug)
    if resultados:
        return resultados
    if use_playwright:
        if debug:
            print("[debug] sin resultados en scraping estático, activando Playwright fallback", file=sys.stderr)
        pw_resultados = _extract_with_playwright(page_url, evento=evento, debug=debug)
        if pw_resultados:
            return pw_resultados
    return resultados


def main() -> int:
    parser = argparse.ArgumentParser(description="Obtiene resultados de Dashport")
    parser.add_argument("--evento", default="1056", help="ID del evento (ej. 1056)")
    parser.add_argument("--url", default=None, help="URL completa del evento (opcional)")
    parser.add_argument("--out", default="resultados.json", help="Archivo de salida JSON")
    parser.add_argument("--sin-proxy", action="store_true", help="Intenta conexión directa")
    parser.add_argument("--debug", action="store_true", help="Muestra fuentes y URLs de extracción")
    parser.add_argument(
        "--sin-playwright",
        action="store_true",
        help="No usar fallback Playwright si el scraping estático no devuelve resultados",
    )
    args = parser.parse_args()

    if args.sin_proxy:
        for key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
            os.environ.pop(key, None)

    try:
        resultados = obtener_resultados(
            args.evento,
            disable_proxy=args.sin_proxy,
            debug=args.debug,
            page_url=args.url,
            use_playwright=not args.sin_playwright,
        )
    except Exception as exc:
        print(f"Error al obtener resultados: {exc}", file=sys.stderr)
        return 1

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in resultados], f, ensure_ascii=False, indent=2)

    print(f"Se guardaron {len(resultados)} resultados en {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
