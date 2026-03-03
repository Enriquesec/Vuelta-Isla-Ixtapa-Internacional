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
from urllib.error import URLError
from urllib.parse import urljoin
from urllib.request import ProxyHandler, Request, build_opener

BASE_URL = "https://dashport.run/live/evento_{evento}"


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

    req = _build_request(url)
    with opener.open(req, timeout=timeout) as response:
        return response.read().decode("utf-8", "ignore")


def extract_next_data(html: str) -> dict[str, Any] | None:
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        flags=re.DOTALL,
    )
    if not match:
        return None
    return json.loads(match.group(1))


def extract_next_data_json_url(html: str, page_url: str) -> str | None:
    # Next.js suele exponer el JSON con datos precargados en /_next/data/<build>/live/evento_xxx.json
    match = re.search(r'"(/_next/data/[^"]*?/live/evento_\d+\.json[^"]*)"', html)
    if not match:
        return None
    return urljoin(page_url, match.group(1).encode("utf-8").decode("unicode_escape"))


def walk_objects(data: Any) -> Iterable[dict[str, Any]]:
    if isinstance(data, dict):
        yield data
        for value in data.values():
            yield from walk_objects(value)
    elif isinstance(data, list):
        for item in data:
            yield from walk_objects(item)


def map_resultado(obj: dict[str, Any]) -> Resultado | None:
    k = {key.lower(): value for key, value in obj.items()}

    posicion = k.get("puesto") or k.get("posicion") or k.get("posición") or k.get("position")
    tiempo = k.get("tiempo") or k.get("time")

    # Evita falsos positivos de objetos no relacionados con resultados.
    if posicion is None and tiempo is None:
        return None

    dorsal = k.get("dorsal") or k.get("bib") or k.get("numero") or k.get("número")
    atleta = k.get("nombre") or k.get("atleta") or k.get("runner") or k.get("competidor")
    categoria = k.get("categoria") or k.get("categoría") or k.get("category")

    return Resultado(
        posicion="" if posicion is None else str(posicion),
        dorsal="" if dorsal is None else str(dorsal),
        atleta="" if atleta is None else str(atleta),
        categoria="" if categoria is None else str(categoria),
        tiempo="" if tiempo is None else str(tiempo),
    )


def resultados_from_json_data(data: dict[str, Any]) -> list[Resultado]:
    unique: dict[tuple[str, str, str, str, str], Resultado] = {}
    for obj in walk_objects(data):
        resultado = map_resultado(obj)
        if not resultado:
            continue
        key = (
            resultado.posicion,
            resultado.dorsal,
            resultado.atleta,
            resultado.categoria,
            resultado.tiempo,
        )
        if any(key):
            unique[key] = resultado
    return list(unique.values())


def resultados_from_tables(html: str) -> list[Resultado]:
    parser = TableTextParser()
    parser.feed(html)

    resultados: list[Resultado] = []
    for row in parser.rows:
        if len(row) < 4:
            continue
        if row[0].lower() in {"pos", "posición", "puesto", "position"}:
            continue

        valores = row + [""] * (5 - len(row))
        resultados.append(
            Resultado(
                posicion=valores[0],
                dorsal=valores[1],
                atleta=valores[2],
                categoria=valores[3],
                tiempo=valores[4],
            )
        )
    return resultados


def obtener_resultados(evento: str, disable_proxy: bool = False) -> list[Resultado]:
    page_url = BASE_URL.format(evento=evento)
    html = fetch_text(page_url, disable_proxy=disable_proxy)

    # Estrategia 1: __NEXT_DATA__ embebido
    next_data = extract_next_data(html)
    if next_data:
        resultados = resultados_from_json_data(next_data)
        if resultados:
            return resultados

    # Estrategia 2: JSON precargado en /_next/data/.../live/evento_xxx.json
    json_url = extract_next_data_json_url(html, page_url)
    if json_url:
        try:
            preloaded = json.loads(fetch_text(json_url, disable_proxy=disable_proxy))
            resultados = resultados_from_json_data(preloaded)
            if resultados:
                return resultados
        except (URLError, json.JSONDecodeError):
            pass

    # Estrategia 3: parseo de tablas HTML
    resultados = resultados_from_tables(html)
    if resultados:
        return resultados

    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="Obtiene resultados de Dashport")
    parser.add_argument("--evento", default="1056", help="ID del evento (ej. 1056)")
    parser.add_argument("--out", default="resultados.json", help="Archivo de salida JSON")
    parser.add_argument(
        "--sin-proxy",
        action="store_true",
        help="Intenta conexión directa ignorando variables de proxy del entorno",
    )
    args = parser.parse_args()

    if args.sin_proxy:
        for key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
            os.environ.pop(key, None)

    try:
        resultados = obtener_resultados(args.evento, disable_proxy=args.sin_proxy)
    except Exception as exc:
        print(f"Error al obtener resultados: {exc}", file=sys.stderr)
        return 1

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in resultados], f, ensure_ascii=False, indent=2)

    print(f"Se guardaron {len(resultados)} resultados en {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
