#!/usr/bin/env python3
"""Scrapper de resultados para eventos en Dashport.

Uso:
    python scrapper/dashport_scrapper.py --evento 1056 --out resultados_1056.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, asdict
from html import unescape
from html.parser import HTMLParser
from typing import Any
from urllib.request import Request, urlopen

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


def fetch_html(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as response:
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


def resultados_from_next_data(next_data: dict[str, Any]) -> list[Resultado]:
    blob = json.dumps(next_data, ensure_ascii=False)

    # Intenta identificar objetos típicos de resultados dentro del JSON de Next.js
    pattern = re.compile(
        r'\{[^{}]*?(?:"puesto"|"posicion"|"position")[^{}]*?(?:"tiempo"|"time")[^{}]*?\}',
        flags=re.IGNORECASE,
    )

    resultados: list[Resultado] = []
    for raw in pattern.findall(blob):
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            continue

        resultados.append(
            Resultado(
                posicion=str(obj.get("puesto") or obj.get("posicion") or obj.get("position") or ""),
                dorsal=str(obj.get("dorsal") or obj.get("bib") or ""),
                atleta=str(obj.get("nombre") or obj.get("atleta") or obj.get("runner") or ""),
                categoria=str(obj.get("categoria") or obj.get("category") or ""),
                tiempo=str(obj.get("tiempo") or obj.get("time") or ""),
            )
        )

    # Limpia filas vacías o repetidas
    unique: dict[tuple[str, str, str, str, str], Resultado] = {}
    for r in resultados:
        key = (r.posicion, r.dorsal, r.atleta, r.categoria, r.tiempo)
        if any(key):
            unique[key] = r

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


def obtener_resultados(evento: str) -> list[Resultado]:
    url = BASE_URL.format(evento=evento)
    html = fetch_html(url)

    next_data = extract_next_data(html)
    if next_data:
        resultados = resultados_from_next_data(next_data)
        if resultados:
            return resultados

    resultados = resultados_from_tables(html)
    if resultados:
        return resultados

    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="Obtiene resultados de Dashport")
    parser.add_argument("--evento", default="1056", help="ID del evento (ej. 1056)")
    parser.add_argument("--out", default="resultados.json", help="Archivo de salida JSON")
    args = parser.parse_args()

    try:
        resultados = obtener_resultados(args.evento)
    except Exception as exc:
        print(f"Error al obtener resultados: {exc}", file=sys.stderr)
        return 1

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in resultados], f, ensure_ascii=False, indent=2)

    print(f"Se guardaron {len(resultados)} resultados en {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
