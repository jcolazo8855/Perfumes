#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup


LOGGER = logging.getLogger("perfume_collector")
DEFAULT_USER_AGENT = (
    "PerfumeResearchBot/1.0 (+contact-you@example.com; purpose=research)"
)


@dataclass
class PerfumeRecord:
    name: str
    brand: str | None = None
    source_site: str | None = None
    source_url: str | None = None
    perfume_type: str | None = None
    concentration: str | None = None
    gender: str | None = None
    year_released: int | None = None
    perfumer: str | None = None
    description: str | None = None
    accords: list[str] = field(default_factory=list)
    top_notes: list[str] = field(default_factory=list)
    middle_notes: list[str] = field(default_factory=list)
    base_notes: list[str] = field(default_factory=list)
    rating_value: float | None = None
    rating_count: int | None = None
    longevity: str | None = None
    sillage: str | None = None
    season: str | None = None
    occasion: str | None = None
    image_url: str | None = None
    raw_json: dict = field(default_factory=dict)


class PerfumeDatabase:
    def __init__(self, db_path: Path) -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def initialize(self) -> None:
        self.conn.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS brands (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS perfumes (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                brand_id INTEGER,
                source_site TEXT,
                source_url TEXT,
                perfume_type TEXT,
                concentration TEXT,
                gender TEXT,
                year_released INTEGER,
                perfumer TEXT,
                description TEXT,
                rating_value REAL,
                rating_count INTEGER,
                longevity TEXT,
                sillage TEXT,
                season TEXT,
                occasion TEXT,
                image_url TEXT,
                raw_json TEXT,
                UNIQUE(source_site, source_url),
                FOREIGN KEY (brand_id) REFERENCES brands(id)
            );
            CREATE TABLE IF NOT EXISTS notes (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS perfume_notes (
                perfume_id INTEGER NOT NULL,
                note_id INTEGER NOT NULL,
                note_role TEXT NOT NULL,
                PRIMARY KEY (perfume_id, note_id, note_role),
                FOREIGN KEY (perfume_id) REFERENCES perfumes(id) ON DELETE CASCADE,
                FOREIGN KEY (note_id) REFERENCES notes(id)
            );
            CREATE TABLE IF NOT EXISTS accords (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS perfume_accords (
                perfume_id INTEGER NOT NULL,
                accord_id INTEGER NOT NULL,
                PRIMARY KEY (perfume_id, accord_id),
                FOREIGN KEY (perfume_id) REFERENCES perfumes(id) ON DELETE CASCADE,
                FOREIGN KEY (accord_id) REFERENCES accords(id)
            );
            """
        )
        self.conn.commit()

    def upsert_perfume(self, record: PerfumeRecord) -> None:
        brand_id = self._upsert_lookup("brands", record.brand) if record.brand else None
        raw_json = json.dumps(record.raw_json, ensure_ascii=True, sort_keys=True)
        self.conn.execute(
            """
            INSERT INTO perfumes (
                name, brand_id, source_site, source_url, perfume_type, concentration,
                gender, year_released, perfumer, description, rating_value, rating_count,
                longevity, sillage, season, occasion, image_url, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_site, source_url) DO UPDATE SET
                name = excluded.name,
                brand_id = excluded.brand_id,
                perfume_type = excluded.perfume_type,
                concentration = excluded.concentration,
                gender = excluded.gender,
                year_released = excluded.year_released,
                perfumer = excluded.perfumer,
                description = excluded.description,
                rating_value = excluded.rating_value,
                rating_count = excluded.rating_count,
                longevity = excluded.longevity,
                sillage = excluded.sillage,
                season = excluded.season,
                occasion = excluded.occasion,
                image_url = excluded.image_url,
                raw_json = excluded.raw_json
            """,
            (
                record.name,
                brand_id,
                record.source_site,
                record.source_url,
                record.perfume_type,
                record.concentration,
                record.gender,
                record.year_released,
                record.perfumer,
                record.description,
                record.rating_value,
                record.rating_count,
                record.longevity,
                record.sillage,
                record.season,
                record.occasion,
                record.image_url,
                raw_json,
            ),
        )
        perfume_id = self.conn.execute(
            "SELECT id FROM perfumes WHERE source_site = ? AND source_url = ?",
            (record.source_site, record.source_url),
        ).fetchone()["id"]
        self.conn.execute("DELETE FROM perfume_notes WHERE perfume_id = ?", (perfume_id,))
        self.conn.execute("DELETE FROM perfume_accords WHERE perfume_id = ?", (perfume_id,))
        self._insert_notes(perfume_id, "top", record.top_notes)
        self._insert_notes(perfume_id, "middle", record.middle_notes)
        self._insert_notes(perfume_id, "base", record.base_notes)
        for accord in dedupe_preserve_order(record.accords):
            accord_id = self._upsert_lookup("accords", accord)
            self.conn.execute(
                "INSERT OR IGNORE INTO perfume_accords (perfume_id, accord_id) VALUES (?, ?)",
                (perfume_id, accord_id),
            )
        self.conn.commit()

    def summary(self) -> dict[str, int]:
        row = self.conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM brands) AS brand_count,
                (SELECT COUNT(*) FROM perfumes) AS perfume_count,
                (SELECT COUNT(*) FROM notes) AS note_count,
                (SELECT COUNT(*) FROM accords) AS accord_count
            """
        ).fetchone()
        return dict(row)

    def _upsert_lookup(self, table: str, value: str) -> int:
        self.conn.execute(f"INSERT OR IGNORE INTO {table} (name) VALUES (?)", (value.strip(),))
        row = self.conn.execute(f"SELECT id FROM {table} WHERE name = ?", (value.strip(),)).fetchone()
        return int(row["id"])

    def _insert_notes(self, perfume_id: int, role: str, values: list[str]) -> None:
        for value in dedupe_preserve_order(values):
            note_id = self._upsert_lookup("notes", value)
            self.conn.execute(
                "INSERT OR IGNORE INTO perfume_notes (perfume_id, note_id, note_role) VALUES (?, ?, ?)",
                (perfume_id, note_id, role),
            )


class PoliteHttpClient:
    def __init__(
        self,
        cache_dir: Path,
        user_agent: str = DEFAULT_USER_AGENT,
        min_delay_seconds: float = 8.0,
        max_delay_seconds: float = 16.0,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.user_agent = user_agent
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.timeout_seconds = timeout_seconds
        self.last_request_by_host: dict[str, float] = {}
        self.robot_parsers: dict[str, RobotFileParser] = {}
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})

    def get(self, url: str, use_cache: bool = True) -> requests.Response | None:
        if not self._allowed_by_robots(url):
            LOGGER.warning("Blocked by robots.txt: %s", url)
            return None
        cache_path = self._cache_path(url)
        if use_cache and cache_path.exists():
            response = requests.Response()
            response.status_code = 200
            response.url = url
            response._content = cache_path.read_bytes()
            return response
        self._wait_turn(url)
        for attempt in range(3):
            try:
                response = self.session.get(url, timeout=self.timeout_seconds)
                self.last_request_by_host[urlparse(url).netloc] = time.time()
                if response.status_code == 200:
                    cache_path.write_text(response.text, encoding="utf-8")
                    return response
                if response.status_code in {403, 429}:
                    LOGGER.warning("Received %s from %s; stopping.", response.status_code, url)
                    return None
            except requests.RequestException as exc:
                LOGGER.warning("Request failed for %s: %s", url, exc)
            time.sleep((attempt + 1) * 10)
        return None

    def _wait_turn(self, url: str) -> None:
        host = urlparse(url).netloc
        previous = self.last_request_by_host.get(host)
        if previous is None:
            return
        delay = random.uniform(self.min_delay_seconds, self.max_delay_seconds)
        remaining = delay - (time.time() - previous)
        if remaining > 0:
            time.sleep(remaining)

    def _allowed_by_robots(self, url: str) -> bool:
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        robots_url = urljoin(base, "/robots.txt")
        if base not in self.robot_parsers:
            parser = RobotFileParser()
            parser.set_url(robots_url)
            try:
                parser.read()
            except Exception:
                return False
            self.robot_parsers[base] = parser
        return self.robot_parsers[base].can_fetch(self.user_agent, url)

    def _cache_path(self, url: str) -> Path:
        return self.cache_dir / f"{hashlib.sha256(url.encode('utf-8')).hexdigest()}.html"


class SiteAdapter:
    site_name: str = "generic"
    start_urls: list[str] = []

    def discover_product_links(self, soup: BeautifulSoup, page_url: str) -> Iterable[str]:
        for tag in soup.select("a[href]"):
            absolute = urljoin(page_url, tag.get("href", "").strip())
            if self.looks_like_product_url(absolute):
                yield absolute

    def parse_product_page(self, soup: BeautifulSoup, page_url: str) -> PerfumeRecord | None:
        raise NotImplementedError

    def looks_like_product_url(self, url: str) -> bool:
        return False


class FragranticaAdapter(SiteAdapter):
    site_name = "fragrantica"
    start_urls = ["https://www.fragrantica.com/designers/"]

    def parse_product_page(self, soup: BeautifulSoup, page_url: str) -> PerfumeRecord | None:
        title = soup.select_one("h1")
        if not title:
            return None
        page_text = soup.get_text(" ", strip=True)
        return PerfumeRecord(
            name=clean_text(title.get_text(" ", strip=True)),
            brand=first_text(soup, ["a[href*='/designers/']", "[itemprop='brand']"]),
            source_site=self.site_name,
            source_url=page_url,
            description=first_text(soup, ["div[itemprop='description']", ".fragrantica-blockquote"]),
            accords=extract_list_by_heading(soup, ["main accords", "accords"]),
            top_notes=extract_list_by_heading(soup, ["top notes"]),
            middle_notes=extract_list_by_heading(soup, ["middle notes", "heart notes"]),
            base_notes=extract_list_by_heading(soup, ["base notes"]),
            rating_value=extract_rating_metrics(page_text)[0],
            rating_count=extract_rating_metrics(page_text)[1],
            year_released=extract_year(page_text),
        )

    def looks_like_product_url(self, url: str) -> bool:
        return "/perfume/" in url


class ParfumoAdapter(SiteAdapter):
    site_name = "parfumo"
    start_urls = ["https://www.parfumo.com/Perfumes"]

    def parse_product_page(self, soup: BeautifulSoup, page_url: str) -> PerfumeRecord | None:
        title = soup.select_one("h1")
        if not title:
            return None
        page_text = soup.get_text(" ", strip=True)
        return PerfumeRecord(
            name=clean_text(title.get_text(" ", strip=True)),
            brand=first_text(soup, ["a[href*='/Brands/']", ".brand a"]),
            source_site=self.site_name,
            source_url=page_url,
            description=first_text(soup, [".text_content", ".text"]),
            accords=extract_list_by_heading(soup, ["accords", "main accords"]),
            top_notes=extract_list_by_heading(soup, ["top note", "top notes"]),
            middle_notes=extract_list_by_heading(soup, ["heart note", "heart notes"]),
            base_notes=extract_list_by_heading(soup, ["base note", "base notes"]),
            rating_value=extract_rating_metrics(page_text)[0],
            rating_count=extract_rating_metrics(page_text)[1],
            year_released=extract_year(page_text),
        )

    def looks_like_product_url(self, url: str) -> bool:
        return "/Perfumes/" in url


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def first_text(soup: BeautifulSoup, selectors: list[str]) -> str | None:
    for selector in selectors:
        tag = soup.select_one(selector)
        if tag:
            text = clean_text(tag.get_text(" ", strip=True))
            if text:
                return text
    return None


def extract_year(page_text: str) -> int | None:
    match = re.search(r"\b(19\d{2}|20\d{2})\b", page_text)
    return int(match.group(1)) if match else None


def extract_rating_metrics(page_text: str) -> tuple[float | None, int | None]:
    value_match = re.search(r"(\d+(?:\.\d+)?)\s*/\s*5", page_text)
    count_match = re.search(r"(\d[\d,]*)\s+(?:votes?|ratings?)", page_text, re.IGNORECASE)
    value = float(value_match.group(1)) if value_match else None
    count = int(count_match.group(1).replace(",", "")) if count_match else None
    return value, count


def extract_list_by_heading(soup: BeautifulSoup, heading_names: list[str]) -> list[str]:
    results: list[str] = []
    targets = {item.lower() for item in heading_names}
    for container in soup.find_all(["div", "section", "p", "h2", "h3", "h4", "strong"]):
        label = clean_text(container.get_text(" ", strip=True)).lower()
        if label not in targets:
            continue
        sibling = container.find_next_sibling()
        if sibling:
            for tag in sibling.select("a, span, li"):
                text = clean_text(tag.get_text(" ", strip=True))
                if text and len(text) < 80:
                    results.append(text)
    return dedupe_preserve_order(results)


def dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = value.strip().lower()
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(value.strip())
    return result


class PerfumeCrawler:
    def __init__(self, db: PerfumeDatabase, client: PoliteHttpClient, adapters: list[SiteAdapter]) -> None:
        self.db = db
        self.client = client
        self.adapters = adapters

    def crawl(self, max_pages_per_site: int = 25) -> None:
        for adapter in self.adapters:
            visited: set[str] = set()
            queue = list(adapter.start_urls)
            pages_seen = 0
            while queue and pages_seen < max_pages_per_site:
                url = queue.pop(0)
                if url in visited:
                    continue
                visited.add(url)
                response = self.client.get(url)
                if response is None:
                    continue
                pages_seen += 1
                soup = BeautifulSoup(response.text, "html.parser")
                if adapter.looks_like_product_url(url):
                    record = adapter.parse_product_page(soup, url)
                    if record:
                        self.db.upsert_perfume(record)
                    continue
                for discovered in adapter.discover_product_links(soup, url):
                    if discovered not in visited:
                        queue.append(discovered)


def seed_example_records(db: PerfumeDatabase) -> None:
    db.upsert_perfume(
        PerfumeRecord(
            name="Example Citrus Eau de Parfum",
            brand="Example House",
            source_site="manual",
            source_url="manual://example-citrus-edp",
            perfume_type="Eau de Parfum",
            concentration="EDP",
            gender="Unisex",
            year_released=2024,
            perfumer="Jane Doe",
            description="Example seed record used to verify schema and queries.",
            accords=["citrus", "fresh", "aromatic"],
            top_notes=["bergamot", "lemon"],
            middle_notes=["lavender", "neroli"],
            base_notes=["musk", "cedar"],
            rating_value=4.2,
            rating_count=18,
            longevity="moderate",
            sillage="moderate",
            season="spring/summer",
            occasion="daytime",
            raw_json={"seed": True},
        )
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polite perfume data collector")
    parser.add_argument("--db", type=Path, default=Path("perfumes.db"))
    parser.add_argument("--cache", type=Path, default=Path(".cache"))
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    crawl_parser = subparsers.add_parser("crawl")
    crawl_parser.add_argument("--max-pages-per-site", type=int, default=25)

    subparsers.add_parser("seed")
    subparsers.add_parser("stats")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    db = PerfumeDatabase(args.db)
    db.initialize()

    if args.command == "seed":
        seed_example_records(db)
        print(f"Seeded example record into {args.db}")
        return
    if args.command == "stats":
        print(json.dumps(db.summary(), indent=2))
        return

    client = PoliteHttpClient(cache_dir=args.cache)
    crawler = PerfumeCrawler(db, client, [FragranticaAdapter(), ParfumoAdapter()])
    crawler.crawl(max_pages_per_site=args.max_pages_per_site)


if __name__ == "__main__":
    main()
