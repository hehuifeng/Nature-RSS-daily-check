#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rss_daily_pipeline.py  (config-driven)
--------------------------------------
- Read config.json (see example at the end)
- Generate a separate Markdown report per feed for the current day (filename includes journal name + second-level timestamp)
- Report title contains:
  # {journal} RSS Report — most recently had new articles on: {last_new_ts} — generated on: {today}
- State stored in SQLite; ETag/Last-Modified handled via the feeds table; de-dup via the seen table
- Optional translation: when translator == "openai", use the openai_* settings from config

Author: ChatGPT (Nature Article extractor project)
"""

from __future__ import annotations
import contextlib
import dataclasses
import hashlib
import html
import io
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------- Timezone (Asia/Beijing by default) ----------
try:
    from zoneinfo import ZoneInfo
    DEFAULT_TZ = ZoneInfo(os.getenv("PIPELINE_TZ", "Asia/Beijing"))
except Exception:
    DEFAULT_TZ = None  # fall back to system local time

HEADERS = {"User-Agent": "rss-daily-pipeline/1.1 (+https://example.com)"}

# -------------------------- Utilities --------------------------

def now_dt():
    return datetime.now(DEFAULT_TZ) if DEFAULT_TZ else datetime.now().astimezone()

def now_ts() -> str:
    # ISO format with second precision (for content/DB)
    return now_dt().isoformat(timespec="seconds")

def ts_for_filename() -> str:
    # Compact timestamp for filenames: YYYYMMDD_HHMMSS
    d = now_dt()
    return d.strftime("%Y%m%d_%H%M%S")

def today_ymd() -> str:
    return now_dt().date().isoformat()

def sha256(text: str | bytes) -> str:
    if isinstance(text, str):
        text = text.encode("utf-8", "ignore")
    return hashlib.sha256(text).hexdigest()

def ensure_dir(p: str | os.PathLike) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)

def clean_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = html.unescape(s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def pick_first(*vals):
    for v in vals:
        if v:
            return v
    return None

def sanitize_filename(s: str, default: str = "report"):
    if not s:
        return default
    # Remove unsafe filename characters
    s = re.sub(r"[\\/:*?\"<>|]", "_", s)
    s = re.sub(r"\s+", "_", s)
    s = s.strip("._")
    return s or default

# -------------------------- Database --------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS feeds (
  feed_url TEXT PRIMARY KEY,
  etag TEXT,
  last_modified TEXT,
  last_checked_at TEXT
);
CREATE TABLE IF NOT EXISTS seen (
  uid TEXT PRIMARY KEY,         -- DOI>GUID>ID>link
  feed_url TEXT NOT NULL,
  article_url TEXT,
  doi TEXT,
  pub_date TEXT,
  first_seen_at TEXT
);
CREATE TABLE IF NOT EXISTS articles (
  uid TEXT PRIMARY KEY,
  feed_url TEXT NOT NULL,
  journal TEXT,
  title_en TEXT,
  title_cn TEXT,
  type TEXT,
  pub_date TEXT,
  doi TEXT,
  article_url TEXT,
  abstract_en TEXT,
  abstract_cn TEXT,
  raw_jsonld TEXT,
  fetched_at TEXT,
  last_updated_at TEXT
);
"""

def db_connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript(SCHEMA)
    return conn

# -------------------------- Feed Parsing --------------------------

NS = {
    'rdf': "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    'rss': "http://purl.org/rss/1.0/",
    'dc': "http://purl.org/dc/elements/1.1/",
    'content': "http://purl.org/rss/1.0/modules/content/",
    'prism': "http://prismstandard.org/namespaces/basic/2.0/",
    'admin': "http://webns.net/mvcb/",
    'atom': "http://www.w3.org/2005/Atom",
}

def fetch_feed(url: str, etag: Optional[str], last_modified: Optional[str], timeout: int = 25) -> Tuple[int, Dict[str, str], Optional[bytes]]:
    headers = dict(HEADERS)
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    resp = requests.get(url, headers=headers, timeout=timeout)
    if resp.status_code == 304:
        return 304, resp.headers, None
    resp.raise_for_status()
    return resp.status_code, resp.headers, resp.content

def xml_root(xml_bytes: bytes):
    from xml.etree import ElementTree as ET
    return ET.fromstring(xml_bytes)

def xml_text(node):
    return node.text.strip() if (node is not None and node.text) else None

def guess_doi(*candidates) -> Optional[str]:
    for s in candidates:
        if not s:
            continue
        m = re.search(r'\b(10\.\d{4,9}/[^\s<>"\']+)', str(s))
        if m:
            return m.group(1).rstrip(').,;')
    return None

def parse_feed(xml_bytes: bytes) -> List[Dict[str, Any]]:
    """Return a list of dict items with minimal fields: title, link, id/guid/about, pub_date, doi, journal."""
    root = xml_root(xml_bytes)
    tag = root.tag.split('}', 1)[-1] if '}' in root.tag else root.tag
    items: List[Dict[str, Any]] = []

    def add_item(d: Dict[str, Any]):
        d["title"] = clean_text(d.get("title"))
        d["link"] = d.get("link")
        d["id_like"] = d.get("id_like")
        d["pub_date"] = clean_text(d.get("pub_date"))
        d["doi"] = clean_text(d.get("doi"))
        d["journal"] = clean_text(d.get("journal"))
        items.append(d)

    # RSS 1.0 / RDF
    if tag == "RDF" or root.find('rss:channel', NS) is not None:
        for item in root.findall('rss:item', NS):
            title = xml_text(item.find('rss:title', NS)) or xml_text(item.find('dc:title', NS))
            link = xml_text(item.find('rss:link', NS))
            about = item.get(f'{{{NS["rdf"]}}}about')
            dc_date = xml_text(item.find('dc:date', NS))
            prism_doi = xml_text(item.find('prism:doi', NS))
            prism_publicationName = xml_text(item.find('prism:publicationName', NS))
            dc_identifier = xml_text(item.find('dc:identifier', NS))
            add_item({
                "title": title,
                "link": link,
                "id_like": about or dc_identifier or link,
                "pub_date": dc_date,
                "doi": prism_doi or guess_doi(dc_identifier, link),
                "journal": prism_publicationName,
            })
        return items

    # RSS 2.0
    if tag == "rss" or root.find('channel') is not None:
        channel = root.find('channel')
        for item in channel.findall('item') if channel is not None else []:
            title = xml_text(item.find('title')) or xml_text(item.find('dc:title', NS))
            link = xml_text(item.find('link'))
            guid = xml_text(item.find('guid'))
            pubDate = xml_text(item.find('pubDate')) or xml_text(item.find('dc:date', NS))
            description = xml_text(item.find('description'))
            add_item({
                "title": title,
                "link": link,
                "id_like": guid or link,
                "pub_date": pubDate,
                "doi": guess_doi(guid, description, link),
                "journal": None,
            })
        return items

    # Atom
    if tag == "feed" or root.find('atom:feed', NS) is not None:
        feed = root
        for entry in feed.findall('atom:entry', NS) or feed.findall('entry'):
            title = xml_text(entry.find('atom:title', NS)) or xml_text(entry.find('title'))
            link_el = entry.find('atom:link[@rel="alternate"]', NS) or entry.find('link')
            link = link_el.get('href') if link_el is not None else None
            entry_id = xml_text(entry.find('atom:id', NS)) or xml_text(entry.find('id'))
            published = xml_text(entry.find('atom:published', NS)) or xml_text(entry.find('published'))
            updated = xml_text(entry.find('atom:updated', NS)) or xml_text(entry.find('updated'))
            add_item({
                "title": title,
                "link": link,
                "id_like": entry_id or link,
                "pub_date": published or updated,
                "doi": guess_doi(entry_id, link),
                "journal": None,
            })
        return items

    return items

# -------------------------- Article HTML extraction --------------------------

def soupify(html_text: str) -> BeautifulSoup:
    return BeautifulSoup(html_text, "lxml")

def select_meta(soup: BeautifulSoup, name: str) -> Optional[str]:
    el = soup.find("meta", attrs={"name": name})
    if el and el.get("content"):
        return el["content"].strip()
    return None

def parse_jsonld(soup: BeautifulSoup) -> List[dict]:
    found = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        if isinstance(data, list):
            found.extend(data)
        elif isinstance(data, dict):
            found.append(data)
    return found

def pick_article_obj(jsonlds: List[dict]) -> Optional[dict]:
    for obj in jsonlds:
        t = obj.get("@type") or obj.get("type")
        if isinstance(t, list):
            t = t[0]
        if t in ("Article", "ScholarlyArticle", "NewsArticle"):
            return obj
    return None

def extract_from_jsonld(obj: dict) -> Dict[str, Optional[str]]:
    title = obj.get("headline") or obj.get("name")
    abstract = obj.get("abstract") or obj.get("description")
    date_published = obj.get("datePublished") or obj.get("dateCreated")
    journal = None
    atype = obj.get("articleSection") or obj.get("type")
    doi = None
    ident = obj.get("identifier")
    if isinstance(ident, dict):
        ident = ident.get("value") or ident.get("@id")
    if isinstance(ident, list):
        for it in ident:
            s = it if isinstance(it, str) else (it.get("value") or it.get("@id"))
            if s and "10." in s:
                m = re.search(r'10\.\d{4,9}/\S+', s)
                if m:
                    doi = m.group(0)
                    break
    elif isinstance(ident, str) and "10." in ident:
        m = re.search(r'10\.\d{4,9}/\S+', ident)
        if m:
            doi = m.group(0)
    ispart = obj.get("isPartOf")
    if isinstance(ispart, dict):
        journal = ispart.get("name") or journal
    publisher = obj.get("publisher")
    if isinstance(publisher, dict):
        journal = journal or publisher.get("name")

    return {
        "title": clean_text(title),
        "abstract": clean_text(abstract),
        "date_published": clean_text(date_published),
        "journal": clean_text(journal),
        "type": clean_text(atype),
        "doi": clean_text(doi),
    }

def extract_article_fields(html_text: str, url: str) -> Dict[str, Optional[str]]:
    soup = soupify(html_text)
    jsonlds = parse_jsonld(soup)
    obj = pick_article_obj(jsonlds)
    base = extract_from_jsonld(obj) if obj else {}

    base.setdefault("journal", select_meta(soup, "citation_journal_title"))
    base.setdefault("title", select_meta(soup, "citation_title"))
    base.setdefault("doi", select_meta(soup, "citation_doi"))
    base.setdefault("date_published", select_meta(soup, "citation_publication_date"))
    base.setdefault("type", select_meta(soup, "citation_article_type"))
    base.setdefault("abstract", select_meta(soup, "dc.description") or select_meta(soup, "description"))

    if not base.get("abstract"):
        abstract_el = soup.find(lambda tag: tag.name in ["section", "div", "p"] and tag.get("id") and "abstract" in tag.get("id").lower())
        if not abstract_el:
            abstract_el = soup.find(lambda tag: tag.name in ["section", "div"] and any("abstract" in (c.lower() if isinstance(c, str) else "") for c in tag.get("class", [])))
        if not abstract_el:
            ogdesc = soup.find("meta", attrs={"property": "og:description"})
            if ogdesc and ogdesc.get("content"):
                base["abstract"] = clean_text(ogdesc["content"])
        else:
            base["abstract"] = clean_text(abstract_el.get_text(" "))

    base["article_url"] = url
    return base

# -------------------------- Translation --------------------------

def translate_openai_en2zh(texts: List[str], cfg: Dict[str, Any]) -> List[str]:
    """
    Translate a batch of English texts into Chinese using an OpenAI-compatible Chat Completions API.
    Read config from cfg["openai"]: {api_key, base_url, model}
    If configuration is missing or there is an error, returns the original texts.
    """
    ocfg = cfg.get("openai") or {}
    api_key = ocfg.get("api_key")
    base_url = (ocfg.get("base_url") or "https://api.openai.com/v1").rstrip("/")
    model = ocfg.get("model") or "gpt-4o-mini"

    if not api_key:
        return texts

    url = f"{base_url}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    outputs: List[str] = []
    for chunk in texts:
        if not chunk:
            outputs.append("")
            continue
        prompt = f"Please translate the following English text into Simplified Chinese accurately, without explanations or bracketed notes:\n\n{chunk}"
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a precise bilingual scientific translator (EN->ZH-CN). Return only the translation."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2,
        }
        try:
            r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=cfg.get("http_timeout", 60))
            r.raise_for_status()
            data = r.json()
            cn = data["choices"][0]["message"]["content"].strip()
        except Exception:
            cn = chunk  # fallback
        outputs.append(cn)
        time.sleep(cfg.get("sleep_between_translations", 0.4))
    return outputs

# -------------------------- Core pipeline --------------------------

@dataclass
class NewArticle:
    uid: str
    feed_url: str
    article_url: str
    doi: Optional[str]
    pub_date: Optional[str]
    meta: Dict[str, Optional[str]]
    fetched_at: str

def compute_uid(item: Dict[str, Any]) -> str:
    doi = item.get("doi")
    if doi:
        return f"doi:{doi.lower()}"
    if item.get("id_like"):
        return f"id:{item['id_like']}"
    if item.get("link"):
        return f"linkhash:{sha256(item['link'])[:16]}"
    digest = sha256(json.dumps(item, sort_keys=True))
    return f"hash:{digest[:16]}"

def check_new_items(conn: sqlite3.Connection, feed_url: str, feed_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    new_items = []
    for it in feed_items:
        uid = compute_uid(it)
        row = cur.execute("SELECT 1 FROM seen WHERE uid=?", (uid,)).fetchone()
        if row is None:
            it["_uid"] = uid
            new_items.append(it)
    return new_items

def mark_seen(conn: sqlite3.Connection, uid: str, feed_url: str, article_url: str, doi: Optional[str], pub_date: Optional[str]) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO seen(uid, feed_url, article_url, doi, pub_date, first_seen_at) VALUES (?,?,?,?,?,?)",
        (uid, feed_url, article_url, doi, pub_date, now_ts())
    )
    conn.commit()

def upsert_article(conn: sqlite3.Connection, uid: str, feed_url: str, fields: Dict[str, Optional[str]], raw_jsonld: Optional[dict]) -> None:
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO articles(uid, feed_url, journal, title_en, title_cn, type, pub_date, doi, article_url, abstract_en, abstract_cn, raw_jsonld, fetched_at, last_updated_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(uid) DO UPDATE SET
             journal=excluded.journal,
             title_en=excluded.title_en,
             title_cn=excluded.title_cn,
             type=excluded.type,
             pub_date=excluded.pub_date,
             doi=excluded.doi,
             article_url=excluded.article_url,
             abstract_en=excluded.abstract_en,
             abstract_cn=excluded.abstract_cn,
             raw_jsonld=excluded.raw_jsonld,
             last_updated_at=excluded.last_updated_at
        """,
        (
            uid,
            feed_url,
            fields.get("journal"),
            fields.get("title_en"),
            fields.get("title_cn"),
            fields.get("type"),
            fields.get("pub_date"),
            fields.get("doi"),
            fields.get("article_url"),
            fields.get("abstract_en"),
            fields.get("abstract_cn"),
            json.dumps(raw_jsonld, ensure_ascii=False) if raw_jsonld else None,
            now_ts(),
            now_ts(),
        )
    )
    conn.commit()

def get_last_new_ts_for_feed(conn: sqlite3.Connection, feed_url: str) -> Optional[str]:
    cur = conn.cursor()
    row = cur.execute("SELECT MAX(first_seen_at) FROM seen WHERE feed_url=?", (feed_url,)).fetchone()
    if row and row[0]:
        return row[0]
    return None

def majority_journal(items: List[NewArticle]) -> Optional[str]:
    counter: Dict[str, int] = {}
    for na in items:
        j = na.meta.get("journal")
        if j:
            counter[j] = counter.get(j, 0) + 1
    if not counter:
        return None
    return sorted(counter.items(), key=lambda kv: kv[1], reverse=True)[0][0]

def run_pipeline(config: Dict[str, Any]) -> List[str]:
    feeds: List[str] = config.get("feeds") or []
    if not feeds:
        print("No feeds provided in config.json (key: 'feeds').", file=sys.stderr)
        sys.exit(2)

    out_dir = config.get("out_dir", "./reports")
    db_path = config.get("db", "./rss_state.db")
    translator = config.get("translator", "none")
    http_timeout = int(config.get("http_timeout", 25))
    sleep_between = float(config.get("sleep_between_fetches", 0.5))

    ensure_dir(out_dir)
    conn = db_connect(db_path)
    cur = conn.cursor()
    today = today_ymd()
    written_paths: List[str] = []

    for feed_url in feeds:
        todays_items: List[NewArticle] = []

        # --- fetch with caching headers ---
        feed_row = cur.execute("SELECT etag,last_modified FROM feeds WHERE feed_url=?", (feed_url,)).fetchone()
        etag = feed_row[0] if feed_row else None
        last_mod = feed_row[1] if feed_row else None

        try:
            status, headers, content = fetch_feed(feed_url, etag, last_mod, timeout=http_timeout)
        except Exception as e:
            print(f"[WARN] fetch feed failed: {feed_url} -> {e}", file=sys.stderr)
            continue

        if status == 304:
            cur.execute("INSERT OR REPLACE INTO feeds(feed_url, etag, last_modified, last_checked_at) VALUES (?,?,?,?)",
                        (feed_url, etag, last_mod, now_ts()))
            conn.commit()
            print(f"[INFO] 304 Not Modified: {feed_url}")
            # Even on 304, write a "no new articles today" file (per requirements)
            items = []
        else:
            cur.execute("INSERT OR REPLACE INTO feeds(feed_url, etag, last_modified, last_checked_at) VALUES (?,?,?,?)",
                        (feed_url, headers.get("ETag"), headers.get("Last-Modified"), now_ts()))
            conn.commit()
            items = parse_feed(content or b"")
            print(f"[INFO] Feed parsed: {feed_url}, items={len(items)}")

        new_items = check_new_items(conn, feed_url, items)

        # --- process new items ---
        for it in new_items:
            uid = it["_uid"]
            link = it.get("link")
            doi = it.get("doi")
            pub_date = it.get("pub_date")

            html_text = None
            try:
                r = requests.get(link, headers=HEADERS, timeout=http_timeout)
                r.raise_for_status()
                html_text = r.text
            except Exception as e:
                print(f"[WARN] fetch article failed: {link} -> {e}", file=sys.stderr)
                mark_seen(conn, uid, feed_url, link or "", doi, pub_date)
                time.sleep(sleep_between)
                continue

            soup = soupify(html_text)
            jsonlds = parse_jsonld(soup)
            jsonld_obj = pick_article_obj(jsonlds)
            fields_from_jsonld = extract_from_jsonld(jsonld_obj) if jsonld_obj else {}
            fallback_fields = extract_article_fields(html_text, link or "")
            merged = {**fallback_fields, **{k: v for k, v in fields_from_jsonld.items() if v}}

            journal = merged.get("journal")
            title_en = merged.get("title") or it.get("title")
            atype = merged.get("type")
            date_published = merged.get("date_published") or it.get("pub_date")
            doi_final = merged.get("doi") or doi
            article_url = merged.get("article_url") or link or ""
            abstract_en = merged.get("abstract")

            if translator == "openai":
                title_cn, abstract_cn = translate_openai_en2zh([title_en or "", abstract_en or ""], config)
            else:
                title_cn, abstract_cn = title_en, abstract_en

            record = {
                "journal": journal,
                "title_en": title_en,
                "title_cn": title_cn,
                "type": atype,
                "pub_date": date_published,
                "doi": doi_final,
                "article_url": article_url,
                "abstract_en": abstract_en,
                "abstract_cn": abstract_cn,
            }

            upsert_article(conn, uid, feed_url, record, jsonld_obj)
            mark_seen(conn, uid, feed_url, article_url, doi_final, date_published)

            todays_items.append(NewArticle(
                uid=uid,
                feed_url=feed_url,
                article_url=article_url,
                doi=doi_final,
                pub_date=date_published,
                meta=record,
                fetched_at=now_ts(),
            ))

            time.sleep(sleep_between)

        # --- decide journal name for filename/header ---
        journal_name = majority_journal(todays_items)
        if not journal_name:
            # If there are no new articles today, look up the most common journal for this feed from historical articles; otherwise fall back to domain/placeholder
            row = cur.execute("SELECT journal, COUNT(*) c FROM articles WHERE feed_url=? AND journal IS NOT NULL GROUP BY journal ORDER BY c DESC LIMIT 1", (feed_url,)).fetchone()
            if row and row[0]:
                journal_name = row[0]
            else:
                journal_name = re.sub(r"^https?://(www\.)?", "", feed_url).split("/")[0] or "UnknownJournal"

        # --- header time: the most recent time this feed "had new articles" ---
        last_new_ts = get_last_new_ts_for_feed(conn, feed_url) or now_ts()

        # --- write per-feed markdown (second-level timestamp + journal name) ---
        ensure_dir(out_dir)
        fname = f"rss_report_{sanitize_filename(journal_name)}_{ts_for_filename()}.md"
        md_path = os.path.join(out_dir, fname)
        with io.open(md_path, "w", encoding="utf-8") as f:
            last_new_date = last_new_ts.split("T")[0] if last_new_ts else today
            today_date = today  # today_ymd() already returns YYYY-MM-DD
            f.write(f"# {journal_name} RSS Report — {last_new_date} — {today_date}\n\n")
            if not todays_items:
                f.write("No new articles today.\n")
            else:
                for i, na in enumerate(todays_items, 1):
                    m = na.meta
                    f.write(f"**{i}. {m.get('title_en') or ''}**\n\n")

                    if m.get("title_cn"):
                        f.write(f"**Title (ZH-CN)**: {m['title_cn']}\n\n")
                    if m.get("journal"):
                        f.write(f"**Journal**: {m['journal']}\n\n")
                    if m.get("type"):
                        f.write(f"**Type**: {m['type']}\n\n")
                    if m.get("pub_date"):
                        f.write(f"**Publication date**: {m['pub_date']}\n\n")
                    if m.get("doi"):
                        f.write(f"**DOI**: {m['doi']}\n\n")
                    if m.get("article_url"):
                        f.write(f"**Article URL**: {m['article_url']}\n\n")
                    if m.get("abstract_en"):
                        f.write(f"**Abstract (EN)**:\n\n{m['abstract_en']}\n\n")
                    if m.get("abstract_cn"):
                        f.write(f"**Abstract (ZH-CN)**:\n\n{m['abstract_cn']}\n\n")

                    f.write("---\n\n")

        written_paths.append(md_path)
        print(f"[OK] Markdown written to: {md_path}")

    conn.close()
    return written_paths

# -------------------------- Entry --------------------------

def main():
    # No CLI; always read from config.json
    cfg_path = os.getenv("PIPELINE_CONFIG", "config.json")
    try:
        with open(cfg_path, "r", encoding="utf-8") as rf:
            config = json.load(rf)
    except Exception as e:
        print(f"Failed to read {cfg_path}: {e}", file=sys.stderr)
        sys.exit(2)

    run_pipeline(config)

if __name__ == "__main__":
    main()
