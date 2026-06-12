#!/usr/bin/env python3
import bz2
import html.parser
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "hw5" / "data"
DUMP_DIR = DATA / "wiki_dump"
OUT = DATA / "wiki_sample.jsonl"
INDEX_URL = "https://dumps.wikimedia.org/enwiki/latest/"
TARGET_GB = float(os.environ.get("TARGET_GB", "6"))
DOCS = int(os.environ.get("DOCS", "0"))
MAX_BYTES = int(TARGET_GB * 1024 * 1024 * 1024)
INSECURE_SSL = os.environ.get("WIKI_INSECURE_SSL", "0") == "1"
SSL_CONTEXT = ssl._create_unverified_context() if INSECURE_SSL else None
DOWNLOAD_RETRIES = int(os.environ.get("WIKI_DOWNLOAD_RETRIES", "8"))
DOWNLOAD_TIMEOUT = int(os.environ.get("WIKI_DOWNLOAD_TIMEOUT", "600"))


class LinkParser(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        href = dict(attrs).get("href", "")
        if re.fullmatch(r"enwiki-latest-pages-articles\d+\.xml-p\d+p\d+\.bz2", href):
            self.links.append(href)


def fetch_links():
    with urlopen(INDEX_URL, timeout=60) as resp:
        html = resp.read().decode("utf-8", errors="replace")
    parser = LinkParser()
    parser.feed(html)
    return sorted(set(parser.links), key=natural_chunk_key)


def download(url, path):
    if path.exists() and path.stat().st_size > 0:
        print(f"[wiki-download] using cached {path.name}")
        return
    tmp = path.with_suffix(path.suffix + ".part")
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        existing = tmp.stat().st_size if tmp.exists() else 0
        print(f"[wiki-download] downloading {url} attempt={attempt}/{DOWNLOAD_RETRIES} resume={existing} bytes")
        try:
            with urlopen(url, timeout=DOWNLOAD_TIMEOUT, start=existing) as resp, tmp.open("ab") as out:
                status = getattr(resp, "status", None)
                if existing > 0 and status == 200:
                    print("[wiki-download] server ignored Range; restarting partial file")
                    out.close()
                    tmp.unlink(missing_ok=True)
                    existing = 0
                    with urlopen(url, timeout=DOWNLOAD_TIMEOUT, start=0) as retry_resp, tmp.open("wb") as retry_out:
                        copy_response(retry_resp, retry_out)
                else:
                    copy_response(resp, out)
            tmp.rename(path)
            return
        except (TimeoutError, urllib.error.URLError, OSError) as err:
            if attempt >= DOWNLOAD_RETRIES:
                raise
            sleep_for = min(60, 2 ** attempt)
            print(f"[wiki-download] transient download error: {err}; retrying in {sleep_for}s")
            time.sleep(sleep_for)


def copy_response(resp, out):
    while True:
        chunk = resp.read(1024 * 1024)
        if not chunk:
            break
        out.write(chunk)
        out.flush()


def urlopen(url, timeout, start=0):
    try:
        headers = {"User-Agent": "algorithms-itmo-hw5/1.0"}
        if start > 0:
            headers["Range"] = f"bytes={start}-"
        req = urllib.request.Request(url, headers=headers)
        return urllib.request.urlopen(req, timeout=timeout, context=SSL_CONTEXT)
    except urllib.error.URLError as err:
        reason = getattr(err, "reason", err)
        if isinstance(reason, ssl.SSLCertVerificationError) or "CERTIFICATE_VERIFY_FAILED" in str(err):
            message = """
[wiki-download] TLS certificate verification failed for Python.

Fix option 1, recommended on macOS Python.org installs:
  open "/Applications/Python 3.10/Install Certificates.command"

Fix option 2, one-off benchmark download without TLS verification:
  WIKI_INSECURE_SSL=1 DOWNLOAD=1 TARGET_GB=6 scripts/hw5_prepare_wiki_sample.sh

The insecure mode is only for downloading the public Wikimedia dump when local Python
certificates are broken.
"""
            raise SystemExit(message) from err
        raise


def natural_chunk_key(name):
    match = re.search(r"articles(\d+)\.xml-p(\d+)p(\d+)\.bz2", name)
    if not match:
        return (10**9, name)
    return tuple(int(part) for part in match.groups())


def clean_wikitext(text):
    text = re.sub(r"<ref[^>]*>.*?</ref>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\{\{[^{}]*\}\}", " ", text)
    text = re.sub(r"\[\[([^|\]]*\|)?([^\]]+)\]\]", r"\2", text)
    text = re.sub(r"\[https?://[^\s\]]+\s*([^\]]*)\]", r"\1", text)
    text = re.sub(r"'{2,}", "", text)
    text = re.sub(r"={2,}[^=]+={2,}", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_chunk(path, out, state):
    ns_uri = None
    with bz2.open(path, "rb") as f:
        for event, elem in ET.iterparse(f, events=("start", "end")):
            if ns_uri is None and event == "start" and elem.tag.startswith("{"):
                ns_uri = elem.tag.split("}", 1)[0][1:]
            if event != "end" or not elem.tag.endswith("page"):
                continue
            def find(name):
                if ns_uri:
                    return elem.find(f"{{{ns_uri}}}{name}")
                return elem.find(name)
            title_el = find("title")
            ns_el = find("ns")
            id_el = find("id")
            rev = find("revision")
            text_el = None
            if rev is not None:
                text_el = rev.find(f"{{{ns_uri}}}text") if ns_uri else rev.find("text")
            if ns_el is not None and ns_el.text == "0" and id_el is not None and title_el is not None and text_el is not None:
                text = clean_wikitext(text_el.text or "")
                if len(text) >= 200:
                    rec = {"id": int(id_el.text), "title": title_el.text or "", "text": text}
                    line = json.dumps(rec, ensure_ascii=False) + "\n"
                    out.write(line)
                    state["docs"] += 1
                    state["bytes"] += len(line.encode("utf-8"))
                    if state["docs"] % 1000 == 0:
                        print(f"[wiki-download] docs={state['docs']} jsonl={state['bytes'] / (1024**3):.2f}GB")
            elem.clear()
            if (DOCS and state["docs"] >= DOCS) or state["bytes"] >= MAX_BYTES:
                return True
    return False


def main():
    DATA.mkdir(parents=True, exist_ok=True)
    DUMP_DIR.mkdir(parents=True, exist_ok=True)
    if OUT.exists() and OUT.stat().st_size >= MAX_BYTES:
        print(f"[wiki-download] existing sample is already large enough: {OUT}")
        return
    print(f"[wiki-download] official source: {INDEX_URL}")
    print(f"[wiki-download] target JSONL size: {TARGET_GB:.1f}GB, docs limit: {DOCS or 'none'}")
    if INSECURE_SSL:
        print("[wiki-download] WARNING: TLS certificate verification is disabled")
    links = fetch_links()
    if not links:
        raise SystemExit("no pages-articles chunks found")
    state = {"docs": 0, "bytes": 0}
    mode = "a" if OUT.exists() else "w"
    if OUT.exists():
        state["bytes"] = OUT.stat().st_size
        with OUT.open("rb") as f:
            state["docs"] = sum(1 for _ in f)
    with OUT.open(mode, encoding="utf-8") as out:
        for href in links:
            chunk = DUMP_DIR / href
            download(INDEX_URL + href, chunk)
            done = parse_chunk(chunk, out, state)
            out.flush()
            print(f"[wiki-download] after {href}: docs={state['docs']} jsonl={state['bytes'] / (1024**3):.2f}GB")
            if done:
                break
    print(f"[wiki-download] written {OUT}")
    print(f"[wiki-download] docs={state['docs']} jsonl={state['bytes'] / (1024**3):.2f}GB")


if __name__ == "__main__":
    main()
