#!/usr/bin/env python3
"""Map tous les sites d'agences via Firecrawl /v2/map.

- 1 fichier JSONL par agence : mapping/<placeId>.jsonl (1 ligne = 1 URL)
- 1 appel /map par site unique (les agences qui partagent la même URL réutilisent le résultat)
- N clés API en rotation : 1 worker par clé, tous piochent dans la même file.
  Clé épuisée (402) / invalide (401) -> elle est retirée, la tâche repart sur une autre clé.
- Reprise automatique : les agences déjà présentes dans mapping/ sont ignorées.

Variables d'environnement :
  FIRECRAWL_API_KEYS   clés séparées par virgule ou retour à la ligne (obligatoire)
  CSV_PATH             défaut: agences.csv
  OUT_DIR              défaut: mapping
  ERRORS_PATH          défaut: mapping_errors.jsonl
  MAP_LIMIT            nb max d'URLs par site, défaut: 5000
  KEY_BUDGET           crédits max utilisés par clé dans ce run, défaut: 1400
  MAX_RUNTIME_MIN      arrêt propre après N minutes, défaut: 335
  REQUEST_TIMEOUT      timeout HTTP en secondes, défaut: 120
"""
import csv
import json
import os
import queue
import re
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

import requests

API_URL = "https://api.firecrawl.dev/v2/map"

CSV_PATH = os.environ.get("CSV_PATH", "agences.csv")
OUT_DIR = Path(os.environ.get("OUT_DIR", "mapping"))
ERRORS_PATH = Path(os.environ.get("ERRORS_PATH", "mapping_errors.jsonl"))
MAP_LIMIT = int(os.environ.get("MAP_LIMIT", "5000"))
KEY_BUDGET = int(os.environ.get("KEY_BUDGET", "1400"))
MAX_RUNTIME = float(os.environ.get("MAX_RUNTIME_MIN", "335")) * 60
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "120"))
MAX_ATTEMPTS = 3

START = time.time()
write_lock = threading.Lock()
state_lock = threading.Lock()
stats = defaultdict(int)
pending = 0
stop = threading.Event()


def load_keys():
    raw = os.environ.get("FIRECRAWL_API_KEYS", "")
    keys = [k.strip() for k in re.split(r"[,\n;\s]+", raw) if k.strip()]
    seen, out = set(), []
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def norm_url(u: str) -> str:
    u = (u or "").strip()
    if u and not re.match(r"^https?://", u, re.I):
        u = "https://" + u
    return u


def url_key(u: str) -> str:
    return re.sub(r"^https?://(www\.)?", "", u.lower()).rstrip("/")


def out_path(place_id: str) -> Path:
    return OUT_DIR / f"{place_id}.jsonl"


def log_error(entry: dict):
    with write_lock:
        with ERRORS_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def write_links(place_ids, links):
    lines = []
    for item in links:
        if isinstance(item, str):
            rec = {"url": item}
        else:
            rec = {"url": item.get("url")}
            if item.get("title"):
                rec["title"] = item["title"]
            if item.get("description"):
                rec["description"] = item["description"]
        if rec.get("url"):
            lines.append(json.dumps(rec, ensure_ascii=False))
    content = "\n".join(lines) + ("\n" if lines else "")
    for pid in place_ids:
        p = out_path(pid)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(p)


def call_map(key: str, url: str):
    """Retourne (status, payload|None, retry_after)."""
    try:
        r = requests.post(
            API_URL,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"url": url, "limit": MAP_LIMIT, "sitemap": "include"},
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as e:
        return -1, str(e), 0
    retry_after = 0
    try:
        retry_after = int(r.headers.get("Retry-After", 0))
    except ValueError:
        pass
    try:
        data = r.json()
    except ValueError:
        data = {"error": r.text[:300]}
    return r.status_code, data, retry_after


def finish_task():
    global pending
    with state_lock:
        pending -= 1
        if pending <= 0:
            stop.set()


def worker(idx: int, key: str, q: "queue.Queue"):
    tag = f"key{idx + 1:02d}"
    used = 0
    while not stop.is_set():
        if time.time() - START > MAX_RUNTIME:
            print(f"[{tag}] deadline atteinte, arrêt propre", flush=True)
            stop.set()
            return
        if used >= KEY_BUDGET:
            print(f"[{tag}] budget local épuisé ({used}), clé retirée", flush=True)
            return
        try:
            task = q.get(timeout=1)
        except queue.Empty:
            continue

        url, place_ids, attempt = task["url"], task["place_ids"], task["attempt"]
        status, data, retry_after = call_map(key, url)

        if status == 200 and isinstance(data, dict) and data.get("success", True):
            used += 1
            links = data.get("links") or []
            write_links(place_ids, links)
            with state_lock:
                stats["ok"] += 1
                stats["urls"] += len(links)
                done = stats["ok"] + stats["failed"]
            if not links:
                log_error({"url": url, "place_ids": place_ids, "error": "empty"})
            if done % 100 == 0:
                print(f"[{tag}] {done} sites traités ({stats['failed']} échecs)", flush=True)
            finish_task()
        elif status in (401, 402):
            print(f"[{tag}] clé retirée (HTTP {status}) après {used} appels", flush=True)
            q.put(task)  # un autre worker reprendra
            return
        elif status == 429:
            q.put(task)
            time.sleep(max(retry_after, 15))
        elif status == -1 or status >= 500:
            if attempt + 1 < MAX_ATTEMPTS:
                task["attempt"] = attempt + 1
                q.put(task)
                time.sleep(5 * (attempt + 1))
            else:
                fail(url, place_ids, status, data)
        else:  # 400, 403, 404... -> inutile de réessayer
            used += 1
            fail(url, place_ids, status, data)


def fail(url, place_ids, status, data):
    with state_lock:
        stats["failed"] += 1
    log_error({"url": url, "place_ids": place_ids, "status": status,
               "error": (data.get("error") if isinstance(data, dict) else str(data))})
    finish_task()


def build_tasks():
    groups = defaultdict(list)
    first_url = {}
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            pid = (row.get("placeId") or "").strip()
            url = norm_url(row.get("site_web"))
            if not pid or not url:
                continue
            k = url_key(url)
            groups[k].append(pid)
            first_url.setdefault(k, url)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tasks, reused = [], 0
    for k, pids in groups.items():
        todo = [p for p in pids if not out_path(p).exists()]
        if not todo:
            continue
        done = [p for p in pids if out_path(p).exists()]
        if done:  # même site déjà mappé pour une autre agence -> copie, 0 crédit
            content = out_path(done[0]).read_text(encoding="utf-8")
            for p in todo:
                out_path(p).write_text(content, encoding="utf-8")
            reused += len(todo)
            continue
        tasks.append({"url": first_url[k], "place_ids": todo, "attempt": 0})
    total_agences = sum(len(v) for v in groups.values())
    print(f"{total_agences} agences, {len(groups)} sites uniques, "
          f"{len(tasks)} à mapper, {reused} copiées depuis un mapping existant", flush=True)
    return tasks


def main():
    global pending
    keys = load_keys()
    if not keys:
        sys.exit("FIRECRAWL_API_KEYS vide : ajoute le secret GitHub.")
    tasks = build_tasks()
    if not tasks:
        print("Rien à faire.")
        return
    print(f"{len(keys)} clés, budget {KEY_BUDGET} crédits/clé "
          f"= {len(keys) * KEY_BUDGET} crédits max pour {len(tasks)} appels", flush=True)

    q = queue.Queue()
    for t in tasks:
        q.put(t)
    pending = len(tasks)

    threads = [threading.Thread(target=worker, args=(i, k, q), daemon=True)
               for i, k in enumerate(keys)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    remaining = max(pending, 0)
    print(f"Terminé : {stats['ok']} ok, {stats['failed']} échecs, "
          f"{stats['urls']} URLs, {remaining} restants (relance le workflow pour reprendre)",
          flush=True)


if __name__ == "__main__":
    main()
