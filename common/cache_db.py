import aiosqlite
import time
import json
from datetime import datetime, timedelta

DB_PATH = "cache/cache.db"

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS tag_cache (
    tag TEXT PRIMARY KEY,
    links TEXT NOT NULL,
    expires_at INTEGER
);

CREATE TABLE IF NOT EXISTS gallery_cache (
    url TEXT PRIMARY KEY,
    tag TEXT,
    snippets TEXT NOT NULL,
    expires_at INTEGER
);

CREATE TABLE IF NOT EXISTS history_tags (
    tag TEXT PRIMARY KEY,
    added_at INTEGER
);
"""

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_TABLES)
        await db.commit()


# -------------------------------
#  TAG CACHE
# -------------------------------
async def load_tag(tag: str, ttl_days: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT links, expires_at FROM tag_cache WHERE tag=?",
            (tag.lower(),)
        )
        row = await c.fetchone()

    if not row:
        return None

    links_json, expires_at = row

    if time.time() > expires_at:
        return None

    return json.loads(links_json)


async def save_tag(tag: str, links: list, ttl_days: int):
    expires = time.time() + ttl_days * 86400

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "REPLACE INTO tag_cache (tag, links, expires_at) VALUES (?, ?, ?)",
            (tag.lower(), json.dumps(links), expires)
        )
        await db.commit()

async def add_tags(tags: list[str]):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        for t in tags:
            await db.execute(
                "REPLACE INTO history_tags (tag, added_at) VALUES (?, ?)",
                (t.lower(), now)
            )
        await db.commit()


async def get_last(n=None):
    async with aiosqlite.connect(DB_PATH) as db:
        if n is None:  # all
            c = await db.execute(
                "SELECT tag FROM history_tags ORDER BY added_at ASC"
            )
        else:
            c = await db.execute(
                "SELECT tag FROM history_tags ORDER BY added_at DESC LIMIT ?",
                (n,)
            )

        rows = await c.fetchall()

    # newest first if n specified, else original order
    return [r[0] for r in rows]
# -------------------------------
#  GALLERY CACHE
# -------------------------------
async def load_gallery(url: str, ttl_days: int):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "SELECT snippets, expires_at FROM gallery_cache WHERE url=?",
            (url,)
        )
        row = await c.fetchone()

    if not row:
        return None

    snippets_json, expires_at = row

    if time.time() > expires_at:
        return None

    return json.loads(snippets_json)


async def save_gallery(url: str, tag: str, snippets: list, ttl_days: int):
    expires = time.time() + ttl_days * 86400

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "REPLACE INTO gallery_cache (url, tag, snippets, expires_at) VALUES (?, ?, ?, ?)",
            (url, tag, json.dumps(snippets), expires)
        )
        await db.commit()
