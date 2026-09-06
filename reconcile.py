#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.27", "pydantic>=2.7", "pyyaml>=6", "python-dotenv>=1"]
# ///
"""Reconcile a Miniflux instance with feeds.yaml.

Identity: categories by title, feeds by url. Feeds and categories absent from
the file are deleted unless --no-prune. Use --dry-run to print the plan only.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx
import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field

DEFAULT_CATEGORY = "All"  # built-in, cannot be deleted


class FeedOpts(BaseModel):
    """Per-feed options accepted by POST/PUT /v1/feeds."""

    model_config = ConfigDict(extra="forbid")

    crawler: bool | None = None
    disabled: bool | None = None
    hide_globally: bool | None = None
    ignore_http_cache: bool | None = None
    allow_self_signed_certificates: bool | None = None
    fetch_via_proxy: bool | None = None
    no_media_player: bool | None = None
    user_agent: str | None = None
    cookie: str | None = None
    username: str | None = None
    password: str | None = None
    proxy_url: str | None = None
    scraper_rules: str | None = None
    rewrite_rules: str | None = None
    urlrewrite_rules: str | None = None
    disable_http2: bool | None = None
    ignore_entry_updates: bool | None = None
    webhook_url: str | None = None
    blocklist_rules: str | None = None
    keeplist_rules: str | None = None
    block_filter_entry_rules: str | None = None
    keep_filter_entry_rules: str | None = None
    description: str | None = None
    site_url: str | None = None

    def set_fields(self) -> dict[str, Any]:
        return {k: v for k, v in self.model_dump().items() if v is not None}


class Feed(FeedOpts):
    url: str
    title: str | None = None


class Category(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str
    hide_globally: bool = False
    feeds: list[Feed] = Field(default_factory=list)


class Fever(BaseModel):
    """Fever API credentials. Miniflux stores md5("user:pass") as fever_token."""

    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    username: str
    password: str

    @property
    def token(self) -> str:
        return hashlib.md5(f"{self.username}:{self.password}".encode()).hexdigest()


class Integrations(BaseModel):
    model_config = ConfigDict(extra="forbid")
    fever: Fever | None = None


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")
    user: dict[str, Any] | None = None
    integrations: Integrations | None = None
    feed_defaults: FeedOpts = Field(default_factory=FeedOpts)
    categories: list[Category]


class Client:
    def __init__(self, url: str, key: str) -> None:
        self.http = httpx.Client(
            base_url=url.rstrip("/") + "/v1",
            headers={"X-Auth-Token": key},
            timeout=60,  # feed creation fetches the feed synchronously
        )

    def req(self, method: str, path: str, **kw: Any) -> Any:
        r = self.http.request(method, path, **kw)
        if r.status_code >= 400:
            sys.exit(f"{method} {path} -> {r.status_code}: {r.text}")
        return r.json() if r.content else None


class Db:
    """Run SQL in the postgres container over ssh; needed only for integrations."""

    def __init__(self) -> None:
        self.ssh = os.environ.get("MINIFLUX_SSH")
        self.container = os.environ.get("MINIFLUX_DB_CONTAINER", "miniflux-db")
        if not self.ssh:
            sys.exit("integrations need MINIFLUX_SSH (user@host) in .env")

    def sql(self, query: str) -> str:
        cmd = [
            "ssh",
            "-o",
            "BatchMode=yes",
            self.ssh,
            f"PATH=$PATH:/usr/local/bin docker exec -i {self.container} psql -U miniflux -tAq",
        ]
        r = subprocess.run(
            cmd, input=query, capture_output=True, text=True, check=False
        )
        if r.returncode != 0 or "ERROR" in r.stderr:
            sys.exit(f"sql failed: {r.stderr.strip()}")
        return r.stdout.strip()


def q(v: str) -> str:
    """SQL-quote a string literal."""
    return "'" + v.replace("'", "''") + "'"


class Action:
    def __init__(self, desc: str, run) -> None:
        self.desc = desc
        self.run = run

    def __repr__(self) -> str:
        return self.desc


def norm_url(u: str) -> str:
    return u.strip().rstrip("/")


def plan(cfg: Config, c: Client, prune: bool) -> list[Action]:
    actions: list[Action] = []
    # ids resolved lazily so created categories can be referenced by later steps
    cat_ids: dict[str, int] = {}

    # --- categories ---
    remote_cats = {x["title"]: x for x in c.req("GET", "/categories")}
    for cat in cfg.categories:
        if cat.title in remote_cats:
            rc = remote_cats[cat.title]
            cat_ids[cat.title] = rc["id"]
            if rc.get("hide_globally", False) != cat.hide_globally:
                actions.append(
                    Action(
                        f"category update  {cat.title}: hide_globally={cat.hide_globally}",
                        lambda rc=rc, cat=cat: c.req(
                            "PUT",
                            f"/categories/{rc['id']}",
                            json={
                                "title": cat.title,
                                "hide_globally": cat.hide_globally,
                            },
                        ),
                    )
                )
        else:

            def create_cat(cat=cat) -> None:
                r = c.req("POST", "/categories", json={"title": cat.title})
                cat_ids[cat.title] = r["id"]
                if cat.hide_globally:
                    c.req(
                        "PUT",
                        f"/categories/{r['id']}",
                        json={"title": cat.title, "hide_globally": True},
                    )

            actions.append(Action(f"category create  {cat.title}", create_cat))

    # --- feeds ---
    remote_feeds = {norm_url(f["feed_url"]): f for f in c.req("GET", "/feeds")}
    desired_urls: set[str] = set()
    for cat in cfg.categories:
        for feed in cat.feeds:
            key = norm_url(feed.url)
            desired_urls.add(key)
            opts = cfg.feed_defaults.set_fields() | feed.set_fields()
            opts.pop("url", None)
            opts.pop("title", None)
            rf = remote_feeds.get(key)
            if rf is None:

                def create_feed(feed=feed, cat=cat, opts=opts) -> None:
                    r = c.req(
                        "POST",
                        "/feeds",
                        json={"feed_url": feed.url, "category_id": cat_ids[cat.title]}
                        | opts,
                    )
                    if feed.title:
                        c.req(
                            "PUT", f"/feeds/{r['feed_id']}", json={"title": feed.title}
                        )

                actions.append(
                    Action(f"feed create      [{cat.title}] {feed.url}", create_feed)
                )
                continue
            # diff managed fields against remote
            diff: dict[str, Any] = {}
            if rf["category"]["title"] != cat.title:
                diff["category_id"] = cat.title  # resolved at apply time
            if feed.title and rf["title"] != feed.title:
                diff["title"] = feed.title
            for k, v in opts.items():
                if rf.get(k) != v:
                    diff[k] = v
            if diff:
                shown = ", ".join(f"{k}={v!r}" for k, v in diff.items())

                def update_feed(rf=rf, diff=diff) -> None:
                    body = dict(diff)
                    if "category_id" in body:
                        body["category_id"] = cat_ids[body["category_id"]]
                    c.req("PUT", f"/feeds/{rf['id']}", json=body)

                actions.append(
                    Action(f"feed update      {rf['feed_url']}: {shown}", update_feed)
                )

    if prune:
        for key, rf in remote_feeds.items():
            if key not in desired_urls:
                actions.append(
                    Action(
                        f"feed DELETE      [{rf['category']['title']}] {rf['feed_url']}",
                        lambda rf=rf: c.req("DELETE", f"/feeds/{rf['id']}"),
                    )
                )
        desired_cats = {cat.title for cat in cfg.categories}
        for title, rc in remote_cats.items():
            if title not in desired_cats and title != DEFAULT_CATEGORY:
                actions.append(
                    Action(
                        f"category DELETE  {title}",
                        lambda rc=rc: c.req("DELETE", f"/categories/{rc['id']}"),
                    )
                )

    me = c.req("GET", "/me")

    # --- integrations (direct SQL, no API) ---
    if cfg.integrations and cfg.integrations.fever:
        fv, db = cfg.integrations.fever, Db()
        cur = db.sql(
            f"select fever_enabled, fever_username, fever_token from integrations where user_id={me['id']}"
        )
        want = f"{'t' if fv.enabled else 'f'}|{fv.username}|{fv.token}"
        if cur != want:
            actions.append(
                Action(
                    f"fever update     enabled={fv.enabled} username={fv.username!r}",
                    lambda: db.sql(
                        f"update integrations set fever_enabled={fv.enabled}, "
                        f"fever_username={q(fv.username)}, fever_token={q(fv.token)} "
                        f"where user_id={me['id']}"
                    ),
                )
            )

    # --- user settings ---
    if cfg.user:
        udiff = {k: v for k, v in cfg.user.items() if me.get(k) != v}
        unknown = set(cfg.user) - set(me)
        if unknown:
            sys.exit(f"unknown user settings: {sorted(unknown)}")
        if udiff:
            shown = ", ".join(f"{k}: {me.get(k)!r} -> {v!r}" for k, v in udiff.items())
            actions.append(
                Action(
                    f"user update      {shown}",
                    lambda: c.req("PUT", f"/users/{me['id']}", json=udiff),
                )
            )
    return actions


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "config", nargs="?", default=Path(__file__).with_name("feeds.yaml"), type=Path
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="print the plan, change nothing"
    )
    ap.add_argument(
        "--no-prune", action="store_true", help="keep feeds/categories not in the file"
    )
    args = ap.parse_args()

    load_dotenv(Path(__file__).with_name(".env"))
    url, key = os.environ.get("MINIFLUX_URL"), os.environ.get("MINIFLUX_API_KEY")
    if not url or not key:
        sys.exit("set MINIFLUX_URL and MINIFLUX_API_KEY (in .env or the environment)")

    text = args.config.read_text()
    missing = [v for v in re.findall(r"\$\{(\w+)\}", text) if v not in os.environ]
    if missing:
        sys.exit(
            f"unset variables referenced in {args.config.name}: {sorted(set(missing))}"
        )
    text = re.sub(r"\$\{(\w+)\}", lambda m: os.environ[m.group(1)], text)
    cfg = Config.model_validate(yaml.safe_load(text))
    actions = plan(cfg, Client(url, key), prune=not args.no_prune)

    if not actions:
        print("in sync, nothing to do")
        return
    for a in actions:
        print(("plan  " if args.dry_run else "apply ") + a.desc)
        if not args.dry_run:
            a.run()
    print(f"{len(actions)} action(s) {'planned' if args.dry_run else 'applied'}")


if __name__ == "__main__":
    main()
