# -*- coding: utf-8 -*-
"""
Stream a gigantic CSV of product links and emit multiple sitemap part files (50k URLs each by default)
plus a sitemap index whose <loc> entries use the PUBLIC_BASE_URL (set to https://www.leeladiamond.com/sitemaps).
"""

import argparse
import csv
import datetime as dt
import os
from typing import Iterable, List

import pandas as pd


def iter_links(csv_source: str, link_col: str = "link", chunksize: int = 200_000) -> Iterable[str]:
    """
    Stream URLs from a (very large) CSV using pandas chunks.
    Only the link_col is read. Empty/NA trimmed out.
    """
    for chunk in pd.read_csv(csv_source, dtype=str, usecols=[link_col], chunksize=chunksize):
        s = chunk[link_col].dropna().astype(str).str.strip()
        for u in s[s.ne("")].tolist():
            yield u


def write_urlset_xml(path: str, urls: List[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
        for u in urls:
            # basic XML escaping for <loc>
            u = u.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            f.write("  <url>\n")
            f.write(f"    <loc>{u}</loc>\n")
            f.write("  </url>\n")
        f.write("</urlset>\n")


def write_index_xml(index_path: str, base_url: str, part_names: List[str]) -> None:
    base_url = base_url.rstrip("/")
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    with open(index_path, "w", encoding="utf-8", newline="") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
        for name in part_names:
            loc = f"{base_url}/{name}"
            f.write("  <sitemap>\n")
            f.write(f"    <loc>{loc}</loc>\n")
            f.write(f"    <lastmod>{now}</lastmod>\n")
            f.write("  </sitemap>\n")
        f.write("</sitemapindex>\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="CSV URL or path")
    ap.add_argument("--outdir", default="./sitemaps_out", help="Output folder")
    ap.add_argument("--basename", default="leela-products-", help="Prefix for part files")
    ap.add_argument("--per-file", type=int, default=50000, help="URLs per sitemap part (max 50000)")
    ap.add_argument("--public-base-url", required=True, help="Public base for <loc> in index (e.g. https://www.leeladiamond.com/sitemaps)")
    ap.add_argument("--index-name", default="sitemap-index.xml", help="Index filename")
    ap.add_argument("--link-column", default="link", help="Column in CSV with URLs")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    urls_iter = iter_links(args.csv, link_col=args.link_column)

    part_names: List[str] = []
    buf: List[str] = []
    part = 1

    for u in urls_iter:
        buf.append(u)
        if len(buf) >= args.per_file:
            name = f"{args.basename}{part:05d}.xml"
            write_urlset_xml(os.path.join(args.outdir, name), buf)
            part_names.append(name)
            buf.clear()
            part += 1

    if buf:
        name = f"{args.basename}{part:05d}.xml"
        write_urlset_xml(os.path.join(args.outdir, name), buf)
        part_names.append(name)

    if not part_names:
        raise SystemExit("No URLs found in the CSV; aborting.")

    write_index_xml(os.path.join(args.outdir, args.index_name), args.public_base_url, part_names)
    print(f"OK: wrote {len(part_names)} parts + index '{args.index_name}' to {args.outdir}")


if __name__ == "__main__":
    main()
