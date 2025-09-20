#!/usr/bin/env python3
import os, argparse, datetime
import pandas as pd
from urllib.parse import urlparse, urlunparse

def normalize_url(u: str) -> str:
    """Ensure URLs point to https://www.leeladiamond.com/..."""
    u = (u or "").strip()
    if not u:
        return u
    p = urlparse(u)
    if p.netloc.endswith("leeladiamond.com"):
        p = p._replace(scheme="https", netloc="www.leeladiamond.com")
    return urlunparse(p._replace(query="", fragment=""))

def iter_links(csv_source, link_col="link", chunksize=200000):
    for chunk in pd.read_csv(csv_source, dtype=str, usecols=[link_col], chunksize=chunksize):
        for url in chunk[link_col].dropna().astype(str):
            url = normalize_url(url)
            if url:
                yield url

def write_urlset_xml(file_path, urls):
    with open(file_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
        for u in urls:
            f.write("  <url>\n")
            f.write(f"    <loc>{u}</loc>\n")
            f.write("  </url>\n")
        f.write("</urlset>\n")

def write_index_xml(index_path, part_files, public_base_url):
    now = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    with open(index_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
        for name in part_files:
            loc = f"{public_base_url.rstrip('/')}/{name}"
            f.write("  <sitemap>\n")
            f.write(f"    <loc>{loc}</loc>\n")
            f.write(f"    <lastmod>{now}</lastmod>\n")
            f.write("  </sitemap>\n")
        f.write("</sitemapindex>\n")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="CSV URL or path")
    parser.add_argument("--outdir", required=True, help="Output directory")
    parser.add_argument("--basename", default="sitemap-", help="Base name for part files")
    parser.add_argument("--per-file", type=int, default=50000, help="URLs per sitemap file")
    parser.add_argument("--public-base-url", required=True, help="Base URL where sitemaps are hosted")
    parser.add_argument("--index-name", default="sitemap-index.xml", help="Sitemap index filename")
    parser.add_argument("--link-column", default="link", help="CSV column containing URLs")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    links_iter = iter_links(args.csv, args.link_column)
    buffer, part_names = [], []
    part = 1

    for url in links_iter:
        buffer.append(url)
        if len(buffer) >= args.per_file:
            part_name = f"{args.basename}{part:05d}.xml"
            write_urlset_xml(os.path.join(args.outdir, part_name), buffer)
            part_names.append(part_name)
            buffer.clear()
            part += 1

    if buffer:
        part_name = f"{args.basename}{part:05d}.xml"
        write_urlset_xml(os.path.join(args.outdir, part_name), buffer)
        part_names.append(part_name)

    index_path = os.path.join(args.outdir, args.index_name)
    write_index_xml(index_path, part_names, args.public_base_url)

    print(f"Generated {len(part_names)} sitemap part files; index at {index_path}")

if __name__ == "__main__":
    main()
