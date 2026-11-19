#!/usr/bin/env python3
"""dbt の manifest.json / catalog.json から OKF (Open Knowledge Format) v0.2 のバンドルを生成する。

決定的な変換で、LLM・乱数・ネットワークアクセスを含まない。同じ入力からは常に同じ出力が出る。

  python3 scripts/generate_okf.py --manifest target/manifest.json --catalog target/catalog.json --out target/okf

出力:
  okf/
    index.md                  目次（OKF の予約ファイル名）
    catalog-index.json        検索用の機械可読インデックス
    <layer>/<model>.md        1モデル = 1コンセプト（<layer> は models/ 以下のディレクトリ）

参照: https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict

# OKF のアクター規約（`<producer>/<version>`）に合わせる。出力の形式を変えたら上げる。
GENERATOR = "dbt-manifest-to-okf/1.0"

# 巨大な STRUCT を持つモデルは、ネストした列の展開だけで千列を超え、1ファイルが数百KBになる。
# 外れ値だけを抑える上限で、普通のモデルには効かない。
MAX_SCHEMA_COLUMNS = 150
MAX_TYPE_CHARS = 120


def yaml_scalar(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def one_line(text: str, limit: int = 300) -> str:
    flat = re.sub(r"\s+", " ", (text or "").strip())
    return flat if len(flat) <= limit else flat[: limit - 1].rstrip() + "…"


def fq_name(node: dict) -> str:
    """relation_name の引用符（BigQuery はバッククォート、DuckDB などはダブルクォート）を外す。"""
    return re.sub(r'[`"]', "", node.get("relation_name") or "")


def layer_of(node: dict) -> str:
    """models/ 以下のディレクトリ。fqn は [プロジェクト名, ディレクトリ..., モデル名]。"""
    return "/".join(node["fqn"][1:-1])


def okf_enabled(node: dict) -> bool:
    """meta.okf: false を付けたモデルはカタログに載せない。既定は載せる。"""
    meta = (node.get("config") or {}).get("meta") or {}
    return bool(meta.get("okf", True))


def summarize_type(type_str: str) -> str:
    if len(type_str) <= MAX_TYPE_CHARS:
        return type_str
    return type_str[:MAX_TYPE_CHARS].rstrip() + "… (省略)"


def cap_columns(merged: list[tuple[str, str, str]]) -> tuple[list[tuple[str, str, str]], int]:
    """列が多すぎるときは、ネストの浅い列から残す。同じ深さの中では元の順序を保つ。"""
    if len(merged) <= MAX_SCHEMA_COLUMNS:
        return merged, 0
    indexed = sorted(enumerate(merged), key=lambda x: (x[1][0].count("."), x[0]))
    kept = sorted(indexed[:MAX_SCHEMA_COLUMNS], key=lambda x: x[0])
    return [item for _, item in kept], len(merged) - MAX_SCHEMA_COLUMNS


def merge_columns(node: dict, catalog_node: dict | None) -> list[tuple[str, str, str]]:
    """列の並びと説明は manifest、型は catalog から取る。catalog にしかない列は後ろに足す。"""
    columns = node.get("columns") or {}
    cat_columns = (catalog_node or {}).get("columns") or {}
    # catalog の列名は大文字のことがある（Snowflake など）ので、照合は小文字で行う
    cat_by_lower = {k.lower(): v for k, v in cat_columns.items()}

    merged = []
    seen = set()
    for col_name, col in columns.items():
        seen.add(col_name.lower())
        ctype = (cat_by_lower.get(col_name.lower()) or {}).get("type") or ""
        desc = re.sub(r"\s*\n\s*", " ", (col.get("description") or "").strip())
        merged.append((col_name, ctype, desc))
    for col_name, col in sorted(cat_columns.items(), key=lambda x: x[1].get("index", 0)):
        if col_name.lower() not in seen:
            merged.append((col_name, col.get("type") or "", ""))
    return merged


def build_concept(node: dict, catalog_node: dict | None, scheme: str) -> str:
    name = node["name"]
    fq = fq_name(node)
    description = (node.get("description") or "").strip()
    materialized = (node.get("config") or {}).get("materialized")
    tags = sorted(set(list(node.get("tags") or []) + [t for t in (layer_of(node), materialized) if t]))

    fm = ["---", "type: table", f"title: {yaml_scalar(name)}"]
    fm.append(f"resource: {yaml_scalar(f'{scheme}://{fq}')}")
    if description:
        fm.append(f"description: {yaml_scalar(one_line(description))}")
    if tags:
        fm.append("tags:")
        fm.extend(f"  - {yaml_scalar(t)}" for t in tags)
    fm += ["generated:", f"  by: {yaml_scalar(GENERATOR)}", "---"]

    out = ["\n".join(fm), "", f"# {name}", ""]
    if description:
        out += [description, ""]

    merged = merge_columns(node, catalog_node)
    if merged:
        shown, omitted = cap_columns(merged)
        out += ["# Schema", "", f"`{fq}`", "", "| column | type | description |", "|---|---|---|"]
        for col_name, ctype, desc in shown:
            cell = desc.replace("|", "\\|")
            out.append(f"| `{col_name}` | {summarize_type(ctype)} | {cell} |")
        out.append("")
        if omitted:
            out += [f"> ネストの深い列 {omitted} 件を省略しています（全 {len(merged)} 列）。", ""]

    return "\n".join(out).rstrip() + "\n"


def build_index(entries: list[dict], title: str) -> str:
    by_layer: dict[str, list[dict]] = defaultdict(list)
    for e in entries:
        by_layer[e["layer"]].append(e)

    out = ["---", "type: index", f"title: {yaml_scalar(title)}", "generated:", f"  by: {yaml_scalar(GENERATOR)}", "---", ""]
    out += [f"# {title}", "", f"dbt の manifest から自動生成。{len(entries)} テーブル。", ""]
    for layer in sorted(by_layer):
        out += [f"## {layer or '(models 直下)'}", ""]
        for e in sorted(by_layer[layer], key=lambda x: x["name"]):
            summary = f" — {e['summary']}" if e["summary"] else ""
            out.append(f"- [{e['name']}]({e['path']}){summary}")
        out.append("")
    return "\n".join(out).rstrip() + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--catalog")
    ap.add_argument("--out", required=True)
    ap.add_argument("--title", help="index.md の見出し。省略時は「<dbt のプロジェクト名> データカタログ」")
    args = ap.parse_args()

    with open(args.manifest) as f:
        manifest = json.load(f)
    metadata = manifest.get("metadata") or {}

    # manifest の構造はバージョンで変わる。黙って壊れないよう、想定外のバージョンでは止める
    schema_version = metadata.get("dbt_schema_version", "")
    if "manifest/v12" not in schema_version:
        raise SystemExit(f"未対応の manifest schema です: {schema_version}（v12 を前提にしています）")

    catalog_nodes = {}
    if args.catalog:
        with open(args.catalog) as f:
            catalog_nodes = (json.load(f) or {}).get("nodes") or {}

    scheme = metadata.get("adapter_type") or "dbt"
    project = metadata.get("project_name") or "dbt"
    title = args.title or f"{project} データカタログ"

    models = {k: v for k, v in (manifest.get("nodes") or {}).items() if v.get("resource_type") == "model"}
    # ephemeral は CTE として展開されるだけで実体のテーブルが無く、載せると存在しないテーブルに誘導する
    materialized = {
        k: v for k, v in models.items()
        if fq_name(v) and (v.get("config") or {}).get("materialized") != "ephemeral"
    }
    targets = {k: v for k, v in materialized.items() if okf_enabled(v)}

    entries = []
    for key, node in sorted(targets.items()):
        rel_path = "/".join(node["fqn"][1:]) + ".md"
        abs_path = os.path.join(args.out, rel_path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, "w") as f:
            f.write(build_concept(node, catalog_nodes.get(key), scheme))
        entries.append({
            "name": node["name"],
            "layer": layer_of(node),
            "resource": f"{scheme}://{fq_name(node)}",
            "path": rel_path,
            "summary": one_line(node.get("description") or "", 120),
            "tags": sorted(set(node.get("tags") or [])),
        })

    with open(os.path.join(args.out, "index.md"), "w") as f:
        f.write(build_index(entries, title))

    # 読む側はこれだけを読んで検索し、必要なコンセプトだけを取りに行く。
    # OKF の予約ファイル名は index.md / log.md だけなので、.json は仕様とぶつからない
    index_json = {
        "generated_by": GENERATOR,
        "dbt_version": metadata.get("dbt_version"),
        "count": len(entries),
        "tables": sorted(entries, key=lambda x: (x["layer"], x["name"])),
    }
    with open(os.path.join(args.out, "catalog-index.json"), "w") as f:
        json.dump(index_json, f, ensure_ascii=False, indent=1, sort_keys=True)

    with_desc = sum(1 for e in entries if e["summary"])
    print(f"manifest schema : {schema_version}")
    print(f"生成            : {len(entries)} テーブル → {args.out}")
    print(f"除外(ephemeral) : {len(models) - len(materialized)}")
    # 設定ミスで載っているつもりのモデルが消えても気づけるよう、除外した数を出す
    print(f"除外(meta.okf)  : {len(materialized) - len(targets)}")
    if entries:
        print(f"description 有り: {with_desc}/{len(entries)}")


if __name__ == "__main__":
    main()
