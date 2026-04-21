# Legacy Power Query Archive

Source materials extracted from the legacy MLB Power BI workflow. Reference-only; not part of the production ETL. These files exist so future sessions can reason about the PBI's data lineage without having to re-parse source docx files.

## Committed artifacts

- `MLB_PBIX_REFERENCE.md` - Authoritative reference document. Covers page inventory, model tables, visual field bindings, M query catalog, dependency map, cleanup recommendations, and Azure SQL schema design hints. Start here.
- `miscMLBinstructions_full.txt` - Extracted text of `miscMLBinstructions.docx`. Contains the M code authoring rules and the fnGet/all consolidation pattern spec.
- `m_query_catalog.json` - Structured index of all 61 query sections in `mlbStatQueries.docx`. Each entry records source line range, API URL(s), output column count, and sample column names.
- `pbix_visual_catalog.json` - Compact JSON inventory of all 135 visuals across all 10 pages. Each visual is recorded as `{id, type, title, refs}` where `refs` is a list of `[kind, table, field]` triples. Alias tables (`b`, `p`, `m`, `t`, `u`, `p1`, `p2`) are preserved as they appear in the DAX projections; see section 3.12 of `MLB_PBIX_REFERENCE.md` for alias resolution.

## Pending artifacts

- `mlbStatQueries_full.txt` - Verbatim extracted M code from `mlbStatQueries.docx`. 225 KB, 4508 lines across 61 named query sections. Not committed here because a single 225 KB paste is at the edge of what the GitHub MCP inline-commit path handles reliably. To commit locally from `C:\Users\1stLake\sports-modeling\etl\mlb\_legacy_powerquery`:

```powershell
# Regenerate from source on demand:
python -c "from docx import Document; d = Document(r'C:\Users\1stLake\OneDrive - Schnapp\mlbStatQueries.docx'); open('mlbStatQueries_full.txt','w',encoding='utf-8').write('\n'.join(p.text for p in d.paragraphs))"
git add mlbStatQueries_full.txt
git commit -m "docs: archive raw M code from mlbStatQueries.docx"
git push
```

The parsed `m_query_catalog.json` already captures the line range, endpoint URL, and column counts for all 61 query sections, so the raw text is useful only when you need to inspect a specific query's exact transformation logic.

## Regeneration

If any of the above artifacts need to be regenerated from source, the source files are at:
- `C:\Users\1stLake\OneDrive - Schnapp\mlbStatQueries.docx` (M code, via python-docx)
- `C:\Users\1stLake\OneDrive - Schnapp\miscMLBinstructions.docx` (authoring rules, via python-docx)
- `C:\Users\1stLake\OneDrive - Schnapp\mlbSavantV3.pbix` (visual catalog, via `unzip` then parse `Report/definition/pages/*/visuals/*/visual.json`)

## Relationship to production ETL

Nothing in this directory is executed by production ETL. Production MLB ingestion lives (or will live) in `etl/mlb_etl.py` and related Python scripts under `etl/`. This archive exists only so that the schema design work can reference the legacy Power Query behavior, and so that any MLB-specific business logic encoded in the PBI measures (HR Pattern Early/Late, Pattern HitRate, L5AB EV, etc.) can be translated into SQL views or ETL transformations.

Delete this entire directory once the MLB Azure SQL schema is live and all legacy PBI business logic has been captured in production ETL, DDL, or documentation.
