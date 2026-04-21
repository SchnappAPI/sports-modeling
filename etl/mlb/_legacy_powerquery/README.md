# Legacy Power Query Archive

Source materials extracted from the legacy MLB Power BI workflow. Reference-only; not part of the production ETL. These files exist so future sessions can reason about the PBI's data lineage without having to re-parse source docx files.

## Committed artifacts

- `MLB_PBIX_REFERENCE.md` - Authoritative reference document. Covers page inventory, model tables, visual field bindings, M query catalog, dependency map, cleanup recommendations, and Azure SQL schema design hints. Start here.
- `miscMLBinstructions_full.txt` - Extracted text of `miscMLBinstructions.docx`. Contains the M code authoring rules and the fnGet/all consolidation pattern spec.
- `m_query_catalog.json` - Structured index of all 61 query sections in `mlbStatQueries.docx`. Each entry records source line range, API URL(s), output column count, and sample column names.

## Pending artifacts (to be committed from local)

These files are too large for reliable inline commit via the API. They are preserved on Austin's machine and should be committed using a local `git add` / `git commit` / `git push` from `C:\Users\1stLake\sports-modeling`:

- `mlbStatQueries_full.txt` - Verbatim extracted M code from `mlbStatQueries.docx`. 225 KB, 4508 lines. The raw source for every entry in `m_query_catalog.json`.
- `pbix_visual_catalog.json` - Full structured inventory of all 135 visuals across 10 pages, with every field reference preserved. 164 KB.

**To commit locally:**

```powershell
# From C:\Users\1stLake\sports-modeling
cd etl\mlb\_legacy_powerquery
# Copy the extraction artifacts into this directory first
# (Claude will email or provide them separately if needed, or they can be regenerated from the docx/pbix sources on demand)
git add mlbStatQueries_full.txt pbix_visual_catalog.json
git commit -m "docs: archive raw M code and visual catalog extracts"
git push
```

If the files are no longer available locally, they can be regenerated from:
- `C:\Users\1stLake\OneDrive - Schnapp\mlbStatQueries.docx` via python-docx text extraction
- `C:\Users\1stLake\OneDrive - Schnapp\mlbSavantV3.pbix` via zip extraction + parsing of `Report/definition/pages/*/visuals/*/visual.json`

The regeneration scripts can be reconstructed from the workflow documented in `MLB_PBIX_REFERENCE.md`.

## Relationship to production ETL

Nothing in this directory is executed by production ETL. Production MLB ingestion lives (or will live) in `etl/mlb_etl.py` and related Python scripts under `etl/`. This archive exists only so that the schema design work can reference the legacy Power Query behavior, and so that any MLB-specific business logic encoded in the PBI measures (HR Pattern Early/Late, Pattern HitRate, L5AB EV, etc.) can be translated into SQL views or ETL transformations.

Delete this entire directory once the MLB Azure SQL schema is live and all legacy PBI business logic has been captured in production ETL, DDL, or documentation.
