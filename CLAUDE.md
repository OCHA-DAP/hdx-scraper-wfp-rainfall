# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-wfp-rainfall** compiles country-level WFP Rainfall data from HDX into country and global datasets for use in HAPI. It reads rainfall datasets from HDX (around 200 read calls) and creates updated global and country-level datasets (around 200 write calls).

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python -m hdx.scraper.wfp_rainfall
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_wfp_rainfall.py
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline in `__main__.py`:

1. **`main`** — Verifies HDX write access, then instantiates `Pipeline` to download WFP rainfall data and generate global datasets for each year-to-date period, updating them in HDX.

### Key design points

- **Config files**: Dataset metadata lives in `src/hdx/scraper/wfp_rainfall/config/` (`hdx_dataset_static.yaml`, `project_configuration.yaml`).
- **Multiple YTD periods**: Data is processed per year-to-date period; the pipeline iterates over all available YTD keys.

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-wfp-rainfall` entry.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
