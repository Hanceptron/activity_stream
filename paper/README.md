# KeySpark - project paper

LaTeX source for the BDA5011 project paper. IEEE conference format
(`IEEEtran`).

## Compile

```sh
latexmk -pdf main.tex      # local TeX (TeX Live / MacTeX)
# or: upload this whole `paper/` folder to Overleaf (IEEEtran is built in)
```

Run twice (or use `latexmk`) so `bibtex` resolves the references.

## Files

- `main.tex` - the paper.
- `references.bib` - bibliography. **Every entry must be verified before
  submission** (see the warning at the top of the file).
- `figures/` - put screenshots / plots here.

## Status - what's drafted vs. to-do

Drafted now (stable): Introduction/Problem, System Architecture (the core),
Dataset structure, Results structure with the measured throughput/latency.

Search the source for these tags before submission:

- `% TODO(data-freeze)` - final numbers, filled ~1–2 days before the demo:
  - final event count + collection span (Abstract, §Dataset)
  - final ML RMSE/MAE/R² from `uv run python -m streamguard.ml evaluate`
    (Abstract, Table II)
  - re-run `uv run python -m streamguard.benchmark batch|streaming` and
    refresh Table I
- `% TODO(figure)` - screenshots/plots to add to `figures/`:
  - dashboard (live metrics + time series)
  - Spark UI → Structured Streaming tab (input/process rate)
  - predicted-vs-actual next-minute keystrokes (matplotlib)
  - optional: keystrokes-per-window histogram, feature-importance bar
- `% TODO(citations)` - Related Work needs 3–5 verified domain references
  (keystroke dynamics, fatigue/cognitive-load from input behavior, prior
  streaming productivity-monitoring systems).
- `% TODO(authors)` - group member names + institution in the title block.

## Citations / integrity

`references.bib` entries are recalled from memory and **must be verified**
against the real papers. Do not submit unverified citations.
