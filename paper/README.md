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

Drafted now (stable): Introduction/Problem, Related Work, System Architecture
(the core, incl. the liveness model), Dataset, and Results (throughput/latency
plus the liveness classification metrics). The earlier fatigue index and
next-minute forecaster have been removed in favor of the liveness classifier.

Search the source for these tags before submission:

- `% TODO(data-freeze)` - final numbers, filled ~1-2 days before the demo:
  - final event count + collection span (Abstract, §Dataset)
  - final liveness accuracy/precision/recall/F1/ROC-AUC from
    `uv run python -m keyspark.ml evaluate` (Abstract, Table II)
  - re-run `uv run python -m keyspark.benchmark batch|streaming` and
    refresh Table I
- `% TODO(figure)` - screenshots/plots to add to `figures/`:
  - dashboard (live metrics + time series), incl. the history calendar with
    a flagged (red) automation day, and the ML metrics card
  - Spark UI -> Structured Streaming tab (input/process rate)
  - optional: keystrokes-per-window histogram, liveness feature-importance bar
- `% TODO(citations)` - DONE: verified automation-detection references added
  (BeCAPTCHA-Mouse, Human/Bot/Cyborg, Battle of Botcraft) alongside the
  keystroke-dynamics and streaming-systems cites. Spot-check all DOIs live
  before submission.
- `% TODO(authors)` - author + institution are set (Murat Emirhan Aykut,
  Bahcesehir University); only the optional contact email is still tagged
  in the title block.

## Citations / integrity

`references.bib` entries are recalled from memory and **must be verified**
against the real papers. Do not submit unverified citations.
