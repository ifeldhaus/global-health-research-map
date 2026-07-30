# Methodology

Status: draft. Figures marked `[pending]` are filled in after the July 2026
corpus rebuild and validation round complete.

## Corpus

**Definition.** All works indexed by [OpenAlex](https://openalex.org) with a
`primary_location` in one of 11 core global health journals
(see [`data/journal_list.csv`](../data/journal_list.csv) for the list and
per-journal inclusion rationale), publication years 2010–2026.

**Snapshot.** The corpus is a point-in-time snapshot pulled in July 2026.
2026 is a partial publication year; per-year trend analyses exclude or flag
it. Citation counts and funder records reflect OpenAlex as of the snapshot
date.

**Why journal-based.** A journal-based boundary is reproducible and stable:
anyone re-running `pipeline/01_corpus_pull.py` against the same journal list
and date window retrieves the same corpus (modulo OpenAlex index updates).
A topic-based boundary (search terms, concept tags) is sensitive to query
choices and indexing changes, and makes the denominator of every trend claim
contestable. The cost is coverage: global health papers in general medical
journals (NEJM, Lancet, JAMA) are excluded. This is listed as a limitation
and a planned v2 extension.

**Abstracts.** OpenAlex stores abstracts as inverted indexes; these are
reconstructed to plain text at pull time. Of 33,964 works, 4,516 (13.3%) lack
a usable abstract and are tagged by `pipeline/tag_unclassifiable.py` —
`no_abstract` (2,814), `boilerplate_abstract` (1,592), and
`insufficient_abstract` (110) — then excluded from classification rather than
treated as missing at random. The remaining 29,448 works (86.7%) carry all
three classification labels. The dashboard's Data Completeness page reports
the breakdown.

## Classification

Three LLM classification passes over every work with a usable abstract
(title + abstract, abstract truncated to 300 words):

| Pass | Script | Output |
|---|---|---|
| Topic | `pipeline/02_topic_classify.py` | 15 categories (A–O), 143 subtopics + confidence |
| Methods | `pipeline/03_methods_classify.py` | 18 method types (M01–M18) + confidence |
| Study country | `pipeline/06_study_country.py` | ISO 3166-1 alpha-2 code(s), GLOBAL, or UNKNOWN + confidence |

Taxonomies live in [`data/taxonomy/`](../data/taxonomy/). Prompts are built
from the taxonomy CSVs at run time (single source of truth); the full prompt
text is embedded in each script. Labels are parsed defensively and validated
against the taxonomy — malformed or out-of-taxonomy responses coerce to the
uncategorized sentinels (`Z|Z00`, `M18`, `UNKNOWN`) rather than entering the
data silently.

**Model and route.** The July 2026 corpus build was classified entirely via
the batch route (route 2 below): unclassified works were exported in batches
of 100 and labelled by Claude Code subscription agents (Claude Opus 4.8,
Anthropic), whose output was imported through the same parsers and taxonomy
validation as the API route. The `claude-haiku-4-5` default noted below is the
unused API-route fallback, not the model used for this build.
All 88,344 labels (29,448 works × 3 passes) were produced this way. Two
execution routes exist, with identical prompts, parsing, and database writes:

1. **API route** — the numbered scripts call the Anthropic API directly
   (default `claude-haiku-4-5`; `CLASSIFIER_MODEL` overrides).
2. **Batch route** — `pipeline/batch_io.py` exports unclassified works as
   batch files for classification by Claude Code agents and imports the
   returned labels through the same parsers.

**Study country** is the country the research is about (study population /
data collection), not author affiliation; the prompt forbids inferring it
from affiliations. Multi-country studies keep up to 6 codes
(pipe-separated); beyond that, or for region-wide studies naming no specific
countries, the label is GLOBAL.

**Fallback-label rates.** Across the 29,448 classified works, the
uncategorized-sentinel share is low on all three passes: topic `Z`
(uncategorized) 5.2%, methods `M18` (undeterminable) 3.8%, study-country
`UNKNOWN` 4.6%. Study-country `GLOBAL` is 35.5%, reflecting the corpus's
heavy load of systematic reviews, multi-country studies, and global-policy
commentary rather than a labelling failure.

**Known data artifact — conference-abstract misalignment.** A subset of
records (notably CUGH and other conference-proceeding entries) carry an
OpenAlex `abstract` field that is scrambled or paste-shifted relative to the
title. Where the abstract clearly did not match the title, classification
fell back to the reliable title signal with lowered confidence. These records
are a small, journal-concentrated minority; the confidence field flags them
for anyone filtering on high-confidence labels only.

## Validation

A 200-paper validation sample, proportionally stratified by journal, is
drawn by `validation/01_sample.py`. The export is blind: the labeler file
contains no LLM labels (they are held in a separate key file). Hand labels
are compared to LLM labels by `validation/02_kappa.py`, which joins the
current database labels by `openalex_id` (`--from-db` re-scores existing
hand labels after any re-classification).

Thresholds (Cohen's kappa): topic category ≥ 0.75, topic subtopic ≥ 0.65,
methods ≥ 0.70. Below threshold → revise prompts, re-classify, re-validate
on a fresh sample. Results: `[pending]`.

Confidence calibration (does self-reported confidence track accuracy?) is
reported alongside kappa in `validation/VALIDATION_REPORT.md`.

## Gender inference

First and last author first names are gendered via
[genderize.io](https://genderize.io) (`pipeline/05_gender_infer.py`) with
two corrections from Santamaría & Mihaljević (2018): diacritics stripped,
first component of compound names used. Names below probability 0.6, single
initials, and un-gendered names are recorded as `unknown`. Inference is
probabilistic and binary, with lower accuracy for non-Western names —
results are reported at population level only, never per-person.

## Funders

OpenAlex funder records are stored per work (`grants` table). Works with
empty funder data are backfilled from the OpenAlex batch API
(`pipeline/04_funder_normalize.py`), then matched to a hand-curated
canonical funder list ([`data/funders_canonical.csv`](../data/funders_canonical.csv))
by OpenAlex funder ID, with alias fallback. Funder data completeness varies
by year (weaker pre-2015) and is analyzed as a variable, not assumed
complete.

## Disease burden (GBD)

IHME Global Burden of Disease 2023 results (DALYs and Deaths, global, all
ages, both sexes) are loaded by `pipeline/07_gbd_burden.py` and mapped to
topic categories via the hand-built
[`data/taxonomy/topic_burden_map.csv`](../data/taxonomy/topic_burden_map.csv).
Burden data ends at the GBD 2023 release regardless of the corpus window.
See [`gbd_burden_methodology.md`](gbd_burden_methodology.md) for design
decisions.

## Known limitations

See the README's Known Limitations section; the dashboard surfaces the same
caveats inline next to the affected charts.
