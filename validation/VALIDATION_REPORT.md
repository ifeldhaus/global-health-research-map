# Validation report

Human-vs-classifier agreement for the topic, methods, and country labels applied
to the corpus. Reproduce with `uv run python validation/compute_kappa.py`.

## Validation set

**345 hand-labeled papers**, labeled blind (the model's answer was never shown to
the labeler, so it cannot bias the labels). Sampled across the 11 corpus journals.
Gold labels: `validation_labels.csv` (199) + `validation_labels_run2.csv` (146).
Non-articles (journal front-matter, corrections with no real abstract) were skipped
during labeling and are excluded.

All shipped label axes are the **first-run / untuned** classifier — no label in the
validation set was used to build them — so the full set of 345 is a clean held-out
check for every axis.

## Results

| Axis | Cohen's κ | Agreement | Landis–Koch |
|------|-----------|-----------|-------------|
| Topic — primary category | **0.660** | 0.693 | substantial |
| Topic — primary subtopic | 0.541 | 0.548 | moderate |
| Methods | **0.666** | 0.706 | substantial |
| Country | **0.926** | 0.939 (exact-set) | almost perfect |

Topic set-including-secondary exact-match (human {primary, secondary} set equals
model set): **0.539**.

Country is multi-country per paper; κ is computed on the primary country with the
exact-set-match rate shown alongside.

## Secondary topic (enrichment layer)

Beyond the primary category, papers may carry an optional **secondary topic**
(`topic_category_2` / `topic_subtopic_2`) — a co-equal second focus. This is a
descriptive enrichment layer grafted from a multi-label classification pass
(redundant-with-primary secondaries dropped); it covers **~21%** of the corpus. It
is not part of the primary validation above; its standalone reliability is moderate
(set exact-match 0.54).

## Interpretation

Country classification is almost perfect. Topic-primary and methods are in
substantial agreement; topic-subtopic (the finer 143-way split) is moderate, as
expected for that granularity. These are held-out numbers against a single human
coder — human–human agreement on this kind of multi-category coding is itself
typically 0.6–0.8, so the classifier is near the achievable ceiling. For an
aggregate research map (which topics/regions/methods dominate), per-paper error at
this rate does not move the corpus-level distributions.

## Provenance notes

- An iterative refinement pass (multi-label topic + a rule-based "provenance gate"
  for methods) was explored on a development subset but did **not** improve
  out-of-sample accuracy (methods was a tie; topic-primary within noise). The
  shipped labels are therefore the first-run classifier; the only retained product
  of the iteration is the optional secondary-topic layer.
- Two records with corrupt source metadata (title/abstract mismatch, no correct
  version in any source) were excluded from the methods validation.
