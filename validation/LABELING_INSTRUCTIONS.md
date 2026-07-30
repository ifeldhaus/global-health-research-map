# Blind validation labeling — how to

**Open:** double-click `validation/label_tool.html` (opens in your browser; no server needed).
Use the **same browser + profile** each time — your progress autosaves to that browser and resumes where you left off.

## Per paper (all keyboard, no mouse needed)
1. Read the title + abstract on the left.
2. **Category** — press the letter `A`–`O` (or `Z` = none fits).
3. **Subtopic** — press `1`–`9` (only that category's subtopics show; you never scan all 143).
4. **Method** — press `;` then type (e.g. "review", "cross", "RCT"), `Enter` picks the top match.
5. **Country** — press `'` then type a country, `Enter` to add (multiple allowed). Or the `GLOBAL` / `UNKNOWN` buttons. Study country = where the research was done / the population, **not** author affiliation.
6. `Enter` or `→` moves to the next paper. `←` goes back. "Next unlabeled" skips ahead.

The model's answer is **never shown** — that's deliberate. Seeing it would bias your labels and void the kappa.

## When done (or partway)
Click **Export CSV** → downloads `validation_labels.csv`. Tell Claude / drop it in `validation/` and Claude will import it and run `02_kappa.py --from-db` to score agreement.

Progress bar at the top shows X / 200. You can export partway and get an interim kappa on what's labeled so far.

---

## Method disambiguation — locked conventions (rule A′)

The M04 / M16 / M12 boundary uses a **provenance gate, then an output test**. This is the single hardest call; apply it consistently.

1. **Did the authors collect the data themselves** (own fieldwork, survey, trial, specimens)?
   → classify by **design**: M01 RCT · M02 quasi-experimental · M03 cohort/longitudinal · **M04 cross-sectional survey** · M06 qualitative · M17 case study, etc. A survey the authors fielded is **M04**.
2. **Did they analyze an existing standardized dataset** they did not collect (DHS, SAGE, GBD, MICS, national/administrative survey, registry)?
   → **descriptive quantity** (prevalence, incidence, mortality, DALYs, burden, coverage, trend) → **M16**
   → **analytical relationship** (determinants, associations, risk factors, econometrics/financing, cross-national comparison) → **M12**

Shorthand: field a survey → **M04**; mine DHS for relationships → **M12**; use DHS to report prevalence/burden → **M16**. Longitudinal follow-up of the same individuals is **M03** regardless of provenance.

Other locked tie-breaks: systematic search / PROSPERO / PRISMA → **M05** (vs M14 narrative). Named implementation-science framework (RE-AIM, CFIR, NPT) → **M09** (vs M06). Own qualitative data (interviews/focus groups/ethnography) → **M06** even if framed as a "perspective" (vs M15 opinion with no data).

## Validation status (methods)

- Rule A′ locked in `pipeline/03_methods_classify.py` and `batches/methods_system.txt` (single source).
- Dev-set (199) methods **κ = 0.762** (n=197, excl. 2 corrupt records) / 0.754 (n=199). Baseline was 0.713.
- **Excluded from methods validation** — corrupt/mismatched abstracts (no correct text in any source): `W2510810333` (SDG-nutrition title, diabetes abstract attached), `W2515180663` (dengue Kenya, truncated at source).
- Residual disagreement is long-tail boundary ambiguity + small-n codes; accepted as substantial agreement. Sealed-150 certifies the honest out-of-sample number.
