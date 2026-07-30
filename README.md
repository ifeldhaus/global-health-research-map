# Power and Priority in Global Health Research

**Who leads it, who funds it, and what it studies, across 11 global health journals, 2010–2026.**

A computational analysis of 33,964 papers (29,448 with classifiable abstracts) published in 11 global health journals from 2010 through July 2026, examining who funds and produces the research, whose leadership it reflects, what it studies relative to disease burden, and which methods it uses.

**Isabelle Feldhaus — 2026**

📊 [Live dashboard](https://global-health-research-map.streamlit.app) · 📄 Paper — *forthcoming* · 🗂 Preprint — *forthcoming*

---

## What this project is

Global health research is not a neutral scientific enterprise. It is shaped by funding priorities, institutional power, and methodological conventions that determine which questions get asked, who asks them, and how. This project maps that landscape, systematically, quantitatively, and longitudinally, across five research lenses:

- **Funder Power:** Does funder concentration determine what gets studied, and is it increasing?
- **Geographic Power:** Is local research leadership genuinely growing, or structural and persistent?
- **Topic Trends:** Has research followed the NCD burden shift, and what did COVID permanently displace?
- **Methods Gaps:** Where are the highest-value methodological transfer opportunities?
- **Institutions:** Who produces the research, how concentrated is production, and is leadership shifting toward the Global South?

---

## Quickstart

```bash
# Clone and install
git clone https://github.com/ifeldhaus/global-health-research-map
cd global-health-research-map
uv sync

# Set up environment
cp .env.example .env
# Add your ANTHROPIC_API_KEY and OPENALEX_EMAIL to .env

# Initialize database
uv run python pipeline/00_setup_db.py

# Run corpus pull (~29,000 works; resumable if interrupted)
caffeinate -i uv run python pipeline/01_corpus_pull.py

# Classify (each script is resumable; --test for 100 records, --mock for no-API dry runs)
caffeinate -i uv run python pipeline/02_topic_classify.py
caffeinate -i uv run python pipeline/03_methods_classify.py
caffeinate -i uv run python pipeline/06_study_country.py

# Optional: use a stronger classification model for a given run
CLASSIFIER_MODEL=claude-sonnet-5 uv run python pipeline/02_topic_classify.py
```

Classification can also run without an API key through Claude Code
(subscription-billed) via `pipeline/batch_io.py`, which exports unclassified
works as batch files and imports the returned labels. Both routes use the
same prompts, label parsing, and database writes — see the module docstring.

---

## Dashboard

An interactive Streamlit dashboard (Overview, four research lenses, Institutions,
and a Data Completeness page) reads the classified corpus and the materialized
analysis tables.

```bash
uv run streamlit run dashboard/app.py
```

**Live version:** [global-health-research-map.streamlit.app](https://global-health-research-map.streamlit.app),
deployed on Streamlit Community Cloud from this repo. The public app reads a
~22 MB slim DuckDB build (`data/global_health_slim.duckdb`); `dashboard/db.py`
uses the full ~1.9 GB database when it is present locally and falls back to the
slim build otherwise. `requirements.txt` pins the runtime dependencies, and
`deploy/README.md` documents the deploy and how to rebuild the slim database.

---

## Validation

Classification was validated against **345 blind hand-labeled papers** (the model's
label was never shown to the labeler). Cohen's κ against the shipped labels:

| Axis | Cohen's κ | Landis–Koch |
|---|---|---|
| Topic — primary category | 0.66 | substantial |
| Methods | 0.67 | substantial |
| Study country | 0.93 | almost perfect |

Topic also carries an optional **secondary category** (a co-equal second focus,
~21% of papers) as a descriptive enrichment layer. Reproduce the numbers with
`uv run python validation/compute_kappa.py`; full detail in
[`validation/VALIDATION_REPORT.md`](validation/VALIDATION_REPORT.md).

---

## Repository structure

```
pipeline/        # Numbered scripts: corpus pull → classification → enrichment
dashboard/       # Streamlit app (7 pages: overview, four lenses, institutions, data completeness)
notebooks/       # Analysis notebooks, one per research lens + institutions
data/
  taxonomy/      # Topic and methods taxonomy CSVs
  gbd/           # IHME Global Burden of Disease data (downloaded, not versioned)
validation/      # Hand-labeled samples and kappa calculations
docs/            # Methodology notes, known limitations
```

---

## Corpus definition

All papers published in the following journals, 2010 through July 2026 (2026 is a partial year; snapshot pulled July 2026), retrieved via the [OpenAlex API](https://openalex.org):

| Journal | ISSN | Coverage |
|---|---|---|
| Lancet Global Health | 2214-109X | 2013–2026 |
| BMJ Global Health | 2059-7908 | 2016–2026 |
| Global Health Science and Practice | 2169-575X | 2013–2026 |
| Globalization and Health | 1744-8603 | 2010–2026 |
| Bulletin of the World Health Organization | 0042-9686 | 2010–2026 |
| Tropical Medicine & International Health | 1360-2276 | 2010–2026 |
| Health Policy and Planning | 0268-1080 | 2010–2026 |
| Journal of Global Health | 2047-2978 | 2011–2026 |
| Global Public Health | 1744-1692 | 2010–2026 |
| Annals of Global Health | 2214-9996 | 2014–2026 |
| PLOS Global Public Health | 2767-3375 | 2021–2026 |

A journal-based corpus was chosen over a topic-based approach for reproducibility and consistency. Inclusion rationale per journal is in [`data/journal_list.csv`](data/journal_list.csv).

---

## Topic taxonomy

15 primary categories (A–O), 143 subtopics. Full taxonomy: [`data/taxonomy/topic_taxonomy.csv`](data/taxonomy/topic_taxonomy.csv)

Categories: Maternal & Reproductive Health · Child & Adolescent Health · Infectious Disease · HIV/AIDS/TB/Malaria · Neglected Tropical Diseases · NCDs · Mental Health · Nutrition · Health Systems · Health Economics · Climate & Environment · Conflict & Humanitarian · Surgical & Emergency Care · Epidemiology & Burden · Research Methods

---

## Known limitations

- **Corpus boundary:** Journal-based approach excludes global health papers in general medical journals (NEJM, Lancet, JAMA). Supplementary topic-based analysis planned for v2.
- **Gender inference:** Probabilistic, binary, lower accuracy for non-Western names. Reported at population level with confidence thresholds.
- **LLM classification:** Validated on a 345-paper blind hand-labeled sample (topic κ 0.66, methods κ 0.67, country κ 0.93). See [Validation](#validation).
- **Funder data completeness:** OpenAlex funder data is missing for a significant proportion of papers, particularly pre-2015. Missingness is analyzed as a variable.
- **Partial final year:** The corpus includes papers through the July 2026 snapshot date. Per-year trend analyses exclude or flag 2026, which is incomplete.
- **Causal claims:** This is an observational bibliometric study. Associations do not establish causation.

---

## How to cite

> Feldhaus, I. (2026). *Power and Priority in Global Health Research.* Belle Labs. GitHub: github.com/ifeldhaus/global-health-research-map

---

## License

[CC-BY 4.0](LICENSE) — code, data, and reports are all released under Creative Commons Attribution 4.0. Reuse requires attribution.
