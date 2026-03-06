# Mapping Global Health Research

**Power, Funding, and the Architecture of Scientific Knowledge**

A computational analysis of ~100,000 papers published in core global health journals from 2010–2024, examining funder concentration, geographic equity, topic trends, and methodological gaps.

**Isabelle Feldhaus — Belle Labs / Femme Fortified — 2026**

📊 [Dashboard](#) · 📄 [Report](#) · 🗂 [Preprint](#)

---

## What this project is

Global health research is not a neutral scientific enterprise. It is shaped by funding priorities, institutional power, and methodological conventions that determine which questions get asked, who asks them, and how. This project maps that landscape — systematically, quantitatively, and longitudinally — across four research lenses:

- **Lens A — Funder Power:** Does funder concentration determine what gets studied, and is it increasing?
- **Lens B — Geographic Power:** Is local research leadership genuinely growing, or structural and persistent?
- **Lens C — Topic Trends:** Has research followed the NCD burden shift, and what did COVID permanently displace?
- **Lens D — Methods Gaps:** Where are the highest-value methodological transfer opportunities?

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

# Run corpus pull (overnight — 6–12 hrs)
caffeinate -i uv run python pipeline/01_corpus_pull.py
```

---

## Repository structure

```
pipeline/        # Numbered scripts: corpus pull → classification → enrichment
analysis/        # Jupyter notebooks, one per research lens
dashboard/       # Streamlit app
data/
  taxonomy/      # Topic and methods taxonomy CSVs
  gbd/           # WHO Global Burden of Disease data
notebooks/       # Exploratory notebooks
validation/      # Hand-labeled samples and kappa calculations
prompts/         # LLM classification prompts, versioned
docs/            # Methodology notes, known limitations
```

---

## Corpus definition

All papers published in the following journals, 2010–2024, retrieved via the [OpenAlex API](https://openalex.org):

| Journal | ISSN |
|---|---|
| Lancet Global Health | 2214-109X |
| BMJ Global Health | 2059-7908 |
| PLOS Medicine | 1549-1277 |
| Global Health Science and Practice | 2169-575X |
| Globalization and Health | 1744-8603 |
| International Journal of Epidemiology | 0300-5771 |
| Bulletin of the World Health Organization | 0042-9686 |
| Tropical Medicine & International Health | 1360-2276 |
| American Journal of Tropical Medicine and Hygiene | 0002-9637 |
| Health Policy and Planning | 0268-1080 |

A journal-based corpus was chosen over a topic-based approach for reproducibility and consistency. See `docs/methodology.md` for rationale.

---

## Topic taxonomy

15 primary categories (A–O), 143 subtopics. Full taxonomy: [`data/taxonomy/topic_taxonomy.csv`](data/taxonomy/topic_taxonomy.csv)

Categories: Maternal & Reproductive Health · Child & Adolescent Health · Infectious Disease · HIV/AIDS/TB/Malaria · Neglected Tropical Diseases · NCDs · Mental Health · Nutrition · Health Systems · Health Economics · Climate & Environment · Conflict & Humanitarian · Surgical & Emergency Care · Epidemiology & Burden · Research Methods

---

## Known limitations

- **Corpus boundary:** Journal-based approach excludes global health papers in general medical journals (NEJM, Lancet, JAMA). Supplementary topic-based analysis planned for v2.
- **Gender inference:** Probabilistic, binary, lower accuracy for non-Western names. Reported at population level with confidence thresholds.
- **LLM classification:** Validated on 200-paper hand-labeled sample. Cohen's kappa reported in methods.
- **Funder data completeness:** OpenAlex funder data is missing for a significant proportion of papers, particularly pre-2015. Missingness is analyzed as a variable.
- **Causal claims:** This is an observational bibliometric study. Associations do not establish causation.

---

## How to cite

> Feldhaus, I. (2026). *Mapping Global Health Research: Power, Funding, and the Architecture of Scientific Knowledge.* Belle Labs. GitHub: github.com/ifeldhaus/global-health-research-map

---

## License

[CC-BY 4.0](LICENSE) — code, data, and reports are all released under Creative Commons Attribution 4.0. Reuse requires attribution.
