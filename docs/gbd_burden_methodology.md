# GBD Burden Data: Methodology and Design Decisions

## Data Source

Disease burden data comes from the **Institute for Health Metrics and
Evaluation (IHME) Global Burden of Disease (GBD) 2023 Results Tool**
(<https://vizhub.healthdata.org/gbd-results/>).

Three separate downloads are required (the IHME tool limits parameter
combinations per download):

| Download | Metric   | Purpose                                    |
|----------|----------|--------------------------------------------|
| 1        | Number   | Absolute DALY/death counts for share ratios |
| 2        | Percent  | Proportion of total burden                 |
| 3        | Rate     | Per-100,000 population (age-standardised)  |

Each download selects:

- **Measure**: DALYs (Disability-Adjusted Life Years) *and* Deaths
- **Cause**: All causes (all hierarchy levels included in the download)
- **Location**: Global
- **Age**: All ages
- **Sex**: Both
- **Year**: All available (1980-2023; analyses use 2010-2021+)

Raw CSVs are saved to `data/gbd/` (gitignored) and processed by
`pipeline/07_gbd_burden.py`.

## Why GBD Level 2 Causes?

The GBD cause hierarchy has four levels:

| Level | Example                         | Count  |
|-------|---------------------------------|--------|
| 1     | Non-communicable diseases       | 3      |
| 2     | Cardiovascular diseases         | ~22    |
| 3     | Ischemic heart disease          | ~170   |
| 4+    | Acute myocardial infarction     | ~370+  |

We map our topic taxonomy to **Level 2 causes** for three reasons:

1. **Granularity match.** The project's 15 topic categories (A-O) are
   thematic groupings that correspond naturally to Level 2 cause clusters.
   For example, Topic F (Non-Communicable Diseases) maps to 10 Level 2
   causes (Cardiovascular diseases, Neoplasms, Chronic respiratory
   diseases, etc.), while Topic G (Mental Health & Substance Use) maps to
   2 Level 2 causes (Mental disorders, Substance use disorders).

2. **Avoids double-counting.** Level 2 causes are mutually exclusive and
   exhaustive within each Level 1 category. Summing Level 2 DALYs
   produces the Level 1 total without overlap. Using Level 3+ would
   require careful hierarchy management to avoid counting a cause under
   both its specific entry and its parent group.

3. **Interpretability.** Level 2 labels are broad enough to be meaningful
   ("Cardiovascular diseases" rather than "Hypertensive heart disease")
   but specific enough to reveal mismatches between research attention
   and disease burden.

### Topics Without a Burden Mapping

Seven topic categories have no GBD burden equivalent and are excluded
from research-intensity-ratio calculations:

| Category | Topic Name                        | Reason                                      |
|----------|-----------------------------------|----------------------------------------------|
| B        | Child & Adolescent Health         | Cross-cutting population group, not a cause  |
| I        | Environmental & Occupational Health | Risk factor in GBD, not a cause             |
| J        | Health Systems & Policy           | Systems-level, no disease burden             |
| K        | Health Economics & Financing      | Systems-level, no disease burden             |
| L        | Epidemiology & Surveillance       | Methodological, no disease burden            |
| N        | Sexual & Gender-Based Violence    | Risk factor / social determinant             |
| O        | Other / Cross-cutting             | Residual category                            |

### Notable Mapping Decisions

| Topic | GBD Level 2 Cause                    | Decision                                                |
|-------|--------------------------------------|---------------------------------------------------------|
| A     | Maternal and neonatal disorders      | GBD combines maternal + neonatal at Level 2; splitting requires Level 3 data. Assigned entirely to Topic A as the best available proxy. |
| D     | HIV/AIDS and sexually transmitted infections | GBD bundles all STIs with HIV; assigned to Topic D (HIV/AIDS/TB/Malaria). |
| D     | Respiratory infections and tuberculosis | Includes lower respiratory infections alongside TB; assigned to Topic D because TB is the dominant burden driver in the global health literature context. |
| E     | Neglected tropical diseases and malaria | GBD groups NTDs with malaria at Level 2; assigned to Topic E rather than splitting with Topic D. |

The full mapping is recorded in `data/taxonomy/topic_burden_map.csv`.

## Measures: DALYs and Deaths

Both DALYs and Deaths are stored. The primary analysis uses DALYs for
the research intensity ratio (publication share / DALY share), but Deaths
serve as a **sensitivity measure** because:

- **DALYs carry methodological baggage.** The disability weights used to
  compute Years Lived with Disability (YLD) are derived from population
  surveys and reflect value judgements about the relative severity of
  health states. These weights have been debated extensively in the
  literature.

- **Deaths are more straightforward.** Mortality counts are less
  susceptible to methodological assumptions, making them a useful
  robustness check.

- **Results may diverge.** Conditions with high disability but low
  mortality (e.g., mental disorders, musculoskeletal disorders) will
  appear more "neglected" under a DALY-based ratio than a death-based
  ratio. Conversely, conditions with high case fatality but lower
  disability burden may look more neglected under deaths. Comparing the
  two reveals whether research allocation patterns are robust to the
  choice of burden metric.

## Database Schema

The `gbd_burden` table stores all downloaded data:

```sql
CREATE TABLE gbd_burden (
    cause       VARCHAR,    -- GBD cause name (all hierarchy levels)
    region      VARCHAR,    -- 'Global' (expandable to country-level)
    year        INTEGER,    -- Calendar year
    measure     VARCHAR,    -- 'DALYs' or 'Deaths'
    metric      VARCHAR,    -- 'Number', 'Percent', or 'Rate'
    sex         VARCHAR,    -- 'Both' (expandable)
    age_group   VARCHAR,    -- 'All ages' (expandable)
    val         DOUBLE,     -- Point estimate
    upper       DOUBLE,     -- Upper uncertainty bound
    lower       DOUBLE,     -- Lower uncertainty bound
    PRIMARY KEY (cause, region, year, measure, metric, sex, age_group)
)
```

The `topic_burden_map` table links topic categories to GBD causes:

```sql
CREATE TABLE topic_burden_map (
    topic_category  VARCHAR,  -- A-O
    topic_name      VARCHAR,
    gbd_cause       VARCHAR,  -- Must match cause in gbd_burden
    notes           VARCHAR,
    PRIMARY KEY (topic_category, gbd_cause)
)
```

## Lens C Usage

The research intensity ratio in Lens C is computed as:

    Research Intensity = (topic publication share) / (topic burden share)

Where burden share is derived by joining `topic_burden_map` to
`gbd_burden`, filtering on the desired measure and metric:

```sql
-- DALYs-based burden share (primary)
WHERE g.measure = 'DALYs' AND g.metric = 'Number'

-- Deaths-based burden share (sensitivity)
WHERE g.measure = 'Deaths' AND g.metric = 'Number'
```

A ratio > 1 indicates a topic receives disproportionately *more*
research attention than its burden share; < 1 indicates relative
*neglect*.
