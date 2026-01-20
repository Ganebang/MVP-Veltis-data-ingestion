# Data Dictionary - Veltis Project

This document defines the data schema tailored for the Veltis "Lobbying & Public Affairs" use case.

## 1. Etablissements (`etablissements.csv`)

The core registry of hospitals and clinics. Enriched for geographical targeting and sector analysis.

| Field | Type | Description | Business Value |
|-------|------|-------------|----------------|
| `vel_id` | UUID | Internal unique ID | Linking records across time and datasets |
| `finess_et` | String | Official FINESS number (9 chars) | Primary key for government data |
| `siret` | String | Business ID (14 chars) | Linking to financial data |
| `raison_sociale` | String | Organisation Name | Identification |
| **`departement`** | String | Department Code (e.g. "75", "01") | **Targeting**: Key for mapping to local MPs / ARS delegations |
| `code_postal` | String | Postal Code | Granular geographic analysis |
| `adresse_postale` | String | Full Address | Logistics |
| **`categorie_etab`** | String | Simplified Sector (Public, Privé, ESPIC) | **Strategy**: Private lobbying vs Public affairs require different approaches |
| **`categorie_detail`** | String | Official Category (e.g. "Centre Hospitalier", "Clinique") | **Context**: Detailed understanding of the facility type |
| `date_updated` | Timestamp | Last update time | Data freshness |

## 2. Qualifications (`qualifications.csv`)

Quality certification status from HAS. Critical for reputation management and comparative white papers.

| Field | Type | Description | Business Value |
|-------|------|-------------|----------------|
| `vel_id` | UUID | Link to Etablissement | |
| `niveau_certification` | String | Certification Level (e.g. "Haute Qualité") | **Benchmarking**: Key differentiator in white papers |
| `date_visite` | Date | Last certification visit | Timing relevant campaigns |
| `url_rapport` | URL | Link to PDF report | Deep dive analysis |
| `score_satisfaction` | Float | e-Satis Patient Score (0-100) | **Argumentation**: Proof of quality / pain points |

## 3. Health Metrics (`health_metrics.csv`)

Detailed clinical and process indicators (IQSS). The raw fuel for "Data-Driven Lobbying".

| Field | Type | Description | Business Value |
|-------|------|-------------|----------------|
| `vel_id` | UUID | Link to Etablissement | |
| `metric_id` | UUID | Unique Metric ID | |
| **Global Scores** | | | |
| `score_all_ssr_ajust` | Float | **Global Score** (Adjusted) | Top-level ranking metric |
| `score_ajust_esatis_region` | Float | Regional e-Satis comparison | Benchmarking against neighbors |
| **Process Scores** | | | |
| `score_accueil_ssr_ajust` | Float | **Reception** Score (Accueil) | Patient onboarding experience |
| `score_pec_ssr_ajust` | Float | **Care** Score (Prise en Charge) | Clinical quality perception |
| `score_lieu_ssr_ajust` | Float | **Facility** Score (Lieu de vie) | Infrastructure/Comfort quality |
| `score_repas_ssr_ajust` | Float | **Food** Score (Repas) | Catering service quality |
| `score_sortie_ssr_ajust` | Float | **Discharge** Score (Sortie) | Continuity of care efficiency |
| **Other** | | | |
| `classement` | String/Char | Rank/Class (A, B, C...) | Rapid visual comparison |
| `evolution` | String/Float | Trend vs Previous Year | **Monitoring**: Highlighting improvement or decline |
| `processed_at` | Timestamp | Processing date | Audit trail |

## Business Logic & Usage

### 1. Lobbying Campaigns
**Goal**: Engage with local decision makers.
- **Filter**: Use `departement` to isolate hospitals in a specific MP's constituency.
- **Segment**: Use `categorie_etab` to tailor the message (e.g., funding issues for Public vs regulation for Private).

### 2. White Paper Creation
**Goal**: Publish industry insights.
- **Compare**: Aggregate `score_satisfaction` or `niveau_certification` by `categorie_detail`.
- **Rank**: Identify top performers in a `region` (via `departement` grouping).

### 3. Monitoring
**Goal**: Watch Client Reputation.
- **Track**: Changes in `niveau_certification` or `score_*` year-over-year.
