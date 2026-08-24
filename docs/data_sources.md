# Authoritative public-data source register

Checked on 2026-08-24. The executable policy is [`config/sources.yml`](../config/sources.yml).
HTTP acquisition records retrieval time, resolved URL, response metadata, byte size and SHA-256.
Record count is captured when the source response exposes a bounded record set (for example
ArcGIS); parser/schema hashes and object URI are added by the subsequent landing/load manifest.

| Source | Authority and supported access | Licence decision | Default scope |
|---|---|---|---|
| [Vicmap Address](https://www.land.vic.gov.au/maps-and-spatial/spatial-data/vicmap-catalogue/vicmap-address) | Victorian Government; weekly ArcGIS REST/FeatureServer | CC BY 4.0; attribution and change notice required | Selected LGA/postcodes; raw data stays out of Git |
| [Vicmap Property](https://www.land.vic.gov.au/maps-and-spatial/spatial-data/vicmap-catalogue/vicmap-property) | Victorian Government; weekly ArcGIS REST/FeatureServer | CC BY 4.0 | Selected geography; simplified analytic geometry |
| [Victorian Property Sales Report](https://discover.data.vic.gov.au/dataset/victorian-property-sales-report-median-house-by-suburb-time-series) | Valuer-General Victoria aggregate XLS/XLSX reports | CC BY 4.0 | Suburb/property-type medians and volumes only; automated workbook download currently blocked by HTTP 403 |
| [ABS Data API](https://www.abs.gov.au/statistics/application-programming-interfaces-apis/data-api-user-guide) | ABS SDMX REST; keyless beta API with response limits | CC BY 4.0 for the selected Census products | Pinned C21 G01/G02/G37 v1.0.0 queries for Victorian SA2 only |
| [RBA statistical tables](https://www.rba.gov.au/statistics/tables/) | Official CSV/XLSX tables; no general supported public REST API assumed | RBA notice has series-specific conditions | RBA-authored F1 series only; preserve vintage |
| [ABN Lookup public bulk extract](https://abr.business.gov.au/Tools/BulkExtract) | Weekly ZIP/XML public subset | CC BY 3.0 Australia | Active non-individual entities; minimise fields |
| [City of Melbourne CLUE](https://www.melbourne.vic.gov.au/census-land-use-and-employment-clue) | Annual confidentialised aggregate CSV export | CC BY 4.0 | Regional context; suppressed cells remain suppressed |

## Hard exclusions

- No scraping of realestate.com.au, Domain, CoreLogic or other proprietary platforms.
- The technically visible Victorian VIPS transaction-level sale layer is disabled. No clear
  dataset-level licence/redistribution permission was verified, so technical accessibility is not
  treated as permission.
- The non-public agency ABR extract and non-confidentialised CLUE data are not used.
- Sole-trader names from ABN data are excluded from portfolio outputs; public business references
  imply no affiliation or endorsement.

## Acquired validation slice

On 2026-08-24 the local fetcher acquired and hashed the RBA F1 CSV, a City of Melbourne CLUE CSV,
one explicitly filtered 25-record Vicmap Address ArcGIS page for postcode 3000 and three ABS Census
2021 SDMX slices (G01 population, G02 median household income and G37 tenure) for Victorian SA2.
The ABS adapter joined 524 geographies; 523 are complete and one special-area row retains missing
household measures for quarantine rather than invention. Raw responses are ignored by Git. This
proves bounded HTTP acquisition and contract capture only, not GCS landing or BigQuery loading.
Exact hashes are in
[`evidence/public_source_validation.json`](../evidence/public_source_validation.json).

## Geography and revision controls

- Postcode/suburb text is not joined directly to SA2 by name. A versioned official correspondence
  is required, and allocation assumptions must remain visible.
- ABS, RBA and VGV observations retain both observation period and publication/retrieval vintage.
- CLUE suppression is a state, not zero or missing-at-random; transformations preserve it.
- Source terms are rechecked before a new source/version is enabled.
