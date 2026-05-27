# Agent Instructions

This repository exposes an OpenAPI/HTTP tool server for processed JUMP production and JUMP Hub data.

## Public Service

Use this bootstrap URL first:

```text
https://jump-agent.net/jump-agent
```

Then inspect the OpenAPI schema:

```text
https://jump-agent.net/openapi.json
```

Human-readable docs are at:

```text
https://jump-agent.net/docs
```

## Authentication

Public discovery endpoints do not require a key:

```text
/health
/jump-agent
/tools
/docs
/openapi.json
```

All data endpoints require an API key. Prefer:

```text
Authorization: Bearer <api-key>
```

The service also accepts:

```text
X-API-Key: <api-key>
```

Do not commit API keys to this repository. On the beta EC2 host, the shared key is stored at:

```text
/srv/jump/api_key
```

## Minimal Call Pattern

1. Fetch `https://jump-agent.net/jump-agent`.
2. Fetch `https://jump-agent.net/openapi.json`.
3. Choose the endpoint whose description matches the question.
4. Send JSON requests with `Authorization: Bearer <api-key>`.
5. Keep requests bounded. Do not ask for full matrices or unbounded profile exports.

Example:

```bash
curl -H "Authorization: Bearer <api-key>" \
  https://jump-agent.net/datasets
```

```bash
curl -H "Authorization: Bearer <api-key>" \
  -H "Content-Type: application/json" \
  -d '{"dataset":"compound_no_source7","preprocessing":"activity_no_target2","filter":"all_sources","activity_params":"default"}' \
  https://jump-agent.net/activity/summary
```

## Endpoint Map

- Entity lookup: `/resolve`, `/entities/summary`, `/search/entities`
- Activity calls: `/activity`, `/activity/summary`, `/compare/configs`
- Consistency and annotation signals: `/consistency`, `/consistency/summary`, `/annotations`
- Cross-source and source summaries: `/cross-source`, `/source/summary`
- Chemical properties and activity cliffs: `/chemical/properties`, `/activity-cliffs`
- Similarity search: `/similarity/neighbors`, `/similarity/pairwise`
- Interpretable feature and gallery records: `/features/interpretable`, `/gallery/images`
- Profile features and bounded row reads: `/profiles/{dataset}/features`, `/profiles/rows`
- Well/cell-count data: `/wells/cell-counts`
- Processed analysis artifacts: `/artifacts/search`, `/artifacts/read`
- Provenance and inventory: `/provenance`, `/datasets`, `/schema/{database}`

## Data Scope

The service is built on processed data, not raw JUMP images:

- JUMP production datastore under `/srv/jump/data/jump_production`
- JUMP Hub/JUMPrr Zenodo files under `/srv/jump/data/jump_hub_zenodo`

This is sufficient for most activity, target, MoA, similarity, metadata, source reproducibility, and processed-paper-output questions. It is not sufficient for new raw-image or single-cell image-processing analyses.
