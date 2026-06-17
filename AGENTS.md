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

Claude-compatible public MCP endpoint:

```text
https://jump-agent.net/mcp
```

Claude setup:

```bash
claude mcp add --transport http jump https://jump-agent.net/mcp
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
/mcp
/tools
/docs
/openapi.json
```

All REST data endpoints require an API key. Prefer:

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

`POST /mcp` is public and exposes curated read-oriented tools. It does not expose memo write/list operations.

## Minimal Call Pattern

1. Fetch `https://jump-agent.net/jump-agent`.
2. For Claude, use the public MCP server at `https://jump-agent.net/mcp`.
3. For REST clients, fetch `https://jump-agent.net/openapi.json`.
4. Choose the endpoint whose description matches the question.
5. Send REST JSON requests with `Authorization: Bearer <api-key>`.
6. Keep requests bounded. Do not ask for full matrices or unbounded profile exports.

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
- Activity calls and run comparisons: `/activity`, `/activity/summary`, `/activity/compare`, `/compare/configs`
- Consistency and annotation signals: `/consistency`, `/consistency/summary`, `/consistency/compare`, `/consistency/sweep`, `/annotations`
- Cross-source and source summaries: `/cross-source`, `/source/summary`
- Chemical properties, scaffolds, and activity cliffs: `/chemical/properties`, `/structure/scaffolds`, `/activity-cliffs`
- Similarity search: `/similarity/neighbors`, `/similarity/pairwise`
- Interpretable feature and gallery records: `/features/interpretable`, `/gallery/images`
- Profile features and bounded row reads: `/profiles/{dataset}/features`, `/profiles/rows`
- Metadata summaries: `/metadata/summary`
- Annotation coverage and dark chemical matter: `/annotations/coverage`, `/annotations/dark-matter`
- Composable workflows: `/workflows`, `/workflows/{name}`, `/workflows/neighborhood`, `/workflows/compose`
- Capability-gap memos: `POST /memos`, `GET /memos`
- Well/cell-count data: `/wells/cell-counts`
- Processed analysis artifacts: `/artifacts/search`, `/artifacts/read`
- Provenance and inventory: `/provenance`, `/datasets`, `/schema/{database}`

## Composable Workflows

Use `/workflows` to discover built-in recipes and the allowlisted steps available to `/workflows/compose`.

`/workflows/compose` lets an agent submit a small declarative pipeline of existing primitives. A later step can set `ids_from` to pull JCP2022 IDs from a previous step, for example taking `"Match JCP2022"` values from `/similarity/neighbors` and feeding them into `/entities/summary`.

The server does not execute arbitrary submitted Python. To create a new primitive, inspect `/openapi.json`, compose existing steps first, then add a reviewed FastAPI endpoint in this repository with focused tests.

## Missing Primitives

If an analysis needs a primitive that is not exposed, submit a memo with `POST /memos`. Include the missing capability, the question it blocks, the endpoint shape you wish existed, and any workaround you tried. Memos are stored server-side as JSONL for maintainer review through `GET /memos`.

## Data Scope

The service is built on processed data, not raw JUMP images:

- JUMP production datastore under `/srv/jump/data/jump_production`
- JUMP Hub/JUMPrr Zenodo files under `/srv/jump/data/jump_hub_zenodo`

This is sufficient for most activity, target, MoA, similarity, metadata, source reproducibility, and processed-paper-output questions. It is not sufficient for new raw-image or single-cell image-processing analyses.
