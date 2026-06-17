# JUMP Agent API

HTTP tool server for the processed JUMP production and JUMP Hub datasets.

The service is designed to run on the EC2 host with data mounted at `/srv/jump/data`.
It exposes bounded JSON functions for agents; it does not serve full matrices or raw images.

The beta instance currently stores two processed data layers:

- JUMP production datastore: `/srv/jump/data/jump_production`
- JUMP Hub/JUMPrr Zenodo files: `/srv/jump/data/jump_hub_zenodo`

These files are sufficient for most paper-style, mechanism, activity, metadata, and processed-output questions.
Raw images or single-cell feature files are still needed for new image-level analyses such as cell-cycle phase
calling from DNA distributions or subpopulation aggregation.

## Run

```bash
export JUMP_DATA_ROOT=/srv/jump/data
export JUMP_API_KEY=<token>
/srv/jump/venv/bin/uvicorn jump_agent_api.app:app --host 0.0.0.0 --port 8000
```

## EC2 Bootstrap

The production beta host uses:

- data root: `/srv/jump/data`
- app checkout: `/srv/jump/app/JumpAgent`
- service: `jump-agent-api`
- API key file: `/srv/jump/api_key`

Download the data:

```bash
mkdir -p /srv/jump/{data,logs,manifests,scripts}
scripts/download_all.sh
```

Deploy or update the API:

```bash
scripts/deploy_ec2.sh
```

Set up HTTPS for `jump-agent.net`:

```bash
JUMP_AGENT_DOMAIN=jump-agent.net scripts/setup_caddy.sh
```

DNS and AWS security group prerequisites:

- `A jump-agent.net -> 18.222.30.220`
- inbound `80/tcp` from `0.0.0.0/0` for Let's Encrypt HTTP validation
- inbound `443/tcp` from `0.0.0.0/0` for agents
- inbound `22/tcp` restricted to admin IPs
- no public inbound `8000/tcp`; FastAPI binds to `127.0.0.1:8000`

Check status:

```bash
scripts/status.sh
curl -H "X-API-Key: $(cat /srv/jump/api_key)" http://127.0.0.1:8000/health
scripts/smoke_test_api.py
```

## Agent Functions

For coding agents with repository access, start with [AGENTS.md](AGENTS.md). For agents that only have a URL, provide:

```text
https://jump-agent.net/jump-agent
```

That manifest points to the public MCP endpoint, OpenAPI schema, docs, auth scheme, example request, and available tools.

Claude users can add the public MCP endpoint directly, with no local bridge file:

```bash
claude mcp add --transport http jump https://jump-agent.net/mcp
```

- `GET /health`
- `GET /jump-agent`
- `POST /mcp`
- `GET /tools`
- `GET /datasets`
- `GET /schema/{database}`
- `GET /resolve?q=...`
- `POST /entities/summary`
- `POST /search/entities`
- `POST /activity`
- `POST /activity/summary`
- `POST /consistency`
- `POST /consistency/summary`
- `POST /activity/compare`
- `POST /consistency/compare`
- `POST /consistency/sweep`
- `POST /compare/configs`
- `POST /cross-source`
- `POST /chemical/properties`
- `POST /similarity/neighbors`
- `POST /similarity/pairwise`
- `POST /features/interpretable`
- `POST /gallery/images`
- `POST /activity-cliffs`
- `POST /profiles/rows`
- `GET /profiles/{dataset}/features`
- `POST /annotations`
- `POST /annotations/coverage`
- `POST /annotations/dark-matter`
- `POST /wells/cell-counts`
- `GET /workflows`
- `GET /workflows/{name}`
- `POST /workflows/neighborhood`
- `POST /workflows/compose`
- `POST /metadata/summary`
- `POST /structure/scaffolds`
- `POST /memos`
- `GET /memos`
- `GET /source/summary`
- `GET /artifacts/search`
- `GET /artifacts/read`
- `GET /provenance`

All REST endpoints except `GET /health`, `GET /jump-agent`, `GET /tools`, `GET /docs`, and `GET /openapi.json`
require an API key when `JUMP_API_KEY` is set. Agents can send either:

```text
Authorization: Bearer <key>
X-API-Key: <key>
```

`POST /mcp` is public and exposes a curated read-oriented MCP tool set. It intentionally does not expose the memo write/list tools.

The manifest at `/jump-agent` is the shortest bootstrap document to share with other agents. The OpenAPI spec is available at `/openapi.json`, so agents can discover request and response shapes directly.

## What Each Function Is For

- Activity calls and run comparisons: `/activity`, `/activity/summary`, `/activity/compare`, `/compare/configs`
- Cross-source reproducibility and source effects: `/cross-source`, `/source/summary`
- Target, MoA, pathway, and annotation consistency: `/consistency`, `/consistency/summary`, `/consistency/compare`, `/consistency/sweep`, `/annotations`
- Compound and gene resolution: `/resolve`, `/entities/summary`, `/search/entities`
- Chemical properties, scaffolds, and activity cliffs: `/chemical/properties`, `/structure/scaffolds`, `/activity-cliffs`
- Morphological nearest neighbors and small exact pairwise comparisons: `/similarity/neighbors`, `/similarity/pairwise`
- Interpretable CellProfiler feature signals and gallery image URLs: `/features/interpretable`, `/gallery/images`
- Profile feature subsets and bounded profile rows: `/profiles/{dataset}/features`, `/profiles/rows`
- Cell-count, plate, well, and source metadata: `/wells/cell-counts`, `/source/summary`
- Bounded metadata group-bys: `/metadata/summary`
- Annotation coverage and dark chemical matter discovery: `/annotations/coverage`, `/annotations/dark-matter`
- Composable workflow recipes and safe agent-defined pipelines: `/workflows`, `/workflows/{name}`, `/workflows/neighborhood`, `/workflows/compose`
- Agent capability-gap feedback: `/memos`
- Processed figures, summaries, CSVs, and Parquets from the analysis repo: `/artifacts/search`, `/artifacts/read`

`/workflows/compose` is the safe path for agent-created analyses. It accepts a small ordered list of allowlisted primitive calls and optional `ids_from` links that pass JCP2022 IDs from one step to the next. It intentionally does not execute arbitrary Python on the server; agents can inspect `/workflows`, `/workflows/{name}`, and `/openapi.json` to build new bounded pipelines from the exposed primitives.

When an agent needs a primitive that does not exist, it should submit a structured memo to `POST /memos` with the missing capability, relevant question, desired endpoint shape, and current workaround. Maintainers can review submitted memos with `GET /memos`.

## Example Calls

Against the public HTTPS service:

```bash
curl https://jump-agent.net/jump-agent

curl -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' \
  https://jump-agent.net/mcp

curl -H "Authorization: Bearer <api-key>" \
  https://jump-agent.net/datasets
```

Against the EC2-local service:

```bash
KEY="$(cat /srv/jump/api_key)"

curl -H "X-API-Key: $KEY" http://127.0.0.1:8000/datasets

curl -H "X-API-Key: $KEY" \
  "http://127.0.0.1:8000/resolve?q=JCP2022_085227"

curl -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"dataset":"compound_no_source7","preprocessing":"activity_no_target2","filter":"all_sources","activity_params":"default"}' \
  http://127.0.0.1:8000/activity/summary

curl -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"id":"JCP2022_085227","modality":"compound","top_k":5}' \
  http://127.0.0.1:8000/similarity/neighbors

curl -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"id":"JCP2022_085227","modality":"compound","top_k":5,"annotation_tables":["repurposing_hub_annotations","compound_source"]}' \
  http://127.0.0.1:8000/workflows/neighborhood

curl -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"steps":[{"name":"similarity_neighbors","save_as":"neighbors","params":{"id":"JCP2022_085227","modality":"compound","top_k":5}},{"name":"entities_summary","save_as":"neighbor_metadata","ids_from":{"step":"neighbors","field":"Match JCP2022"}}]}' \
  http://127.0.0.1:8000/workflows/compose

curl -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"title":"Need organelle feature subset primitive","memo":"An analysis agent needs to compare nuclear, ER, and mitochondrial feature subsets for the same perturbation set.","category":"workflow_request","priority":"normal","related_question":"Organelle-specific perturbations","suggested_endpoint":"/features/organelle-summary","tags":["features","organelle","workflow"]}' \
  http://127.0.0.1:8000/memos

curl -H "X-API-Key: $KEY" \
  "http://127.0.0.1:8000/artifacts/read?relative_path=chemical-space/chemical_space_summary.json"
```
