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
```

## Agent Functions

- `GET /health`
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
- `POST /compare/configs`
- `POST /cross-source`
- `POST /chemical/properties`
- `POST /similarity/neighbors`
- `POST /similarity/pairwise`
- `POST /features/interpretable`
- `POST /gallery/images`
- `POST /activity-cliffs`
- `POST /annotations`
- `POST /wells/cell-counts`
- `GET /source/summary`
- `GET /artifacts/search`
- `GET /artifacts/read`
- `GET /provenance`

All endpoints except `GET /health`, `GET /tools`, `GET /docs`, and `GET /openapi.json`
require an API key when `JUMP_API_KEY` is set. Agents can send either:

```text
Authorization: Bearer <key>
X-API-Key: <key>
```

The OpenAPI spec is available at `/openapi.json`, so agents can discover request and response shapes directly.

## What Each Function Is For

- Activity calls: `/activity`, `/activity/summary`, `/compare/configs`
- Cross-source reproducibility and source effects: `/cross-source`, `/source/summary`
- Target, MoA, pathway, and annotation consistency: `/consistency`, `/consistency/summary`, `/annotations`
- Compound and gene resolution: `/resolve`, `/entities/summary`, `/search/entities`
- Chemical properties and scaffold-style questions: `/chemical/properties`, `/activity-cliffs`
- Morphological nearest neighbors and small exact pairwise comparisons: `/similarity/neighbors`, `/similarity/pairwise`
- Interpretable CellProfiler feature signals and gallery image URLs: `/features/interpretable`, `/gallery/images`
- Profile feature subsets and bounded profile rows: `/profiles/{dataset}/features`, `/profiles/rows`
- Cell-count, plate, well, and source metadata: `/wells/cell-counts`, `/source/summary`
- Processed figures, summaries, CSVs, and Parquets from the analysis repo: `/artifacts/search`, `/artifacts/read`

## Example Calls

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

curl -H "X-API-Key: $KEY" \
  "http://127.0.0.1:8000/artifacts/read?relative_path=chemical-space/chemical_space_summary.json"
```
