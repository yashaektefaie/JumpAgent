from __future__ import annotations

import math
import os
import re
import json
from pathlib import Path
from typing import Any, Literal, Optional

import duckdb
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field


DATA_ROOT = Path(os.environ.get("JUMP_DATA_ROOT", "/srv/jump/data"))
PRODUCTION_ROOT = DATA_ROOT / "jump_production"
ZENODO_ROOT = DATA_ROOT / "jump_hub_zenodo"
METADATA_DB = PRODUCTION_ROOT / "interim" / "jump_metadata_augmented.duckdb"
COPAIRS_DB = PRODUCTION_ROOT / "processed" / "copairs_results.duckdb"
PROFILES_DIR = PRODUCTION_ROOT / "profiles"
PROCESSED_DIR = PRODUCTION_ROOT / "processed"

MAX_LIMIT = 5000
DEFAULT_ACTIVITY_DATASET = "compound_no_source7"
DEFAULT_ACTIVITY_PREPROCESSING = "activity_no_target2"
DEFAULT_FILTER = "all_sources"
DEFAULT_ACTIVITY_PARAMS = "default"
DEFAULT_CONSISTENCY_PREPROCESSING = "consistency_no_target2"
DEFAULT_DISTANCE = "cosine"

API_KEY = os.environ.get("JUMP_API_KEY", "")
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

app = FastAPI(
    title="JUMP Agent API",
    version="0.1.0",
    description=(
        "Bounded query functions over processed JUMP production and JUMP Hub data. "
        "Designed for agents; avoids returning full matrices."
    ),
)

_MATRIX_COLUMNS_CACHE: dict[str, list[str]] = {}


def require_api_key(
    api_key: Optional[str] = Depends(API_KEY_HEADER),
    authorization: Optional[str] = Header(default=None),
) -> None:
    bearer_token = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer_token = authorization.split(" ", 1)[1].strip()
    if API_KEY and api_key != API_KEY and bearer_token != API_KEY:
        raise HTTPException(status_code=401, detail="Missing or invalid API key")


def clamp_limit(limit: Optional[int], default: int = 100) -> int:
    value = default if limit is None else limit
    return max(1, min(value, MAX_LIMIT))


def human_size(size: int) -> str:
    value = float(size)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{size} B"


def file_info(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    stat = path.stat()
    return {"path": str(path), "exists": True, "size": stat.st_size, "human_size": human_size(stat.st_size)}


def clean_float(value: Any) -> Any:
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def rows_to_dicts(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    columns = [desc[0] for desc in cursor.description]
    rows = []
    for row in cursor.fetchall():
        rows.append({column: clean_float(value) for column, value in zip(columns, row)})
    return rows


def query_db(path: Path, sql: str, params: Optional[list[Any]] = None) -> list[dict[str, Any]]:
    if not path.exists():
        raise HTTPException(status_code=503, detail=f"Database not found: {path}")
    con = duckdb.connect(str(path), read_only=True)
    try:
        return rows_to_dicts(con.execute(sql, params or []))
    finally:
        con.close()


def query_metadata_with_copairs(sql: str, params: Optional[list[Any]] = None) -> list[dict[str, Any]]:
    if not METADATA_DB.exists() or not COPAIRS_DB.exists():
        raise HTTPException(status_code=503, detail="Required DuckDB files are missing")
    con = duckdb.connect(str(METADATA_DB), read_only=True)
    try:
        con.execute(f"ATTACH {quote_literal(str(COPAIRS_DB))} AS copairs (READ_ONLY)")
        return rows_to_dicts(con.execute(sql, params or []))
    finally:
        con.close()


def scalar_db(path: Path, sql: str, params: Optional[list[Any]] = None) -> Any:
    if not path.exists():
        raise HTTPException(status_code=503, detail=f"Database not found: {path}")
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(sql, params or []).fetchone()[0]
    finally:
        con.close()


def quote_ident(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_ .()|,'+:/%-]+", identifier):
        raise HTTPException(status_code=400, detail=f"Unsafe identifier: {identifier}")
    return '"' + identifier.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def modality_file(modality: str, suffix: str = "") -> Path:
    allowed = {"compound", "crispr", "orf"}
    if modality not in allowed:
        raise HTTPException(status_code=400, detail=f"modality must be one of {sorted(allowed)}")
    filename = f"{modality}{suffix}.parquet"
    path = ZENODO_ROOT / filename
    if not path.exists():
        raise HTTPException(status_code=503, detail=f"Missing Zenodo file: {path}")
    return path


def matrix_file(modality: str) -> Path:
    return modality_file(modality, "_cosinesim_full")


def profile_file(dataset: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9_]+", dataset):
        raise HTTPException(status_code=400, detail="Invalid dataset name")
    path = PROFILES_DIR / f"{dataset}.parquet"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Profile dataset not found: {dataset}")
    return path


def processed_artifact_path(relative_path: str) -> Path:
    root = PROCESSED_DIR.resolve()
    path = (PROCESSED_DIR / relative_path).resolve()
    try:
        path.relative_to(root)
    except ValueError:
        raise HTTPException(status_code=400, detail="relative_path must stay inside the processed data directory")
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail=f"Artifact not found: {relative_path}")
    return path


def matrix_columns(modality: str) -> list[str]:
    if modality in _MATRIX_COLUMNS_CACHE:
        return _MATRIX_COLUMNS_CACHE[modality]
    path = matrix_file(modality)
    con = duckdb.connect()
    try:
        rows = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    finally:
        con.close()
    columns = [row[0] for row in rows]
    _MATRIX_COLUMNS_CACHE[modality] = columns
    return columns


class RunConfig(BaseModel):
    dataset: str = DEFAULT_ACTIVITY_DATASET
    preprocessing: str = DEFAULT_ACTIVITY_PREPROCESSING
    filter: str = DEFAULT_FILTER
    activity_params: str = DEFAULT_ACTIVITY_PARAMS


class ConsistencyConfig(BaseModel):
    dataset: str = DEFAULT_ACTIVITY_DATASET
    preprocessing: str = DEFAULT_CONSISTENCY_PREPROCESSING
    filter: str = DEFAULT_FILTER
    group_type: str = "repurposing"
    distance: str = DEFAULT_DISTANCE


class IdsRequest(BaseModel):
    ids: list[str] = Field(default_factory=list, max_length=5000)
    include_activity: bool = True
    config: RunConfig = Field(default_factory=RunConfig)


class SearchRequest(BaseModel):
    query: Optional[str] = None
    targets: list[str] = Field(default_factory=list)
    moa: list[str] = Field(default_factory=list)
    disease_area: list[str] = Field(default_factory=list)
    source: list[str] = Field(default_factory=list)
    active: Optional[bool] = None
    has_pains: Optional[bool] = None
    valid_mol: Optional[bool] = None
    mw_min: Optional[float] = None
    mw_max: Optional[float] = None
    logp_min: Optional[float] = None
    logp_max: Optional[float] = None
    limit: int = 100
    offset: int = 0
    config: RunConfig = Field(default_factory=RunConfig)


class ActivityRequest(BaseModel):
    ids: list[str] = Field(default_factory=list, max_length=5000)
    config: RunConfig = Field(default_factory=RunConfig)
    limit: int = 100
    offset: int = 0


class ConsistencyRequest(BaseModel):
    config: ConsistencyConfig = Field(default_factory=ConsistencyConfig)
    group_value: Optional[str] = None
    significant_only: bool = False
    limit: int = 100
    offset: int = 0


class CompareConfigsRequest(BaseModel):
    configs: list[RunConfig] = Field(default_factory=list, max_length=20)


class CrossSourceRequest(BaseModel):
    dataset: str = DEFAULT_ACTIVITY_DATASET
    preprocessing: str = "activity_only_target2"
    filter: str = DEFAULT_FILTER
    ids: list[str] = Field(default_factory=list, max_length=5000)
    limit: int = 100
    offset: int = 0


class ChemicalPropertiesRequest(BaseModel):
    ids: list[str] = Field(default_factory=list, max_length=5000)
    limit: int = 100
    offset: int = 0


class NeighborsRequest(BaseModel):
    id: str
    modality: Literal["compound", "crispr", "orf"] = "compound"
    top_k: int = 20
    include_reverse: bool = False


class PairwiseRequest(BaseModel):
    ids: list[str] = Field(min_length=1, max_length=50)
    modality: Literal["compound", "crispr", "orf"] = "compound"


class FeatureRequest(BaseModel):
    ids: list[str] = Field(default_factory=list, max_length=500)
    modality: Literal["compound", "crispr", "orf"] = "compound"
    limit: int = 100
    offset: int = 0


class GalleryRequest(BaseModel):
    ids: list[str] = Field(default_factory=list, max_length=500)
    modality: Literal["compound", "crispr", "orf"] = "compound"
    limit: int = 20
    offset: int = 0


class ActivityCliffsRequest(BaseModel):
    limit: int = 100
    offset: int = 0


class ProfileRowsRequest(BaseModel):
    dataset: str = DEFAULT_ACTIVITY_DATASET
    ids: list[str] = Field(default_factory=list, max_length=500)
    columns: list[str] = Field(default_factory=list, max_length=100)
    limit: int = 100
    offset: int = 0


class AnnotationRequest(BaseModel):
    table: Literal[
        "repurposing_hub_annotations",
        "chembl_protein_targets",
        "chemical_probes",
        "kinase_probes",
        "mitotox_annotations",
        "toxicity_pk_annotations",
        "toxcast_active_assays",
        "toxcast_annotations",
        "motive_annotations",
        "compound_source",
    ]
    ids: list[str] = Field(default_factory=list, max_length=5000)
    limit: int = 100
    offset: int = 0


class WellRequest(BaseModel):
    ids: list[str] = Field(default_factory=list, max_length=5000)
    sources: list[str] = Field(default_factory=list, max_length=50)
    plates: list[str] = Field(default_factory=list, max_length=500)
    wells: list[str] = Field(default_factory=list, max_length=500)
    limit: int = 100
    offset: int = 0


TOOLS = [
    {"name": "health", "method": "GET", "path": "/health", "description": "Check files, database availability, and disk footprint."},
    {"name": "list_datasets", "method": "GET", "path": "/datasets", "description": "List profile datasets and copairs configurations."},
    {"name": "describe_schema", "method": "GET", "path": "/schema/{database}", "description": "Describe DuckDB or Zenodo Parquet schemas."},
    {"name": "resolve_entity", "method": "GET", "path": "/resolve", "description": "Resolve names, JCP IDs, SMILES, genes, targets, or MoAs."},
    {"name": "get_entity_summary", "method": "POST", "path": "/entities/summary", "description": "Return compound/gene metadata and optional activity."},
    {"name": "search_entities", "method": "POST", "path": "/search/entities", "description": "Filter compounds by metadata, properties, activity, and source."},
    {"name": "get_activity", "method": "POST", "path": "/activity", "description": "Return copairs activity scores and calls."},
    {"name": "get_activity_summary", "method": "POST", "path": "/activity/summary", "description": "Summarize activity calls for a run config."},
    {"name": "get_consistency", "method": "POST", "path": "/consistency", "description": "Return target/MoA/annotation consistency scores."},
    {"name": "get_consistency_summary", "method": "POST", "path": "/consistency/summary", "description": "Summarize consistency by annotation source."},
    {"name": "compare_configs", "method": "POST", "path": "/compare/configs", "description": "Compare activity summaries across run configs."},
    {"name": "get_cross_source_reproducibility", "method": "POST", "path": "/cross-source", "description": "Compare within-source and cross-source reproducibility."},
    {"name": "chemical_properties", "method": "POST", "path": "/chemical/properties", "description": "Return RDKit compound properties."},
    {"name": "nearest_neighbors", "method": "POST", "path": "/similarity/neighbors", "description": "Return JUMP Hub nearest matches."},
    {"name": "pairwise_similarity", "method": "POST", "path": "/similarity/pairwise", "description": "Return exact pairwise cosine similarities for small ID sets."},
    {"name": "interpretable_features", "method": "POST", "path": "/features/interpretable", "description": "Return top interpretable JUMP Hub features."},
    {"name": "gallery_images", "method": "POST", "path": "/gallery/images", "description": "Return gallery image URL records."},
    {"name": "activity_cliffs", "method": "POST", "path": "/activity-cliffs", "description": "Return processed activity-cliff pairs."},
    {"name": "profile_rows", "method": "POST", "path": "/profiles/rows", "description": "Return bounded profile rows and selected feature columns."},
    {"name": "profile_features", "method": "GET", "path": "/profiles/{dataset}/features", "description": "List profile columns/features."},
    {"name": "annotations", "method": "POST", "path": "/annotations", "description": "Return rows from allowlisted annotation tables."},
    {"name": "well_cell_counts", "method": "POST", "path": "/wells/cell-counts", "description": "Return well metadata joined to cell counts."},
    {"name": "source_summary", "method": "GET", "path": "/source/summary", "description": "Summarize compound source coverage and activity calls."},
    {"name": "artifact_search", "method": "GET", "path": "/artifacts/search", "description": "Find processed output files."},
    {"name": "artifact_read", "method": "GET", "path": "/artifacts/read", "description": "Read bounded JSON/CSV/Parquet processed artifacts."},
    {"name": "provenance", "method": "GET", "path": "/provenance", "description": "Return data paths, sizes, and service provenance."},
]


def service_manifest() -> dict[str, Any]:
    return {
        "name": "JUMP Agent API",
        "description": "OpenAPI/HTTP tool server for processed JUMP production and JUMP Hub data.",
        "base_url": "https://jump-agent.net",
        "discovery": {
            "manifest": "https://jump-agent.net/jump-agent",
            "openapi": "https://jump-agent.net/openapi.json",
            "docs": "https://jump-agent.net/docs",
            "tools": "https://jump-agent.net/tools",
            "health": "https://jump-agent.net/health",
        },
        "auth": {
            "required_for_data_endpoints": True,
            "preferred_header": "Authorization: Bearer <api-key>",
            "alternate_header": "X-API-Key: <api-key>",
            "public_endpoints": ["/health", "/tools", "/jump-agent", "/docs", "/openapi.json"],
        },
        "usage": {
            "discover": "Fetch /jump-agent, then /openapi.json for schemas.",
            "call": "Use JSON HTTP requests against listed endpoints with Authorization: Bearer <api-key>.",
            "example": {
                "method": "POST",
                "url": "https://jump-agent.net/activity/summary",
                "headers": {"Authorization": "Bearer <api-key>", "Content-Type": "application/json"},
                "body": {
                    "dataset": "compound_no_source7",
                    "preprocessing": "activity_no_target2",
                    "filter": "all_sources",
                    "activity_params": "default",
                },
            },
        },
        "tools": TOOLS,
    }


@app.get("/jump-agent")
def jump_agent_manifest() -> dict[str, Any]:
    return service_manifest()


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ok": METADATA_DB.exists() and COPAIRS_DB.exists() and ZENODO_ROOT.exists(),
        "data_root": str(DATA_ROOT),
        "metadata_db": file_info(METADATA_DB),
        "copairs_db": file_info(COPAIRS_DB),
        "zenodo_dir": file_info(ZENODO_ROOT),
        "production_dir": file_info(PRODUCTION_ROOT),
    }


@app.get("/tools")
def tools() -> dict[str, Any]:
    return {"tools": TOOLS, "auth": "X-API-Key required" if API_KEY else "disabled"}


@app.get("/datasets", dependencies=[Depends(require_api_key)])
def datasets() -> dict[str, Any]:
    profile_files = sorted(path.stem for path in PROFILES_DIR.glob("*.parquet"))
    activity_configs = query_db(
        COPAIRS_DB,
        """
        SELECT DISTINCT _dataset, _columns, _preprocessing, _filter, _activity_params
        FROM activity_results
        ORDER BY _dataset, _preprocessing, _filter, _activity_params
        """,
    )
    consistency_configs = query_db(
        COPAIRS_DB,
        """
        SELECT DISTINCT _dataset, _columns, _preprocessing, _filter, _group_type, _distance
        FROM consistency_results
        ORDER BY _dataset, _preprocessing, _filter, _group_type, _distance
        """,
    )
    zenodo_files = [
        {"name": path.name, "size": path.stat().st_size, "human_size": human_size(path.stat().st_size)}
        for path in sorted(ZENODO_ROOT.glob("*.parquet"))
    ]
    return {"profiles": profile_files, "activity_configs": activity_configs, "consistency_configs": consistency_configs, "zenodo_files": zenodo_files}


@app.get("/schema/{database}", dependencies=[Depends(require_api_key)])
def schema(database: Literal["metadata", "copairs", "zenodo"], table: Optional[str] = None, limit_columns: int = 200) -> dict[str, Any]:
    if database == "metadata":
        db_path = METADATA_DB
    elif database == "copairs":
        db_path = COPAIRS_DB
    else:
        if table is None:
            return {"files": sorted(path.name for path in ZENODO_ROOT.glob("*.parquet"))}
        path = ZENODO_ROOT / table
        if not path.exists():
            path = ZENODO_ROOT / f"{table}.parquet"
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"Zenodo table not found: {table}")
        con = duckdb.connect()
        try:
            rows = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
        finally:
            con.close()
        return {"database": "zenodo", "table": path.name, "column_count": len(rows), "columns": [row[0] for row in rows[:limit_columns]]}

    if table is None:
        return {"database": database, "tables": query_db(db_path, "SHOW TABLES")}
    table_id = quote_ident(table)
    return {"database": database, "table": table, "columns": query_db(db_path, f"DESCRIBE {table_id}")}


@app.get("/resolve", dependencies=[Depends(require_api_key)])
def resolve(q: str = Query(min_length=1), limit: int = 20) -> dict[str, Any]:
    limit = clamp_limit(limit, 20)
    pattern = f"%{q}%"
    compound_rows = query_db(
        METADATA_DB,
        """
        SELECT
          'compound' AS entity_type,
          Metadata_JCP2022 AS id,
          Metadata_repurposing_name AS name,
          Metadata_SMILES AS smiles,
          Metadata_InChIKey AS inchikey,
          Metadata_repurposing_target AS target,
          Metadata_repurposing_moa AS moa,
          Metadata_repurposing_disease_area AS disease_area
        FROM compound_metadata
        WHERE Metadata_JCP2022 ILIKE ?
           OR Metadata_repurposing_name ILIKE ?
           OR Metadata_SMILES ILIKE ?
           OR Metadata_InChIKey ILIKE ?
           OR Metadata_repurposing_target ILIKE ?
           OR Metadata_repurposing_moa ILIKE ?
           OR Metadata_repurposing_disease_area ILIKE ?
        ORDER BY
          CASE WHEN Metadata_JCP2022 = ? THEN 0
               WHEN Metadata_repurposing_name ILIKE ? THEN 1
               ELSE 2 END,
          Metadata_JCP2022
        LIMIT ?
        """,
        [pattern, pattern, pattern, pattern, pattern, pattern, pattern, q, q, limit],
    )
    gene_rows = query_db(
        METADATA_DB,
        """
        SELECT DISTINCT
          'gene' AS entity_type,
          Metadata_JCP2022 AS id,
          Metadata_Symbol AS symbol,
          Metadata_NCBI_Gene_ID AS ncbi_gene_id,
          Metadata_Gene_Description AS description,
          Metadata_perturbation_modality AS modality
        FROM gene_metadata
        WHERE Metadata_JCP2022 ILIKE ?
           OR Metadata_Symbol ILIKE ?
           OR CAST(Metadata_NCBI_Gene_ID AS VARCHAR) ILIKE ?
           OR Metadata_Gene_Description ILIKE ?
        ORDER BY Metadata_Symbol, Metadata_JCP2022
        LIMIT ?
        """,
        [pattern, pattern, pattern, pattern, limit],
    )
    return {"query": q, "results": (compound_rows + gene_rows)[:limit]}


@app.post("/entities/summary", dependencies=[Depends(require_api_key)])
def entity_summary(request: IdsRequest) -> dict[str, Any]:
    if not request.ids:
        raise HTTPException(status_code=400, detail="ids is required")
    compound_sql = """
        SELECT cm.*
        FROM compound_metadata cm
        WHERE cm.Metadata_JCP2022 = ANY(?)
        ORDER BY cm.Metadata_JCP2022
    """
    compounds = query_db(METADATA_DB, compound_sql, [request.ids])
    genes = query_db(
        METADATA_DB,
        """
        SELECT DISTINCT *
        FROM gene_metadata
        WHERE Metadata_JCP2022 = ANY(?) OR Metadata_Symbol = ANY(?)
        ORDER BY Metadata_JCP2022
        """,
        [request.ids, request.ids],
    )
    activity = []
    if request.include_activity:
        cfg = request.config
        activity = query_db(
            COPAIRS_DB,
            """
            SELECT Metadata_JCP2022, mean_average_precision, mean_normalized_average_precision,
                   p_value, corrected_p_value, below_corrected_p,
                   _dataset, _preprocessing, _filter, _activity_params
            FROM activity_results
            WHERE _dataset = ? AND _preprocessing = ? AND _filter = ? AND _activity_params = ?
              AND Metadata_JCP2022 = ANY(?)
            ORDER BY mean_normalized_average_precision DESC
            """,
            [cfg.dataset, cfg.preprocessing, cfg.filter, cfg.activity_params, request.ids],
        )
    return {"compounds": compounds, "genes": genes, "activity": activity, "config": request.config.model_dump()}


@app.post("/search/entities", dependencies=[Depends(require_api_key)])
def search_entities(request: SearchRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    where = ["1=1"]
    params: list[Any] = []
    if request.query:
        pattern = f"%{request.query}%"
        where.append(
            """
            (
              cm.Metadata_JCP2022 ILIKE ? OR cm.Metadata_repurposing_name ILIKE ?
              OR cm.Metadata_SMILES ILIKE ? OR cm.Metadata_InChIKey ILIKE ?
              OR cm.Metadata_repurposing_target ILIKE ? OR cm.Metadata_repurposing_moa ILIKE ?
              OR cm.Metadata_repurposing_disease_area ILIKE ?
            )
            """
        )
        params.extend([pattern] * 7)
    for values, column in [
        (request.targets, "Metadata_repurposing_target"),
        (request.moa, "Metadata_repurposing_moa"),
        (request.disease_area, "Metadata_repurposing_disease_area"),
    ]:
        for value in values:
            where.append(f"cm.{column} ILIKE ?")
            params.append(f"%{value}%")
    if request.source:
        where.append(
            """
            EXISTS (
              SELECT 1 FROM compound_source cs
              WHERE cs.Metadata_JCP2022 = cm.Metadata_JCP2022
                AND cs.Metadata_Compound_Source = ANY(?)
            )
            """
        )
        params.append(request.source)
    if request.has_pains is not None:
        where.append("cm.Metadata_HasPAINS = ?")
        params.append(request.has_pains)
    if request.valid_mol is not None:
        where.append("cm.Metadata_ValidMol = ?")
        params.append(request.valid_mol)
    if request.mw_min is not None:
        where.append("cm.Metadata_MW >= ?")
        params.append(request.mw_min)
    if request.mw_max is not None:
        where.append("cm.Metadata_MW <= ?")
        params.append(request.mw_max)
    if request.logp_min is not None:
        where.append("cm.Metadata_LogP >= ?")
        params.append(request.logp_min)
    if request.logp_max is not None:
        where.append("cm.Metadata_LogP <= ?")
        params.append(request.logp_max)
    if request.active is not None:
        cfg = request.config
        where.append(
            """
            EXISTS (
              SELECT 1
              FROM copairs.activity_results ar
              WHERE ar.Metadata_JCP2022 = cm.Metadata_JCP2022
                AND ar._dataset = ?
                AND ar._preprocessing = ?
                AND ar._filter = ?
                AND ar._activity_params = ?
                AND ar.below_corrected_p = ?
            )
            """
        )
        params.extend([cfg.dataset, cfg.preprocessing, cfg.filter, cfg.activity_params, request.active])

    sql = f"""
        SELECT
          cm.Metadata_JCP2022, cm.Metadata_repurposing_name, cm.Metadata_SMILES,
          cm.Metadata_InChIKey, cm.Metadata_repurposing_target, cm.Metadata_repurposing_moa,
          cm.Metadata_repurposing_disease_area, cm.Metadata_median_cell_count,
          cm.Metadata_MW, cm.Metadata_LogP, cm.Metadata_TPSA, cm.Metadata_Lipinski_Violations,
          cm.Metadata_QED, cm.Metadata_MurckoScaffold, cm.Metadata_HasPAINS, cm.Metadata_ValidMol
        FROM compound_metadata cm
        WHERE {' AND '.join(where)}
        ORDER BY cm.Metadata_JCP2022
        LIMIT ? OFFSET ?
    """
    params.extend([limit, max(0, request.offset)])
    query_fn = query_metadata_with_copairs if request.active is not None else lambda _sql, _params: query_db(METADATA_DB, _sql, _params)
    return {"results": query_fn(sql, params), "limit": limit, "offset": request.offset}


@app.post("/activity", dependencies=[Depends(require_api_key)])
def activity(request: ActivityRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    cfg = request.config
    where = ["_dataset = ?", "_preprocessing = ?", "_filter = ?", "_activity_params = ?"]
    params: list[Any] = [cfg.dataset, cfg.preprocessing, cfg.filter, cfg.activity_params]
    if request.ids:
        where.append("Metadata_JCP2022 = ANY(?)")
        params.append(request.ids)
    params.extend([limit, max(0, request.offset)])
    rows = query_db(
        COPAIRS_DB,
        f"""
        SELECT Metadata_JCP2022, Metadata_Source, mean_average_precision,
               mean_normalized_average_precision, p_value, corrected_p_value,
               below_p, below_corrected_p, _dataset, _preprocessing, _filter, _activity_params
        FROM activity_results
        WHERE {' AND '.join(where)}
        ORDER BY mean_normalized_average_precision DESC
        LIMIT ? OFFSET ?
        """,
        params,
    )
    return {"config": cfg.model_dump(), "results": rows, "limit": limit, "offset": request.offset}


@app.post("/activity/summary", dependencies=[Depends(require_api_key)])
def activity_summary(config: RunConfig) -> dict[str, Any]:
    rows = query_db(
        COPAIRS_DB,
        """
        SELECT
          COUNT(*) AS n_total,
          SUM(CASE WHEN below_corrected_p THEN 1 ELSE 0 END) AS n_active,
          AVG(mean_normalized_average_precision) AS mean_nmap,
          MEDIAN(mean_normalized_average_precision) AS median_nmap,
          MIN(corrected_p_value) AS min_corrected_p_value
        FROM activity_results
        WHERE _dataset = ? AND _preprocessing = ? AND _filter = ? AND _activity_params = ?
        """,
        [config.dataset, config.preprocessing, config.filter, config.activity_params],
    )
    result = rows[0]
    result["active_rate"] = (result["n_active"] / result["n_total"]) if result["n_total"] else None
    return {"config": config.model_dump(), "summary": result}


@app.post("/consistency", dependencies=[Depends(require_api_key)])
def consistency(request: ConsistencyRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    cfg = request.config
    where = [
        "_dataset = ?",
        "_preprocessing = ?",
        "_filter = ?",
        "_group_type = ?",
        "_distance = ?",
        "_preprocessing NOT LIKE '%_sweep'",
    ]
    params: list[Any] = [cfg.dataset, cfg.preprocessing, cfg.filter, cfg.group_type, cfg.distance]
    if request.group_value:
        where.append("group_value ILIKE ?")
        params.append(f"%{request.group_value}%")
    if request.significant_only:
        where.append("below_corrected_p = true")
    params.extend([limit, max(0, request.offset)])
    rows = query_db(
        COPAIRS_DB,
        f"""
        SELECT group_value, mean_average_precision, mean_normalized_average_precision,
               p_value, corrected_p_value, below_p, below_corrected_p,
               n_perturbations, _dataset, _preprocessing, _filter, _group_type, _distance
        FROM consistency_results
        WHERE {' AND '.join(where)}
        ORDER BY mean_normalized_average_precision DESC
        LIMIT ? OFFSET ?
        """,
        params,
    )
    return {"config": cfg.model_dump(), "results": rows, "limit": limit, "offset": request.offset}


@app.post("/consistency/summary", dependencies=[Depends(require_api_key)])
def consistency_summary(config: ConsistencyConfig) -> dict[str, Any]:
    rows = query_db(
        COPAIRS_DB,
        """
        SELECT
          _group_type AS group_type,
          _distance AS distance,
          COUNT(*) AS n_groups,
          SUM(CASE WHEN below_corrected_p THEN 1 ELSE 0 END) AS n_significant,
          AVG(mean_normalized_average_precision) AS mean_nmap,
          MEDIAN(mean_normalized_average_precision) AS median_nmap
        FROM consistency_results
        WHERE _dataset = ? AND _preprocessing = ? AND _filter = ?
          AND _group_type = ? AND _distance = ?
          AND _preprocessing NOT LIKE '%_sweep'
        GROUP BY _group_type, _distance
        """,
        [config.dataset, config.preprocessing, config.filter, config.group_type, config.distance],
    )
    return {"config": config.model_dump(), "summary": rows[0] if rows else None}


@app.post("/compare/configs", dependencies=[Depends(require_api_key)])
def compare_configs(request: CompareConfigsRequest) -> dict[str, Any]:
    configs = request.configs or [
        RunConfig(dataset="compound_no_source7"),
        RunConfig(dataset="compound_DL_CPCNN_no_source7"),
        RunConfig(dataset="compound_with_source7"),
        RunConfig(dataset="compound_DL_CPCNN_with_source7"),
    ]
    summaries = []
    for cfg in configs:
        summary = activity_summary(cfg)["summary"]
        summaries.append({"config": cfg.model_dump(), "summary": summary})
    return {"results": summaries}


@app.post("/cross-source", dependencies=[Depends(require_api_key)])
def cross_source(request: CrossSourceRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    where_ids = ""
    params: list[Any] = [request.dataset, request.preprocessing, request.filter, request.dataset, request.preprocessing, request.filter]
    if request.ids:
        where_ids = "WHERE w.Metadata_JCP2022 = ANY(?)"
        params.append(request.ids)
    params.extend([limit, max(0, request.offset)])
    rows = query_db(
        COPAIRS_DB,
        f"""
        WITH within_src AS (
            SELECT Metadata_JCP2022, Metadata_Source,
                   mean_normalized_average_precision AS nmap_within,
                   corrected_p_value AS p_within,
                   below_corrected_p AS sig_within
            FROM activity_results
            WHERE _dataset = ? AND _activity_params = 'withinsource'
              AND _preprocessing = ? AND _filter = ?
        ),
        cross_src AS (
            SELECT Metadata_JCP2022, Metadata_Source,
                   mean_normalized_average_precision AS nmap_cross,
                   corrected_p_value AS p_cross,
                   below_corrected_p AS sig_cross
            FROM activity_results
            WHERE _dataset = ? AND _activity_params = 'crosssource'
              AND _preprocessing = ? AND _filter = ?
        )
        SELECT w.Metadata_JCP2022, w.Metadata_Source, w.nmap_within, w.p_within,
               w.sig_within, c.nmap_cross, c.p_cross, c.sig_cross,
               (w.nmap_within - c.nmap_cross) AS nmap_loss
        FROM within_src w
        JOIN cross_src c USING (Metadata_JCP2022, Metadata_Source)
        {where_ids}
        ORDER BY nmap_loss DESC
        LIMIT ? OFFSET ?
        """,
        params,
    )
    return {"results": rows, "limit": limit, "offset": request.offset}


@app.post("/chemical/properties", dependencies=[Depends(require_api_key)])
def chemical_properties(request: ChemicalPropertiesRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    where = "WHERE Metadata_JCP2022 = ANY(?)" if request.ids else ""
    params: list[Any] = [request.ids] if request.ids else []
    params.extend([limit, max(0, request.offset)])
    rows = query_db(
        METADATA_DB,
        f"""
        SELECT *
        FROM compound_properties
        {where}
        ORDER BY Metadata_JCP2022
        LIMIT ? OFFSET ?
        """,
        params,
    )
    return {"results": rows, "limit": limit, "offset": request.offset}


@app.post("/similarity/neighbors", dependencies=[Depends(require_api_key)])
def nearest_neighbors(request: NeighborsRequest) -> dict[str, Any]:
    top_k = clamp_limit(request.top_k, 20)
    path = modality_file(request.modality)
    base_sql = """
        SELECT
          "Perturbation", "Match", "Perturbation-Match Similarity" AS similarity,
          "JCP2022", "Match JCP2022", "Synonyms",
          "Corrected p-value", "Phenotypic activity",
          "Corrected p-value Match", "Phenotypic activity Match",
          "Match resources"
        FROM read_parquet(?)
        WHERE "JCP2022" = ?
        ORDER BY "Perturbation-Match Similarity" DESC
        LIMIT ?
    """
    con = duckdb.connect()
    try:
        rows = rows_to_dicts(con.execute(base_sql, [str(path), request.id, top_k]))
        if request.include_reverse:
            reverse_rows = rows_to_dicts(
                con.execute(
                    """
                    SELECT
                      "Match" AS "Perturbation", "Perturbation" AS "Match",
                      "Perturbation-Match Similarity" AS similarity,
                      "Match JCP2022" AS "JCP2022", "JCP2022" AS "Match JCP2022",
                      "Synonyms", "Corrected p-value Match" AS "Corrected p-value",
                      "Phenotypic activity Match" AS "Phenotypic activity",
                      "Corrected p-value" AS "Corrected p-value Match",
                      "Phenotypic activity" AS "Phenotypic activity Match",
                      "Match resources"
                    FROM read_parquet(?)
                    WHERE "Match JCP2022" = ?
                    ORDER BY "Perturbation-Match Similarity" DESC
                    LIMIT ?
                    """,
                    [str(path), request.id, top_k],
                )
            )
            seen = {(row["JCP2022"], row["Match JCP2022"]) for row in rows}
            for row in reverse_rows:
                key = (row["JCP2022"], row["Match JCP2022"])
                if key not in seen:
                    rows.append(row)
                    seen.add(key)
            rows = sorted(rows, key=lambda row: row.get("similarity") or -999, reverse=True)[:top_k]
    finally:
        con.close()
    return {"modality": request.modality, "id": request.id, "results": rows}


@app.post("/similarity/pairwise", dependencies=[Depends(require_api_key)])
def pairwise_similarity(request: PairwiseRequest) -> dict[str, Any]:
    ids = list(dict.fromkeys(request.ids))
    columns = matrix_columns(request.modality)
    missing = [id_ for id_ in ids if id_ not in columns]
    if missing:
        raise HTTPException(status_code=404, detail={"missing_ids": missing[:20], "message": "IDs not found in matrix columns"})
    row_numbers = [columns.index(id_) + 1 for id_ in ids]
    selected_columns = ", ".join(quote_ident(id_) for id_ in ids)
    path = matrix_file(request.modality)
    sql = f"""
        SELECT rn, {selected_columns}
        FROM (
          SELECT row_number() OVER () AS rn, {selected_columns}
          FROM read_parquet(?)
        )
        WHERE rn = ANY(?)
        ORDER BY rn
    """
    con = duckdb.connect()
    try:
        rows = rows_to_dicts(con.execute(sql, [str(path), row_numbers]))
    finally:
        con.close()
    rn_to_id = {columns.index(id_) + 1: id_ for id_ in ids}
    matrix = []
    for row in rows:
        source_id = rn_to_id[row.pop("rn")]
        matrix.append({"id": source_id, "similarities": {target: row[target] for target in ids}})
    return {"modality": request.modality, "ids": ids, "matrix": matrix}


@app.post("/features/interpretable", dependencies=[Depends(require_api_key)])
def interpretable_features(request: FeatureRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    path = modality_file(request.modality, "_interpretable_features")
    where = 'WHERE "JCP2022" = ANY(?)' if request.ids else ""
    params: list[Any] = [str(path)]
    if request.ids:
        params.append(request.ids)
    params.extend([limit, max(0, request.offset)])
    con = duckdb.connect()
    try:
        rows = rows_to_dicts(
            con.execute(
                f"""
                SELECT *
                FROM read_parquet(?)
                {where}
                ORDER BY "|Cohen's d|" DESC NULLS LAST
                LIMIT ? OFFSET ?
                """,
                params,
            )
        )
    finally:
        con.close()
    return {"modality": request.modality, "results": rows, "limit": limit, "offset": request.offset}


@app.post("/gallery/images", dependencies=[Depends(require_api_key)])
def gallery_images(request: GalleryRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 20)
    path = modality_file(request.modality, "_gallery")
    where = 'WHERE "JCP2022" = ANY(?)' if request.ids else ""
    params: list[Any] = [str(path)]
    if request.ids:
        params.append(request.ids)
    params.extend([limit, max(0, request.offset)])
    con = duckdb.connect()
    try:
        rows = rows_to_dicts(
            con.execute(
                f"""
                SELECT *
                FROM read_parquet(?)
                {where}
                LIMIT ? OFFSET ?
                """,
                params,
            )
        )
    finally:
        con.close()
    return {"modality": request.modality, "results": rows, "limit": limit, "offset": request.offset}


@app.post("/activity-cliffs", dependencies=[Depends(require_api_key)])
def activity_cliffs(request: ActivityCliffsRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    path = PROCESSED_DIR / "activity-cliffs" / "compound_no_source7" / "cliff_pairs.parquet"
    if not path.exists():
        path = PROCESSED_DIR / "activity-cliffs" / "compound_no_source7" / "cliff_pairs.csv"
    con = duckdb.connect()
    try:
        if path.suffix == ".parquet":
            sql = "SELECT * FROM read_parquet(?) LIMIT ? OFFSET ?"
        else:
            sql = "SELECT * FROM read_csv_auto(?) LIMIT ? OFFSET ?"
        rows = rows_to_dicts(con.execute(sql, [str(path), limit, max(0, request.offset)]))
    finally:
        con.close()
    return {"source": str(path), "results": rows, "limit": limit, "offset": request.offset}


@app.post("/profiles/rows", dependencies=[Depends(require_api_key)])
def profile_rows(request: ProfileRowsRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    path = profile_file(request.dataset)
    con = duckdb.connect()
    try:
        available = [row[0] for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()]
        default_cols = [col for col in ["Metadata_JCP2022", "Metadata_Source", "Metadata_Plate", "Metadata_Well", "Metadata_Batch"] if col in available]
        selected = request.columns or default_cols
        bad = [col for col in selected if col not in available]
        if bad:
            raise HTTPException(status_code=400, detail={"unknown_columns": bad[:20]})
        select_sql = ", ".join(quote_ident(col) for col in selected)
        where = 'WHERE "Metadata_JCP2022" = ANY(?)' if request.ids else ""
        params: list[Any] = [str(path)]
        if request.ids:
            params.append(request.ids)
        params.extend([limit, max(0, request.offset)])
        rows = rows_to_dicts(con.execute(f"SELECT {select_sql} FROM read_parquet(?) {where} LIMIT ? OFFSET ?", params))
    finally:
        con.close()
    return {"dataset": request.dataset, "columns": selected, "results": rows, "limit": limit, "offset": request.offset}


@app.get("/profiles/{dataset}/features", dependencies=[Depends(require_api_key)])
def profile_features(dataset: str, limit: int = 500) -> dict[str, Any]:
    path = profile_file(dataset)
    con = duckdb.connect()
    try:
        columns = [row[0] for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()]
    finally:
        con.close()
    metadata_cols = [col for col in columns if col.startswith("Metadata_")]
    feature_cols = [col for col in columns if not col.startswith("Metadata_")]
    return {
        "dataset": dataset,
        "column_count": len(columns),
        "metadata_columns": metadata_cols[:limit],
        "feature_count": len(feature_cols),
        "feature_columns": feature_cols[:limit],
    }


@app.post("/annotations", dependencies=[Depends(require_api_key)])
def annotations(request: AnnotationRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    table_id = quote_ident(request.table)
    where = "WHERE Metadata_JCP2022 = ANY(?)" if request.ids else ""
    params: list[Any] = [request.ids] if request.ids else []
    params.extend([limit, max(0, request.offset)])
    rows = query_db(
        METADATA_DB,
        f"""
        SELECT *
        FROM {table_id}
        {where}
        LIMIT ? OFFSET ?
        """,
        params,
    )
    return {"table": request.table, "results": rows, "limit": limit, "offset": request.offset}


@app.post("/wells/cell-counts", dependencies=[Depends(require_api_key)])
def well_cell_counts(request: WellRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    where = ["1=1"]
    params: list[Any] = []
    if request.ids:
        where.append("w.Metadata_JCP2022 = ANY(?)")
        params.append(request.ids)
    if request.sources:
        where.append("w.Metadata_Source = ANY(?)")
        params.append(request.sources)
    if request.plates:
        where.append("w.Metadata_Plate = ANY(?)")
        params.append(request.plates)
    if request.wells:
        where.append("w.Metadata_Well = ANY(?)")
        params.append(request.wells)
    params.extend([limit, max(0, request.offset)])
    rows = query_db(
        METADATA_DB,
        f"""
        SELECT
          w.Metadata_Source, w.Metadata_Plate, w.Metadata_Well, w.Metadata_JCP2022,
          cc.Metadata_Batch, cc.Metadata_Count_Cells
        FROM well w
        LEFT JOIN cell_counts cc
          ON cc.Metadata_Source = w.Metadata_Source
         AND cc.Metadata_Plate = w.Metadata_Plate
         AND cc.Metadata_Well = w.Metadata_Well
        WHERE {' AND '.join(where)}
        ORDER BY w.Metadata_Source, w.Metadata_Plate, w.Metadata_Well
        LIMIT ? OFFSET ?
        """,
        params,
    )
    return {"results": rows, "limit": limit, "offset": request.offset}


@app.get("/source/summary", dependencies=[Depends(require_api_key)])
def source_summary(
    dataset: str = DEFAULT_ACTIVITY_DATASET,
    preprocessing: str = DEFAULT_ACTIVITY_PREPROCESSING,
    filter: str = DEFAULT_FILTER,
    activity_params: str = DEFAULT_ACTIVITY_PARAMS,
) -> dict[str, Any]:
    rows = query_metadata_with_copairs(
        """
        WITH source_counts AS (
          SELECT Metadata_Compound_Source AS source, COUNT(DISTINCT Metadata_JCP2022) AS n_compounds
          FROM compound_source
          GROUP BY Metadata_Compound_Source
        ),
        active_counts AS (
          SELECT cs.Metadata_Compound_Source AS source,
                 COUNT(DISTINCT ar.Metadata_JCP2022) AS n_active
          FROM compound_source cs
          JOIN copairs.activity_results ar
            ON ar.Metadata_JCP2022 = cs.Metadata_JCP2022
          WHERE ar._dataset = ?
            AND ar._preprocessing = ?
            AND ar._filter = ?
            AND ar._activity_params = ?
            AND ar.below_corrected_p
          GROUP BY cs.Metadata_Compound_Source
        )
        SELECT sc.source, sc.n_compounds, COALESCE(ac.n_active, 0) AS n_active,
               COALESCE(ac.n_active, 0)::DOUBLE / NULLIF(sc.n_compounds, 0) AS active_rate
        FROM source_counts sc
        LEFT JOIN active_counts ac USING (source)
        ORDER BY sc.source
        """,
        [dataset, preprocessing, filter, activity_params],
    )
    return {
        "config": {
            "dataset": dataset,
            "preprocessing": preprocessing,
            "filter": filter,
            "activity_params": activity_params,
        },
        "results": rows,
    }


@app.get("/artifacts/search", dependencies=[Depends(require_api_key)])
def artifact_search(q: str = "", limit: int = 100) -> dict[str, Any]:
    limit = clamp_limit(limit, 100)
    q_lower = q.lower()
    results = []
    for path in PROCESSED_DIR.rglob("*"):
        if not path.is_file():
            continue
        rel = str(path.relative_to(PROCESSED_DIR))
        if q_lower and q_lower not in rel.lower():
            continue
        results.append({"path": str(path), "relative_path": rel, "size": path.stat().st_size, "human_size": human_size(path.stat().st_size)})
        if len(results) >= limit:
            break
    return {"query": q, "results": results}


@app.get("/artifacts/read", dependencies=[Depends(require_api_key)])
def artifact_read(relative_path: str, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    limit = clamp_limit(limit, 100)
    path = processed_artifact_path(relative_path)
    suffix = path.suffix.lower()
    base = {"relative_path": str(path.relative_to(PROCESSED_DIR)), "path": str(path), "size": path.stat().st_size, "human_size": human_size(path.stat().st_size)}
    if suffix == ".json":
        with path.open() as handle:
            return {**base, "kind": "json", "content": json.load(handle)}
    if suffix in {".csv", ".tsv", ".parquet"}:
        con = duckdb.connect()
        try:
            if suffix == ".parquet":
                rows = rows_to_dicts(con.execute("SELECT * FROM read_parquet(?) LIMIT ? OFFSET ?", [str(path), limit, max(0, offset)]))
            else:
                delim = "\t" if suffix == ".tsv" else ","
                rows = rows_to_dicts(
                    con.execute(
                        f"SELECT * FROM read_csv_auto(?, delim = {quote_literal(delim)}, header = true) LIMIT ? OFFSET ?",
                        [str(path), limit, max(0, offset)],
                    )
                )
        finally:
            con.close()
        return {**base, "kind": suffix.lstrip("."), "results": rows, "limit": limit, "offset": offset}
    if suffix in {".txt", ".md"} and path.stat().st_size <= 1_000_000:
        return {**base, "kind": suffix.lstrip("."), "content": path.read_text(errors="replace")}
    return {**base, "kind": suffix.lstrip(".") or "binary", "content": None}


@app.get("/provenance", dependencies=[Depends(require_api_key)])
def provenance() -> dict[str, Any]:
    return {
        "service": "jump-agent-api",
        "version": app.version,
        "data_root": str(DATA_ROOT),
        "production": file_info(PRODUCTION_ROOT),
        "zenodo": file_info(ZENODO_ROOT),
        "metadata_db": file_info(METADATA_DB),
        "copairs_db": file_info(COPAIRS_DB),
        "profile_dir": file_info(PROFILES_DIR),
        "processed_dir": file_info(PROCESSED_DIR),
        "default_configs": {
            "activity": RunConfig().model_dump(),
            "consistency": ConsistencyConfig().model_dump(),
        },
    }
