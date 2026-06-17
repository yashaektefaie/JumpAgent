from __future__ import annotations

from datetime import datetime, timezone
import math
import os
import re
import json
from uuid import uuid4
from pathlib import Path
from typing import Any, Callable, Literal, Optional, Union

import duckdb
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field, ValidationError


DATA_ROOT = Path(os.environ.get("JUMP_DATA_ROOT", "/srv/jump/data"))
PRODUCTION_ROOT = DATA_ROOT / "jump_production"
ZENODO_ROOT = DATA_ROOT / "jump_hub_zenodo"
INTERIM_DIR = PRODUCTION_ROOT / "interim"
METADATA_DB = PRODUCTION_ROOT / "interim" / "jump_metadata_augmented.duckdb"
COPAIRS_DB = PRODUCTION_ROOT / "processed" / "copairs_results.duckdb"
PROFILES_DIR = PRODUCTION_ROOT / "profiles"
PROCESSED_DIR = PRODUCTION_ROOT / "processed"
MEMOS_DIR = Path(os.environ.get("JUMP_MEMOS_DIR", str(DATA_ROOT.parent / "memos")))

MAX_LIMIT = 5000
DEFAULT_ACTIVITY_DATASET = "compound_no_source7"
DEFAULT_ACTIVITY_PREPROCESSING = "activity_no_target2"
DEFAULT_FILTER = "all_sources"
DEFAULT_ACTIVITY_PARAMS = "default"
DEFAULT_CONSISTENCY_PREPROCESSING = "consistency_no_target2"
DEFAULT_DISTANCE = "cosine"
MCP_PROTOCOL_VERSION = "2025-06-18"
MCP_MAX_TEXT_CHARS = int(os.environ.get("JUMP_MCP_MAX_TEXT_CHARS", "200000"))

ANNOTATION_TABLES = {
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
}

TARGET_ANNOTATION_COLUMNS = {
    "repurposing": "Metadata_repurposing_target",
    "uniprot": "Metadata_Uniprot_target",
    "chemical_probes": "Metadata_chmprb_target_genes",
    "moa": "Metadata_repurposing_moa",
    "disease_area": "Metadata_repurposing_disease_area",
    "motive_biokg": "Metadata_motive_gene_biokg",
    "motive_opentargets": "Metadata_motive_gene_opentargets",
    "motive_primekg": "Metadata_motive_gene_primekg",
    "toxcast_assay": "Metadata_txcst_active_assays",
}

ANNOTATION_SOURCE_TABLES = {
    "repurposing": "repurposing_hub_annotations",
    "chembl": "chembl_protein_targets",
    "chemical_probes": "chemical_probes",
    "kinase_probes": "kinase_probes",
    "motive": "motive_annotations",
    "toxcast": "toxcast_annotations",
    "mitotox": "mitotox_annotations",
    "toxicity_pk": "toxicity_pk_annotations",
}

SUMMARY_TABLES = {
    "metadata": {
        "plate",
        "well",
        "perturbation",
        "perturbation_control",
        "compound",
        "compound_source",
        "compound_metadata",
        "gene_metadata",
        "cell_counts",
        "compound_properties",
        *ANNOTATION_TABLES,
    },
    "copairs": {"activity_results", "activity_scores", "consistency_results", "consistency_scores"},
}

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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def memo_date_path(created_at: str) -> Path:
    return MEMOS_DIR / f"{created_at[:10]}.jsonl"


def safe_tags(tags: list[str]) -> list[str]:
    cleaned = []
    for tag in tags:
        value = re.sub(r"[^A-Za-z0-9_.:/+-]+", "-", tag.strip())[:60].strip("-")
        if value and value not in cleaned:
            cleaned.append(value)
    return cleaned[:20]


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


def db_path(database: Literal["metadata", "copairs"]) -> Path:
    return METADATA_DB if database == "metadata" else COPAIRS_DB


def require_allowed_table(database: Literal["metadata", "copairs"], table: str) -> None:
    allowed = SUMMARY_TABLES[database]
    if table not in allowed:
        raise HTTPException(status_code=400, detail={"message": f"Unsupported table for {database}", "allowed": sorted(allowed)})


def table_columns(database: Literal["metadata", "copairs"], table: str) -> list[str]:
    require_allowed_table(database, table)
    table_id = quote_ident(table)
    return [row["column_name"] for row in query_db(db_path(database), f"DESCRIBE {table_id}")]


def require_columns(database: Literal["metadata", "copairs"], table: str, columns: list[str]) -> None:
    available = set(table_columns(database, table))
    missing = [column for column in columns if column not in available]
    if missing:
        raise HTTPException(status_code=400, detail={"unknown_columns": missing, "available_columns": sorted(available)})


def entity_ids_from_rows(rows: Any, preferred_field: Optional[str] = None) -> list[str]:
    fallback_fields = ["Metadata_JCP2022", "JCP2022", "Match JCP2022", "id"]
    seen: set[str] = set()
    ids: list[str] = []

    def visit(value: Any, fields: list[str]) -> None:
        if isinstance(value, dict):
            for field in fields:
                if field and isinstance(value.get(field), str) and value[field].startswith("JCP"):
                    if value[field] not in seen:
                        seen.add(value[field])
                        ids.append(value[field])
            for child in value.values():
                visit(child, fields)
        elif isinstance(value, list):
            for child in value:
                visit(child, fields)

    if preferred_field:
        visit(rows, [preferred_field])
        if ids:
            return ids
    visit(rows, fallback_fields)
    return ids


def activity_where_sql(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return (
        f"{prefix}_dataset = ? AND {prefix}_preprocessing = ? "
        f"AND {prefix}_filter = ? AND {prefix}_activity_params = ?"
    )


def consistency_where_sql(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return (
        f"{prefix}_dataset = ? AND {prefix}_preprocessing = ? AND {prefix}_filter = ? "
        f"AND {prefix}_group_type = ? AND {prefix}_distance = ?"
    )


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


class NeighborhoodRequest(BaseModel):
    id: str
    modality: Literal["compound", "crispr", "orf"] = "compound"
    top_k: int = 10
    include_reverse: bool = False
    include_query: bool = True
    include_activity: bool = True
    include_features: bool = True
    include_gallery: bool = True
    config: RunConfig = Field(default_factory=RunConfig)
    annotation_tables: list[Literal[
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
    ]] = Field(default_factory=list, max_length=10)


class MetadataSummaryRequest(BaseModel):
    database: Literal["metadata", "copairs"] = "metadata"
    table: str
    group_by: list[str] = Field(default_factory=list, max_length=4)
    count_distinct: Optional[str] = None
    filters: dict[str, list[Union[str, int, float, bool]]] = Field(default_factory=dict)
    limit: int = 100
    offset: int = 0


class ActivityCompareRequest(BaseModel):
    left: RunConfig = Field(default_factory=RunConfig)
    right: RunConfig = Field(default_factory=lambda: RunConfig(dataset="compound_DL_CPCNN_no_source7"))
    ids: list[str] = Field(default_factory=list, max_length=5000)
    limit: int = 100
    offset: int = 0


class ConsistencyCompareRequest(BaseModel):
    left: ConsistencyConfig = Field(default_factory=ConsistencyConfig)
    right: ConsistencyConfig = Field(default_factory=lambda: ConsistencyConfig(dataset="compound_DL_CPCNN_no_source7"))
    group_value: Optional[str] = None
    significant_only: bool = False
    limit: int = 100
    offset: int = 0


class ConsistencySweepRequest(BaseModel):
    dataset: str = DEFAULT_ACTIVITY_DATASET
    preprocessing: str = "consistency_no_target2_sweep"
    filter: str = DEFAULT_FILTER
    group_type: str = "repurposing"
    distance: str = DEFAULT_DISTANCE
    thresholds: list[float] = Field(default_factory=list, max_length=20)
    group_value: Optional[str] = None
    significant_only: bool = False
    limit: int = 100
    offset: int = 0


class AnnotationCoverageRequest(BaseModel):
    sources: list[Literal[
        "repurposing",
        "chembl",
        "chemical_probes",
        "kinase_probes",
        "motive",
        "toxcast",
        "mitotox",
        "toxicity_pk",
    ]] = Field(default_factory=list, max_length=8)
    group_by: Literal["none", "compound_source", "imaging_source"] = "none"
    limit: int = 100
    offset: int = 0


class DarkMatterRequest(BaseModel):
    config: RunConfig = Field(default_factory=RunConfig)
    annotation_groups: list[Literal[
        "repurposing",
        "uniprot",
        "chemical_probes",
        "moa",
        "disease_area",
        "motive_biokg",
        "motive_opentargets",
        "motive_primekg",
        "toxcast_assay",
    ]] = Field(default_factory=list, max_length=9)
    limit: int = 100
    offset: int = 0


class ScaffoldRequest(BaseModel):
    ids: list[str] = Field(default_factory=list, max_length=100)
    scaffold: Optional[str] = None
    config: RunConfig = Field(default_factory=RunConfig)
    active_only: bool = False
    min_size: int = 2
    limit: int = 100
    offset: int = 0


class WorkflowIdsFrom(BaseModel):
    step: str
    field: Optional[str] = None
    target: Literal["ids", "id"] = "ids"
    include_existing: bool = True


class WorkflowStep(BaseModel):
    name: str
    params: dict[str, Any] = Field(default_factory=dict)
    save_as: Optional[str] = None
    ids_from: Optional[WorkflowIdsFrom] = None


class WorkflowComposeRequest(BaseModel):
    steps: list[WorkflowStep] = Field(min_length=1, max_length=12)


class MemoSubmitRequest(BaseModel):
    title: str = Field(min_length=3, max_length=200)
    memo: str = Field(min_length=10, max_length=5000)
    category: Literal["missing_primitive", "bug", "data_gap", "documentation", "workflow_request", "other"] = "missing_primitive"
    priority: Literal["low", "normal", "high"] = "normal"
    agent: Optional[str] = Field(default=None, max_length=120)
    related_question: Optional[str] = Field(default=None, max_length=1000)
    suggested_endpoint: Optional[str] = Field(default=None, max_length=160)
    current_workaround: Optional[str] = Field(default=None, max_length=2000)
    tags: list[str] = Field(default_factory=list, max_length=20)


class EmptyMcpRequest(BaseModel):
    pass


class SchemaMcpRequest(BaseModel):
    database: Literal["metadata", "copairs", "zenodo"]
    table: Optional[str] = None
    limit_columns: int = 200


class ResolveMcpRequest(BaseModel):
    q: str = Field(min_length=1)
    limit: int = 20


class ProfileFeaturesMcpRequest(BaseModel):
    dataset: str
    limit: int = 500


class SourceSummaryMcpRequest(BaseModel):
    dataset: str = DEFAULT_ACTIVITY_DATASET
    preprocessing: str = DEFAULT_ACTIVITY_PREPROCESSING
    filter: str = DEFAULT_FILTER
    activity_params: str = DEFAULT_ACTIVITY_PARAMS


class WorkflowDetailMcpRequest(BaseModel):
    name: str


class ArtifactSearchMcpRequest(BaseModel):
    q: str = ""
    limit: int = 100


class ArtifactReadMcpRequest(BaseModel):
    relative_path: str
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
    {"name": "workflow_catalog", "method": "GET", "path": "/workflows", "description": "List composable workflow recipes and allowed primitive steps."},
    {"name": "workflow_detail", "method": "GET", "path": "/workflows/{name}", "description": "Describe a workflow recipe and its primitive sequence."},
    {"name": "workflow_neighborhood", "method": "POST", "path": "/workflows/neighborhood", "description": "Compound/gene neighborhood: nearest profiles plus metadata, activity, features, and gallery records."},
    {"name": "workflow_compose", "method": "POST", "path": "/workflows/compose", "description": "Run a bounded declarative composition of existing JumpAgent primitives."},
    {"name": "metadata_summary", "method": "POST", "path": "/metadata/summary", "description": "Bounded group-by summaries over allowlisted metadata or copairs tables."},
    {"name": "activity_compare", "method": "POST", "path": "/activity/compare", "description": "Compare per-compound activity calls between two run configs."},
    {"name": "consistency_compare", "method": "POST", "path": "/consistency/compare", "description": "Compare target/MoA consistency results between two run configs."},
    {"name": "consistency_sweep", "method": "POST", "path": "/consistency/sweep", "description": "Query activity-threshold sweep consistency results."},
    {"name": "annotation_coverage", "method": "POST", "path": "/annotations/coverage", "description": "Summarize compound annotation coverage overall or by source."},
    {"name": "dark_matter", "method": "POST", "path": "/annotations/dark-matter", "description": "Find active compounds lacking selected target annotations."},
    {"name": "scaffold_series", "method": "POST", "path": "/structure/scaffolds", "description": "Summarize scaffold activity or list compounds in scaffold series."},
    {"name": "submit_memo", "method": "POST", "path": "/memos", "description": "Submit a structured memo for missing primitives, data gaps, bugs, or workflow requests."},
    {"name": "list_memos", "method": "GET", "path": "/memos", "description": "List submitted capability/request memos for maintainers."},
    {"name": "artifact_search", "method": "GET", "path": "/artifacts/search", "description": "Find processed output files."},
    {"name": "artifact_read", "method": "GET", "path": "/artifacts/read", "description": "Read bounded JSON/CSV/Parquet processed artifacts."},
    {"name": "provenance", "method": "GET", "path": "/provenance", "description": "Return data paths, sizes, and service provenance."},
]


WORKFLOW_RECIPES: dict[str, dict[str, Any]] = {
    "neighborhood": {
        "endpoint": "/workflows/neighborhood",
        "description": "Given a JCP2022 perturbation, return nearest morphological neighbors with annotations, activity, interpretable feature rows, and gallery image records.",
        "primitive_sequence": [
            "similarity_neighbors",
            "entities_summary",
            "activity",
            "features_interpretable",
            "gallery_images",
            "annotations",
        ],
        "questions_helped": [
            "target deconvolution",
            "off-target detection",
            "phenotypic scaffold hopping",
            "mechanism-of-action neighborhood review",
        ],
        "pseudocode": [
            "neighbors = nearest_neighbors(query_id, modality, top_k)",
            "ids = [query_id] + neighbors['Match JCP2022']",
            "metadata = entity_summary(ids)",
            "features = interpretable_features(ids)",
            "gallery = gallery_images(ids)",
        ],
    },
    "dark_matter": {
        "endpoint": "/annotations/dark-matter",
        "description": "Find active compounds that lack selected target/MoA annotations in the pre-joined compound metadata view.",
        "primitive_sequence": ["activity", "compound_metadata"],
        "questions_helped": ["dark chemical matter", "novel mechanism candidate prioritization"],
    },
    "run_comparison": {
        "endpoint": "/activity/compare",
        "description": "Compare activity calls and nMAP deltas across two production configs, including CP vs DL, source 7, or Harmony variants.",
        "primitive_sequence": ["activity_results(left)", "activity_results(right)", "delta/significance summary"],
        "questions_helped": ["Source 7 decision", "batch correction comparison", "foundation model benchmark"],
    },
    "metadata_groupby": {
        "endpoint": "/metadata/summary",
        "description": "Run bounded group-by/count summaries over allowlisted metadata and copairs tables.",
        "primitive_sequence": ["schema", "validated group-by query"],
        "questions_helped": ["source coverage", "plate/well design", "annotation coverage", "data quality triage"],
    },
    "compose": {
        "endpoint": "/workflows/compose",
        "description": "Submit a small ordered list of existing primitive calls. A later step can pull IDs from an earlier step, which gives agents a safe way to build new analyses without arbitrary server-side code execution.",
        "primitive_sequence": ["allowed step", "optional ids_from", "next allowed step"],
        "questions_helped": ["custom bounded agent workflows"],
    },
    "memo_inbox": {
        "endpoint": "/memos",
        "description": "Submit a structured capability-gap memo when existing primitives are insufficient for an analysis.",
        "primitive_sequence": ["agent identifies missing primitive", "POST /memos", "maintainer reviews memo inbox"],
        "questions_helped": ["server roadmap", "missing primitive triage", "agent feedback loop"],
    },
}


def service_manifest() -> dict[str, Any]:
    return {
        "name": "JUMP Agent API",
        "description": "OpenAPI/HTTP tool server for processed JUMP production and JUMP Hub data.",
        "base_url": "https://jump-agent.net",
        "discovery": {
            "manifest": "https://jump-agent.net/jump-agent",
            "mcp": "https://jump-agent.net/mcp",
            "openapi": "https://jump-agent.net/openapi.json",
            "docs": "https://jump-agent.net/docs",
            "tools": "https://jump-agent.net/tools",
            "health": "https://jump-agent.net/health",
        },
        "auth": {
            "required_for_data_endpoints": True,
            "preferred_header": "Authorization: Bearer <api-key>",
            "alternate_header": "X-API-Key: <api-key>",
            "public_endpoints": ["/health", "/tools", "/jump-agent", "/mcp", "/docs", "/openapi.json"],
        },
        "usage": {
            "discover": "Fetch /jump-agent, then /openapi.json for schemas.",
            "call": "Use JSON HTTP requests against listed endpoints with Authorization: Bearer <api-key>.",
            "mcp": "Use claude mcp add --transport http jump https://jump-agent.net/mcp for public MCP access.",
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


@app.get("/workflows", dependencies=[Depends(require_api_key)])
def workflow_catalog() -> dict[str, Any]:
    return {
        "recipes": [
            {"name": name, "endpoint": recipe["endpoint"], "description": recipe["description"]}
            for name, recipe in WORKFLOW_RECIPES.items()
        ],
        "compose": {
            "endpoint": "/workflows/compose",
            "allowed_steps": sorted(COMPOSE_STEP_BUILDERS),
            "ids_from": "A step can set ids_from={step, field, target} to feed JCP2022 IDs from an earlier result into ids or id.",
            "security_model": "Declarative composition of allowlisted primitives only; arbitrary Python execution is intentionally not supported.",
        },
    }


@app.get("/workflows/{name}", dependencies=[Depends(require_api_key)])
def workflow_detail(name: str) -> dict[str, Any]:
    if name not in WORKFLOW_RECIPES:
        raise HTTPException(status_code=404, detail={"message": f"Unknown workflow: {name}", "available": sorted(WORKFLOW_RECIPES)})
    return {"name": name, **WORKFLOW_RECIPES[name]}


@app.post("/workflows/neighborhood", dependencies=[Depends(require_api_key)])
def workflow_neighborhood(request: NeighborhoodRequest) -> dict[str, Any]:
    top_k = clamp_limit(request.top_k, 10)
    neighbors = nearest_neighbors(
        NeighborsRequest(
            id=request.id,
            modality=request.modality,
            top_k=top_k,
            include_reverse=request.include_reverse,
        )
    )
    ids = entity_ids_from_rows(neighbors["results"], preferred_field="Match JCP2022")
    if request.include_query:
        ids = [request.id, *[id_ for id_ in ids if id_ != request.id]]

    response: dict[str, Any] = {
        "query_id": request.id,
        "modality": request.modality,
        "neighbor_count": len(neighbors["results"]),
        "ids": ids,
        "neighbors": neighbors["results"],
    }
    response["entities"] = entity_summary(IdsRequest(ids=ids, include_activity=request.include_activity, config=request.config))
    if request.include_features:
        response["features"] = interpretable_features(FeatureRequest(ids=ids, modality=request.modality, limit=min(MAX_LIMIT, max(100, len(ids) * 20))))
    if request.include_gallery:
        response["gallery"] = gallery_images(GalleryRequest(ids=ids, modality=request.modality, limit=min(100, max(20, len(ids) * 5))))
    if request.annotation_tables:
        response["annotations"] = {
            table: annotations(AnnotationRequest(table=table, ids=ids, limit=min(MAX_LIMIT, max(100, len(ids) * 20))))
            for table in request.annotation_tables
        }
    return response


@app.post("/metadata/summary", dependencies=[Depends(require_api_key)])
def metadata_summary(request: MetadataSummaryRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    require_allowed_table(request.database, request.table)
    validation_columns = [*request.group_by, *request.filters.keys()]
    if request.count_distinct:
        validation_columns.append(request.count_distinct)
    require_columns(request.database, request.table, validation_columns)

    table_id = quote_ident(request.table)
    where = ["1=1"]
    params: list[Any] = []
    for column, values in request.filters.items():
        where.append(f"{quote_ident(column)} = ANY(?)")
        params.append(values)

    group_exprs = [quote_ident(column) for column in request.group_by]
    count_expr = f"COUNT(DISTINCT {quote_ident(request.count_distinct)})" if request.count_distinct else "COUNT(*)"
    select_exprs = [*group_exprs, f"{count_expr} AS n"]
    group_sql = f"GROUP BY {', '.join(group_exprs)}" if group_exprs else ""
    order_sql = "ORDER BY n DESC"
    params.extend([limit, max(0, request.offset)])
    rows = query_db(
        db_path(request.database),
        f"""
        SELECT {', '.join(select_exprs)}
        FROM {table_id}
        WHERE {' AND '.join(where)}
        {group_sql}
        {order_sql}
        LIMIT ? OFFSET ?
        """,
        params,
    )
    return {
        "database": request.database,
        "table": request.table,
        "group_by": request.group_by,
        "count_distinct": request.count_distinct,
        "filters": request.filters,
        "results": rows,
        "limit": limit,
        "offset": request.offset,
    }


@app.post("/activity/compare", dependencies=[Depends(require_api_key)])
def activity_compare(request: ActivityCompareRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    id_filter = "AND Metadata_JCP2022 = ANY(?)" if request.ids else ""
    left = request.left
    right = request.right
    base_params: list[Any] = [
        left.dataset,
        left.preprocessing,
        left.filter,
        left.activity_params,
    ]
    if request.ids:
        base_params.append(request.ids)
    base_params.extend([right.dataset, right.preprocessing, right.filter, right.activity_params])
    if request.ids:
        base_params.append(request.ids)

    joined_cte = f"""
        WITH left_run AS (
          SELECT Metadata_JCP2022, COALESCE(CAST(Metadata_Source AS VARCHAR), '') AS source_key,
                 mean_normalized_average_precision AS left_nmap,
                 corrected_p_value AS left_p,
                 below_corrected_p AS left_active
          FROM activity_results
          WHERE {activity_where_sql()} {id_filter}
        ),
        right_run AS (
          SELECT Metadata_JCP2022, COALESCE(CAST(Metadata_Source AS VARCHAR), '') AS source_key,
                 mean_normalized_average_precision AS right_nmap,
                 corrected_p_value AS right_p,
                 below_corrected_p AS right_active
          FROM activity_results
          WHERE {activity_where_sql()} {id_filter}
        ),
        joined AS (
          SELECT
            COALESCE(l.Metadata_JCP2022, r.Metadata_JCP2022) AS Metadata_JCP2022,
            NULLIF(COALESCE(l.source_key, r.source_key), '') AS Metadata_Source,
            l.left_nmap, r.right_nmap,
            l.left_p, r.right_p,
            COALESCE(l.left_active, false) AS left_active,
            COALESCE(r.right_active, false) AS right_active,
            (r.right_nmap - l.left_nmap) AS delta_nmap
          FROM left_run l
          FULL OUTER JOIN right_run r
            ON l.Metadata_JCP2022 = r.Metadata_JCP2022
           AND l.source_key = r.source_key
        )
    """
    summary = query_db(
        COPAIRS_DB,
        joined_cte
        + """
        SELECT
          COUNT(*) AS n_pairs,
          SUM(CASE WHEN left_nmap IS NOT NULL AND right_nmap IS NOT NULL THEN 1 ELSE 0 END) AS n_common,
          SUM(CASE WHEN left_active THEN 1 ELSE 0 END) AS n_left_active,
          SUM(CASE WHEN right_active THEN 1 ELSE 0 END) AS n_right_active,
          SUM(CASE WHEN NOT left_active AND right_active THEN 1 ELSE 0 END) AS n_gained_active,
          SUM(CASE WHEN left_active AND NOT right_active THEN 1 ELSE 0 END) AS n_lost_active,
          AVG(delta_nmap) AS mean_delta_nmap,
          MEDIAN(delta_nmap) AS median_delta_nmap,
          corr(left_nmap, right_nmap) AS pearson_nmap
        FROM joined
        """,
        base_params,
    )[0]
    rows = query_db(
        COPAIRS_DB,
        joined_cte
        + """
        SELECT *
        FROM joined
        ORDER BY ABS(delta_nmap) DESC NULLS LAST, Metadata_JCP2022
        LIMIT ? OFFSET ?
        """,
        [*base_params, limit, max(0, request.offset)],
    )
    return {"left": left.model_dump(), "right": right.model_dump(), "summary": summary, "results": rows, "limit": limit, "offset": request.offset}


@app.post("/consistency/compare", dependencies=[Depends(require_api_key)])
def consistency_compare(request: ConsistencyCompareRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    left = request.left
    right = request.right
    group_filter = "AND group_value ILIKE ?" if request.group_value else ""
    sig_filter = "AND below_corrected_p" if request.significant_only else ""
    params: list[Any] = [
        left.dataset,
        left.preprocessing,
        left.filter,
        left.group_type,
        left.distance,
    ]
    if request.group_value:
        params.append(f"%{request.group_value}%")
    params.extend([right.dataset, right.preprocessing, right.filter, right.group_type, right.distance])
    if request.group_value:
        params.append(f"%{request.group_value}%")

    joined_cte = f"""
        WITH left_run AS (
          SELECT group_value, mean_normalized_average_precision AS left_nmap,
                 corrected_p_value AS left_p, below_corrected_p AS left_significant,
                 n_perturbations AS left_n_perturbations
          FROM consistency_results
          WHERE {consistency_where_sql()} AND _preprocessing NOT LIKE '%_sweep' {group_filter} {sig_filter}
        ),
        right_run AS (
          SELECT group_value, mean_normalized_average_precision AS right_nmap,
                 corrected_p_value AS right_p, below_corrected_p AS right_significant,
                 n_perturbations AS right_n_perturbations
          FROM consistency_results
          WHERE {consistency_where_sql()} AND _preprocessing NOT LIKE '%_sweep' {group_filter} {sig_filter}
        ),
        joined AS (
          SELECT
            COALESCE(l.group_value, r.group_value) AS group_value,
            l.left_nmap, r.right_nmap,
            l.left_p, r.right_p,
            COALESCE(l.left_significant, false) AS left_significant,
            COALESCE(r.right_significant, false) AS right_significant,
            l.left_n_perturbations, r.right_n_perturbations,
            (r.right_nmap - l.left_nmap) AS delta_nmap
          FROM left_run l
          FULL OUTER JOIN right_run r USING (group_value)
        )
    """
    summary = query_db(
        COPAIRS_DB,
        joined_cte
        + """
        SELECT
          COUNT(*) AS n_groups,
          SUM(CASE WHEN left_nmap IS NOT NULL AND right_nmap IS NOT NULL THEN 1 ELSE 0 END) AS n_common,
          SUM(CASE WHEN left_significant THEN 1 ELSE 0 END) AS n_left_significant,
          SUM(CASE WHEN right_significant THEN 1 ELSE 0 END) AS n_right_significant,
          SUM(CASE WHEN NOT left_significant AND right_significant THEN 1 ELSE 0 END) AS n_gained_significant,
          SUM(CASE WHEN left_significant AND NOT right_significant THEN 1 ELSE 0 END) AS n_lost_significant,
          AVG(delta_nmap) AS mean_delta_nmap,
          MEDIAN(delta_nmap) AS median_delta_nmap,
          corr(left_nmap, right_nmap) AS pearson_nmap
        FROM joined
        """,
        params,
    )[0]
    rows = query_db(
        COPAIRS_DB,
        joined_cte
        + """
        SELECT *
        FROM joined
        ORDER BY ABS(delta_nmap) DESC NULLS LAST, group_value
        LIMIT ? OFFSET ?
        """,
        [*params, limit, max(0, request.offset)],
    )
    return {"left": left.model_dump(), "right": right.model_dump(), "summary": summary, "results": rows, "limit": limit, "offset": request.offset}


@app.post("/consistency/sweep", dependencies=[Depends(require_api_key)])
def consistency_sweep(request: ConsistencySweepRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    where = [
        "_dataset = ?",
        "_preprocessing = ?",
        "_filter = ?",
        "_group_type = ?",
        "_distance = ?",
        "_preprocessing LIKE '%_sweep'",
    ]
    params: list[Any] = [request.dataset, request.preprocessing, request.filter, request.group_type, request.distance]
    if request.thresholds:
        where.append("_activity_threshold = ANY(?)")
        params.append(request.thresholds)
    if request.group_value:
        where.append("group_value ILIKE ?")
        params.append(f"%{request.group_value}%")
    if request.significant_only:
        where.append("below_corrected_p")

    summary = query_db(
        COPAIRS_DB,
        f"""
        SELECT
          _activity_threshold AS activity_threshold,
          COUNT(*) AS n_groups,
          SUM(CASE WHEN below_corrected_p THEN 1 ELSE 0 END) AS n_significant,
          AVG(mean_normalized_average_precision) AS mean_nmap,
          MEDIAN(mean_normalized_average_precision) AS median_nmap
        FROM consistency_results
        WHERE {' AND '.join(where)}
        GROUP BY _activity_threshold
        ORDER BY _activity_threshold
        """,
        params,
    )
    rows = query_db(
        COPAIRS_DB,
        f"""
        SELECT
          group_value, _activity_threshold AS activity_threshold,
          mean_average_precision, mean_normalized_average_precision,
          p_value, corrected_p_value, below_p, below_corrected_p,
          n_perturbations, _dataset, _preprocessing, _filter, _group_type, _distance
        FROM consistency_results
        WHERE {' AND '.join(where)}
        ORDER BY _activity_threshold, mean_normalized_average_precision DESC
        LIMIT ? OFFSET ?
        """,
        [*params, limit, max(0, request.offset)],
    )
    return {"request": request.model_dump(), "summary": summary, "results": rows, "limit": limit, "offset": request.offset}


@app.post("/annotations/coverage", dependencies=[Depends(require_api_key)])
def annotation_coverage(request: AnnotationCoverageRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    sources = request.sources or list(ANNOTATION_SOURCE_TABLES)
    if request.group_by == "compound_source":
        base_sql = """
            SELECT Metadata_JCP2022, Metadata_Compound_Source AS group_value
            FROM compound_source
        """
    elif request.group_by == "imaging_source":
        base_sql = """
            SELECT DISTINCT w.Metadata_JCP2022, w.Metadata_Source AS group_value
            FROM well w
            JOIN perturbation p USING (Metadata_JCP2022)
            WHERE p.Metadata_perturbation_modality = 'compound'
        """
    else:
        base_sql = """
            SELECT Metadata_JCP2022, '__all__' AS group_value
            FROM compound
        """

    source_ctes = []
    source_counts = []
    union_parts = []
    for source in sources:
        table = ANNOTATION_SOURCE_TABLES[source]
        source_ctes.append(f"{source}_annotated AS (SELECT DISTINCT Metadata_JCP2022 FROM {quote_ident(table)})")
        source_counts.append(
            f"COUNT(DISTINCT CASE WHEN {source}_annotated.Metadata_JCP2022 IS NOT NULL THEN base.Metadata_JCP2022 END) AS n_{source}"
        )
        union_parts.append(f"SELECT Metadata_JCP2022 FROM {source}_annotated")

    joins = "\n".join(
        f"LEFT JOIN {source}_annotated ON {source}_annotated.Metadata_JCP2022 = base.Metadata_JCP2022"
        for source in sources
    )
    rows = query_db(
        METADATA_DB,
        f"""
        WITH base AS ({base_sql}),
        {', '.join(source_ctes)},
        any_annotated AS ({' UNION '.join(union_parts)})
        SELECT
          base.group_value,
          COUNT(DISTINCT base.Metadata_JCP2022) AS n_compounds,
          COUNT(DISTINCT CASE WHEN any_annotated.Metadata_JCP2022 IS NOT NULL THEN base.Metadata_JCP2022 END) AS n_any_annotation,
          {', '.join(source_counts)}
        FROM base
        LEFT JOIN any_annotated ON any_annotated.Metadata_JCP2022 = base.Metadata_JCP2022
        {joins}
        GROUP BY base.group_value
        ORDER BY n_compounds DESC
        LIMIT ? OFFSET ?
        """,
        [limit, max(0, request.offset)],
    )
    for row in rows:
        total = row.get("n_compounds") or 0
        row["pct_any_annotation"] = (row.get("n_any_annotation", 0) / total) if total else None
        for source in sources:
            key = f"n_{source}"
            row[f"pct_{source}"] = (row.get(key, 0) / total) if total else None
    return {"sources": sources, "group_by": request.group_by, "results": rows, "limit": limit, "offset": request.offset}


@app.post("/annotations/dark-matter", dependencies=[Depends(require_api_key)])
def dark_matter(request: DarkMatterRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    groups = request.annotation_groups or list(TARGET_ANNOTATION_COLUMNS)
    missing_clauses = [
        f"(cm.{quote_ident(TARGET_ANNOTATION_COLUMNS[group])} IS NULL OR cm.{quote_ident(TARGET_ANNOTATION_COLUMNS[group])} IN ('', 'NA'))"
        for group in groups
    ]
    cfg = request.config
    where = [
        "ar._dataset = ?",
        "ar._preprocessing = ?",
        "ar._filter = ?",
        "ar._activity_params = ?",
        "ar.below_corrected_p",
        *missing_clauses,
    ]
    params = [cfg.dataset, cfg.preprocessing, cfg.filter, cfg.activity_params]
    base_sql = f"""
        FROM compound_metadata cm
        JOIN copairs.activity_results ar ON ar.Metadata_JCP2022 = cm.Metadata_JCP2022
        WHERE {' AND '.join(where)}
    """
    summary = query_metadata_with_copairs(
        f"""
        SELECT COUNT(DISTINCT cm.Metadata_JCP2022) AS n_dark_active,
               AVG(ar.mean_normalized_average_precision) AS mean_nmap,
               MIN(ar.corrected_p_value) AS min_corrected_p_value
        {base_sql}
        """,
        params,
    )[0]
    rows = query_metadata_with_copairs(
        f"""
        SELECT
          cm.Metadata_JCP2022, cm.Metadata_repurposing_name, cm.Metadata_SMILES,
          cm.Metadata_InChIKey, cm.Metadata_MW, cm.Metadata_LogP,
          ar.mean_average_precision, ar.mean_normalized_average_precision,
          ar.corrected_p_value, ar.below_corrected_p
        {base_sql}
        ORDER BY ar.mean_normalized_average_precision DESC
        LIMIT ? OFFSET ?
        """,
        [*params, limit, max(0, request.offset)],
    )
    return {"config": cfg.model_dump(), "annotation_groups": groups, "summary": summary, "results": rows, "limit": limit, "offset": request.offset}


@app.post("/structure/scaffolds", dependencies=[Depends(require_api_key)])
def scaffold_series(request: ScaffoldRequest) -> dict[str, Any]:
    limit = clamp_limit(request.limit, 100)
    cfg = request.config
    if request.scaffold or request.ids:
        scaffolds: list[str] = [request.scaffold] if request.scaffold else []
        if request.ids:
            scaffold_rows = query_db(
                METADATA_DB,
                """
                SELECT DISTINCT Metadata_MurckoScaffold
                FROM compound_metadata
                WHERE Metadata_JCP2022 = ANY(?)
                  AND Metadata_MurckoScaffold IS NOT NULL
                  AND Metadata_MurckoScaffold != ''
                """,
                [request.ids],
            )
            scaffolds.extend(row["Metadata_MurckoScaffold"] for row in scaffold_rows if row.get("Metadata_MurckoScaffold"))
        scaffolds = list(dict.fromkeys(scaffolds))
        if not scaffolds:
            return {"mode": "series", "scaffolds": [], "results": [], "limit": limit, "offset": request.offset}
        active_filter = "AND ar.below_corrected_p" if request.active_only else ""
        rows = query_metadata_with_copairs(
            f"""
            SELECT
              cm.Metadata_MurckoScaffold, cm.Metadata_JCP2022, cm.Metadata_repurposing_name,
              cm.Metadata_SMILES, cm.Metadata_repurposing_target, cm.Metadata_repurposing_moa,
              cm.Metadata_MW, cm.Metadata_LogP,
              ar.mean_normalized_average_precision, ar.corrected_p_value, ar.below_corrected_p
            FROM compound_metadata cm
            LEFT JOIN copairs.activity_results ar
              ON ar.Metadata_JCP2022 = cm.Metadata_JCP2022
             AND ar._dataset = ? AND ar._preprocessing = ? AND ar._filter = ? AND ar._activity_params = ?
            WHERE cm.Metadata_MurckoScaffold = ANY(?)
              {active_filter}
            ORDER BY cm.Metadata_MurckoScaffold, ar.mean_normalized_average_precision DESC NULLS LAST, cm.Metadata_JCP2022
            LIMIT ? OFFSET ?
            """,
            [cfg.dataset, cfg.preprocessing, cfg.filter, cfg.activity_params, scaffolds, limit, max(0, request.offset)],
        )
        return {"mode": "series", "config": cfg.model_dump(), "scaffolds": scaffolds, "results": rows, "limit": limit, "offset": request.offset}

    active_having = "AND SUM(CASE WHEN ar.below_corrected_p THEN 1 ELSE 0 END) > 0" if request.active_only else ""
    rows = query_metadata_with_copairs(
        f"""
        SELECT
          cm.Metadata_MurckoScaffold,
          COUNT(DISTINCT cm.Metadata_JCP2022) AS n_compounds,
          SUM(CASE WHEN ar.below_corrected_p THEN 1 ELSE 0 END) AS n_active,
          SUM(CASE WHEN ar.below_corrected_p THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(DISTINCT cm.Metadata_JCP2022), 0) AS active_rate,
          AVG(ar.mean_normalized_average_precision) AS mean_nmap,
          STDDEV_SAMP(ar.mean_normalized_average_precision) AS sd_nmap,
          MIN(ar.mean_normalized_average_precision) AS min_nmap,
          MAX(ar.mean_normalized_average_precision) AS max_nmap
        FROM compound_metadata cm
        LEFT JOIN copairs.activity_results ar
          ON ar.Metadata_JCP2022 = cm.Metadata_JCP2022
         AND ar._dataset = ? AND ar._preprocessing = ? AND ar._filter = ? AND ar._activity_params = ?
        WHERE cm.Metadata_MurckoScaffold IS NOT NULL AND cm.Metadata_MurckoScaffold != ''
        GROUP BY cm.Metadata_MurckoScaffold
        HAVING COUNT(DISTINCT cm.Metadata_JCP2022) >= ? {active_having}
        ORDER BY n_active DESC NULLS LAST, sd_nmap DESC NULLS LAST, n_compounds DESC
        LIMIT ? OFFSET ?
        """,
        [cfg.dataset, cfg.preprocessing, cfg.filter, cfg.activity_params, request.min_size, limit, max(0, request.offset)],
    )
    return {"mode": "summary", "config": cfg.model_dump(), "min_size": request.min_size, "results": rows, "limit": limit, "offset": request.offset}


def execute_compose_step(name: str, params: dict[str, Any]) -> dict[str, Any]:
    if name not in COMPOSE_STEP_BUILDERS:
        raise HTTPException(status_code=400, detail={"message": f"Unsupported compose step: {name}", "allowed": sorted(COMPOSE_STEP_BUILDERS)})
    return COMPOSE_STEP_BUILDERS[name](params)


@app.post("/workflows/compose", dependencies=[Depends(require_api_key)])
def workflow_compose(request: WorkflowComposeRequest) -> dict[str, Any]:
    outputs: dict[str, Any] = {}
    ordered = []
    for index, step in enumerate(request.steps):
        params = dict(step.params)
        if step.ids_from:
            if step.ids_from.step not in outputs:
                raise HTTPException(status_code=400, detail=f"ids_from step not found: {step.ids_from.step}")
            ids = entity_ids_from_rows(outputs[step.ids_from.step], preferred_field=step.ids_from.field)
            if not ids:
                raise HTTPException(status_code=400, detail=f"No JCP2022 IDs found in step: {step.ids_from.step}")
            if step.ids_from.target == "id":
                params["id"] = params.get("id") or ids[0]
            else:
                existing = params.get("ids", []) if step.ids_from.include_existing else []
                params["ids"] = list(dict.fromkeys([*existing, *ids]))
        result = execute_compose_step(step.name, params)
        key = step.save_as or f"{index}_{step.name}"
        outputs[key] = result
        ordered.append({"step": step.name, "key": key})
    return {"steps": ordered, "results": outputs}


def _resolve_step(params: dict[str, Any]) -> dict[str, Any]:
    q = params.get("q") or params.get("query")
    if not q:
        raise HTTPException(status_code=400, detail="resolve step requires q")
    return resolve(q=str(q), limit=int(params.get("limit", 20)))


@app.post("/memos", dependencies=[Depends(require_api_key)])
def submit_memo(request: MemoSubmitRequest) -> dict[str, Any]:
    created_at = utc_now_iso()
    record = {
        "id": f"memo_{created_at[:10]}_{uuid4().hex[:12]}",
        "created_at": created_at,
        **request.model_dump(),
    }
    record["tags"] = safe_tags(record.get("tags", []))
    path = memo_date_path(created_at)
    try:
        MEMOS_DIR.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
    except OSError as exc:
        raise HTTPException(status_code=503, detail=f"Unable to write memo: {exc}") from exc
    return {"memo": record, "storage": {"kind": "jsonl", "path": str(path)}}


@app.get("/memos", dependencies=[Depends(require_api_key)])
def list_memos(
    category: Optional[Literal["missing_primitive", "bug", "data_gap", "documentation", "workflow_request", "other"]] = None,
    priority: Optional[Literal["low", "normal", "high"]] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    limit = clamp_limit(limit, 100)
    records = []
    if MEMOS_DIR.exists():
        for path in sorted(MEMOS_DIR.glob("*.jsonl"), reverse=True):
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if category and record.get("category") != category:
                        continue
                    if priority and record.get("priority") != priority:
                        continue
                    records.append(record)
    records.sort(key=lambda record: record.get("created_at", ""), reverse=True)
    return {
        "memo_dir": str(MEMOS_DIR),
        "category": category,
        "priority": priority,
        "total": len(records),
        "results": records[max(0, offset): max(0, offset) + limit],
        "limit": limit,
        "offset": offset,
    }


COMPOSE_STEP_BUILDERS = {
    "resolve": _resolve_step,
    "entities_summary": lambda params: entity_summary(IdsRequest(**params)),
    "entity_summary": lambda params: entity_summary(IdsRequest(**params)),
    "search_entities": lambda params: search_entities(SearchRequest(**params)),
    "activity": lambda params: activity(ActivityRequest(**params)),
    "activity_summary": lambda params: activity_summary(RunConfig(**params)),
    "activity_compare": lambda params: activity_compare(ActivityCompareRequest(**params)),
    "consistency": lambda params: consistency(ConsistencyRequest(**params)),
    "consistency_summary": lambda params: consistency_summary(ConsistencyConfig(**params)),
    "consistency_compare": lambda params: consistency_compare(ConsistencyCompareRequest(**params)),
    "consistency_sweep": lambda params: consistency_sweep(ConsistencySweepRequest(**params)),
    "chemical_properties": lambda params: chemical_properties(ChemicalPropertiesRequest(**params)),
    "similarity_neighbors": lambda params: nearest_neighbors(NeighborsRequest(**params)),
    "pairwise_similarity": lambda params: pairwise_similarity(PairwiseRequest(**params)),
    "features_interpretable": lambda params: interpretable_features(FeatureRequest(**params)),
    "gallery_images": lambda params: gallery_images(GalleryRequest(**params)),
    "annotations": lambda params: annotations(AnnotationRequest(**params)),
    "well_cell_counts": lambda params: well_cell_counts(WellRequest(**params)),
    "metadata_summary": lambda params: metadata_summary(MetadataSummaryRequest(**params)),
    "annotation_coverage": lambda params: annotation_coverage(AnnotationCoverageRequest(**params)),
    "dark_matter": lambda params: dark_matter(DarkMatterRequest(**params)),
    "scaffold_series": lambda params: scaffold_series(ScaffoldRequest(**params)),
    "workflow_neighborhood": lambda params: workflow_neighborhood(NeighborhoodRequest(**params)),
    "submit_memo": lambda params: submit_memo(MemoSubmitRequest(**params)),
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
        "memo_dir": file_info(MEMOS_DIR),
        "default_configs": {
            "activity": RunConfig().model_dump(),
            "consistency": ConsistencyConfig().model_dump(),
        },
    }


PUBLIC_COMPOSE_STEP_BUILDERS = {
    name: builder
    for name, builder in COMPOSE_STEP_BUILDERS.items()
    if name != "submit_memo"
}


def workflow_catalog_public() -> dict[str, Any]:
    catalog = workflow_catalog()
    catalog["recipes"] = [
        recipe for recipe in catalog["recipes"]
        if recipe["name"] != "memo_inbox"
    ]
    catalog["compose"]["allowed_steps"] = sorted(PUBLIC_COMPOSE_STEP_BUILDERS)
    catalog["compose"]["security_model"] = (
        "Public MCP composition of allowlisted read-oriented primitives only; "
        "arbitrary Python execution and memo writes are intentionally not supported."
    )
    return catalog


def workflow_detail_public(request: WorkflowDetailMcpRequest) -> dict[str, Any]:
    if request.name == "memo_inbox":
        raise HTTPException(status_code=404, detail="memo_inbox is not exposed through public MCP")
    return workflow_detail(request.name)


def workflow_compose_public(request: WorkflowComposeRequest) -> dict[str, Any]:
    outputs: dict[str, Any] = {}
    ordered = []
    for index, step in enumerate(request.steps):
        if step.name not in PUBLIC_COMPOSE_STEP_BUILDERS:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": f"Unsupported public MCP compose step: {step.name}",
                    "allowed": sorted(PUBLIC_COMPOSE_STEP_BUILDERS),
                },
            )
        params = dict(step.params)
        if step.ids_from:
            if step.ids_from.step not in outputs:
                raise HTTPException(status_code=400, detail=f"ids_from step not found: {step.ids_from.step}")
            ids = entity_ids_from_rows(outputs[step.ids_from.step], preferred_field=step.ids_from.field)
            if not ids:
                raise HTTPException(status_code=400, detail=f"No JCP2022 IDs found in step: {step.ids_from.step}")
            if step.ids_from.target == "id":
                params["id"] = params.get("id") or ids[0]
            else:
                existing = params.get("ids", []) if step.ids_from.include_existing else []
                params["ids"] = list(dict.fromkeys([*existing, *ids]))
        result = PUBLIC_COMPOSE_STEP_BUILDERS[step.name](params)
        key = step.save_as or f"{index}_{step.name}"
        outputs[key] = result
        ordered.append({"step": step.name, "key": key})
    return {"steps": ordered, "results": outputs}


def mcp_input_schema(model: type[BaseModel]) -> dict[str, Any]:
    schema = model.model_json_schema()
    schema.setdefault("type", "object")
    schema.setdefault("properties", {})
    return schema


def mcp_tool(
    name: str,
    description: str,
    input_model: type[BaseModel],
    handler: Callable[[BaseModel], dict[str, Any]],
) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "input_model": input_model,
        "handler": handler,
        "schema": mcp_input_schema(input_model),
    }


MCP_TOOLS: dict[str, dict[str, Any]] = {
    "health": mcp_tool(
        "health",
        "Check data-file and database availability for the JUMP Agent service.",
        EmptyMcpRequest,
        lambda request: health(),
    ),
    "list_datasets": mcp_tool(
        "list_datasets",
        "List available profile datasets, copairs activity configs, consistency configs, and JUMP Hub files.",
        EmptyMcpRequest,
        lambda request: datasets(),
    ),
    "describe_schema": mcp_tool(
        "describe_schema",
        "Describe DuckDB or Zenodo Parquet schemas.",
        SchemaMcpRequest,
        lambda request: schema(request.database, table=request.table, limit_columns=request.limit_columns),
    ),
    "resolve_entity": mcp_tool(
        "resolve_entity",
        "Resolve names, JCP IDs, SMILES, genes, targets, or MoAs to JUMP entities.",
        ResolveMcpRequest,
        lambda request: resolve(q=request.q, limit=request.limit),
    ),
    "get_entity_summary": mcp_tool(
        "get_entity_summary",
        "Return compound/gene metadata and optional activity calls for JCP2022 IDs or gene symbols.",
        IdsRequest,
        lambda request: entity_summary(request),
    ),
    "search_entities": mcp_tool(
        "search_entities",
        "Filter compounds by text, target, MoA, disease area, source, chemical properties, and activity.",
        SearchRequest,
        lambda request: search_entities(request),
    ),
    "get_activity": mcp_tool(
        "get_activity",
        "Return bounded copairs activity scores and significance calls.",
        ActivityRequest,
        lambda request: activity(request),
    ),
    "get_activity_summary": mcp_tool(
        "get_activity_summary",
        "Summarize activity calls for one production run configuration.",
        RunConfig,
        lambda request: activity_summary(request),
    ),
    "get_consistency": mcp_tool(
        "get_consistency",
        "Return target, MoA, or annotation-group consistency scores.",
        ConsistencyRequest,
        lambda request: consistency(request),
    ),
    "get_consistency_summary": mcp_tool(
        "get_consistency_summary",
        "Summarize consistency significance by annotation source and distance metric.",
        ConsistencyConfig,
        lambda request: consistency_summary(request),
    ),
    "compare_configs": mcp_tool(
        "compare_configs",
        "Compare activity summaries across several run configurations.",
        CompareConfigsRequest,
        lambda request: compare_configs(request),
    ),
    "get_cross_source_reproducibility": mcp_tool(
        "get_cross_source_reproducibility",
        "Compare within-source and cross-source reproducibility for compounds.",
        CrossSourceRequest,
        lambda request: cross_source(request),
    ),
    "chemical_properties": mcp_tool(
        "chemical_properties",
        "Return processed RDKit-style chemical property rows.",
        ChemicalPropertiesRequest,
        lambda request: chemical_properties(request),
    ),
    "nearest_neighbors": mcp_tool(
        "nearest_neighbors",
        "Return nearest morphological neighbors from JUMP Hub similarity tables.",
        NeighborsRequest,
        lambda request: nearest_neighbors(request),
    ),
    "pairwise_similarity": mcp_tool(
        "pairwise_similarity",
        "Return exact pairwise cosine similarities for a small set of JCP2022 IDs.",
        PairwiseRequest,
        lambda request: pairwise_similarity(request),
    ),
    "interpretable_features": mcp_tool(
        "interpretable_features",
        "Return top interpretable CellProfiler feature rows for perturbations.",
        FeatureRequest,
        lambda request: interpretable_features(request),
    ),
    "gallery_images": mcp_tool(
        "gallery_images",
        "Return microscopy gallery image URL records for perturbations.",
        GalleryRequest,
        lambda request: gallery_images(request),
    ),
    "activity_cliffs": mcp_tool(
        "activity_cliffs",
        "Return bounded processed activity-cliff pair records.",
        ActivityCliffsRequest,
        lambda request: activity_cliffs(request),
    ),
    "profile_rows": mcp_tool(
        "profile_rows",
        "Return bounded profile rows and selected feature columns.",
        ProfileRowsRequest,
        lambda request: profile_rows(request),
    ),
    "profile_features": mcp_tool(
        "profile_features",
        "List metadata and feature columns for a profile dataset.",
        ProfileFeaturesMcpRequest,
        lambda request: profile_features(request.dataset, limit=request.limit),
    ),
    "annotations": mcp_tool(
        "annotations",
        "Return rows from allowlisted compound annotation tables.",
        AnnotationRequest,
        lambda request: annotations(request),
    ),
    "well_cell_counts": mcp_tool(
        "well_cell_counts",
        "Return well metadata joined to cell-count records.",
        WellRequest,
        lambda request: well_cell_counts(request),
    ),
    "source_summary": mcp_tool(
        "source_summary",
        "Summarize compound source coverage and activity rates.",
        SourceSummaryMcpRequest,
        lambda request: source_summary(
            dataset=request.dataset,
            preprocessing=request.preprocessing,
            filter=request.filter,
            activity_params=request.activity_params,
        ),
    ),
    "workflow_catalog": mcp_tool(
        "workflow_catalog",
        "List public composable workflow recipes and public MCP compose steps.",
        EmptyMcpRequest,
        lambda request: workflow_catalog_public(),
    ),
    "workflow_detail": mcp_tool(
        "workflow_detail",
        "Describe a public workflow recipe and its primitive sequence.",
        WorkflowDetailMcpRequest,
        lambda request: workflow_detail_public(request),
    ),
    "workflow_neighborhood": mcp_tool(
        "workflow_neighborhood",
        "Return a perturbation neighborhood with nearest profiles, metadata, activity, features, and gallery records.",
        NeighborhoodRequest,
        lambda request: workflow_neighborhood(request),
    ),
    "workflow_compose": mcp_tool(
        "workflow_compose",
        "Run a bounded public composition of read-oriented JumpAgent primitives.",
        WorkflowComposeRequest,
        lambda request: workflow_compose_public(request),
    ),
    "metadata_summary": mcp_tool(
        "metadata_summary",
        "Run bounded group-by summaries over allowlisted metadata or copairs tables.",
        MetadataSummaryRequest,
        lambda request: metadata_summary(request),
    ),
    "activity_compare": mcp_tool(
        "activity_compare",
        "Compare per-compound activity calls between two run configurations.",
        ActivityCompareRequest,
        lambda request: activity_compare(request),
    ),
    "consistency_compare": mcp_tool(
        "consistency_compare",
        "Compare target or MoA consistency results between two run configurations.",
        ConsistencyCompareRequest,
        lambda request: consistency_compare(request),
    ),
    "consistency_sweep": mcp_tool(
        "consistency_sweep",
        "Query activity-threshold sweep consistency results.",
        ConsistencySweepRequest,
        lambda request: consistency_sweep(request),
    ),
    "annotation_coverage": mcp_tool(
        "annotation_coverage",
        "Summarize compound annotation coverage overall or by compound/imaging source.",
        AnnotationCoverageRequest,
        lambda request: annotation_coverage(request),
    ),
    "dark_matter": mcp_tool(
        "dark_matter",
        "Find active compounds lacking selected target, MoA, or annotation groups.",
        DarkMatterRequest,
        lambda request: dark_matter(request),
    ),
    "scaffold_series": mcp_tool(
        "scaffold_series",
        "Summarize scaffold activity or list compounds in a scaffold series.",
        ScaffoldRequest,
        lambda request: scaffold_series(request),
    ),
    "artifact_search": mcp_tool(
        "artifact_search",
        "Find processed output files under the analysis artifact directory.",
        ArtifactSearchMcpRequest,
        lambda request: artifact_search(q=request.q, limit=request.limit),
    ),
    "artifact_read": mcp_tool(
        "artifact_read",
        "Read bounded JSON, CSV, TSV, Parquet, Markdown, or text processed artifacts.",
        ArtifactReadMcpRequest,
        lambda request: artifact_read(relative_path=request.relative_path, limit=request.limit, offset=request.offset),
    ),
    "provenance": mcp_tool(
        "provenance",
        "Return data paths, sizes, default configs, and service provenance.",
        EmptyMcpRequest,
        lambda request: provenance(),
    ),
}


def mcp_public_tool_descriptions() -> list[dict[str, Any]]:
    return [
        {
            "name": tool["name"],
            "description": tool["description"],
            "inputSchema": tool["schema"],
            "annotations": {
                "readOnlyHint": True,
                "destructiveHint": False,
                "idempotentHint": True,
                "openWorldHint": True,
            },
        }
        for tool in MCP_TOOLS.values()
    ]


def jsonrpc_result(request_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def jsonrpc_error(request_id: Any, code: int, message: str, data: Any = None) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": error}


def mcp_error_text(exc: Exception) -> str:
    if isinstance(exc, HTTPException):
        return f"HTTP {exc.status_code}: {exc.detail}"
    if isinstance(exc, ValidationError):
        return json.dumps(exc.errors(), default=str)
    return str(exc)


def mcp_tool_result(result: dict[str, Any], is_error: bool = False) -> dict[str, Any]:
    text = json.dumps(result, indent=2, default=str)
    if len(text) > MCP_MAX_TEXT_CHARS:
        text = (
            text[:MCP_MAX_TEXT_CHARS]
            + "\n... truncated by JUMP MCP; rerun with a smaller limit or higher offset."
        )
    response: dict[str, Any] = {"content": [{"type": "text", "text": text}]}
    if is_error:
        response["isError"] = True
    return response


def call_mcp_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    tool = MCP_TOOLS.get(name)
    if tool is None:
        raise KeyError(name)
    input_model = tool["input_model"]
    parsed = input_model(**arguments)
    return tool["handler"](parsed)


def handle_mcp_message(message: Any) -> Optional[dict[str, Any]]:
    if not isinstance(message, dict):
        return jsonrpc_error(None, -32600, "Invalid JSON-RPC request")

    request_id = message.get("id")
    is_notification = "id" not in message
    method = message.get("method")
    params = message.get("params") or {}
    if not isinstance(method, str):
        if is_notification:
            return None
        return jsonrpc_error(request_id, -32600, "JSON-RPC method is required")
    if not isinstance(params, dict):
        if is_notification:
            return None
        return jsonrpc_error(request_id, -32602, "JSON-RPC params must be an object")

    if method == "notifications/initialized":
        return None
    if is_notification:
        return None

    if method == "initialize":
        return jsonrpc_result(
            request_id,
            {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "jump-agent", "version": app.version},
                "instructions": (
                    "Public read-oriented MCP tools for querying processed JUMP Cell Painting "
                    "profiles, activity calls, annotations, nearest-neighbor morphology, "
                    "interpretable features, gallery image records, artifacts, and provenance."
                ),
            },
        )
    if method == "ping":
        return jsonrpc_result(request_id, {})
    if method == "tools/list":
        return jsonrpc_result(request_id, {"tools": mcp_public_tool_descriptions()})
    if method == "tools/call":
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if not isinstance(name, str):
            return jsonrpc_error(request_id, -32602, "tools/call requires a string tool name")
        if not isinstance(arguments, dict):
            return jsonrpc_error(request_id, -32602, "tools/call arguments must be an object")
        try:
            result = call_mcp_tool(name, arguments)
        except KeyError:
            return jsonrpc_error(request_id, -32602, f"Unknown tool: {name}", {"available": sorted(MCP_TOOLS)})
        except ValidationError as exc:
            return jsonrpc_error(
                request_id,
                -32602,
                "Invalid tool arguments",
                json.loads(json.dumps(exc.errors(), default=str)),
            )
        except Exception as exc:
            return jsonrpc_result(request_id, mcp_tool_result({"error": mcp_error_text(exc)}, is_error=True))
        return jsonrpc_result(request_id, mcp_tool_result(result))

    return jsonrpc_error(request_id, -32601, f"Method not found: {method}")


@app.post("/mcp")
async def mcp_endpoint(request: Request) -> Response:
    try:
        payload = await request.json()
    except Exception as exc:
        return JSONResponse(jsonrpc_error(None, -32700, "Parse error", str(exc)), status_code=400)

    if isinstance(payload, list):
        responses = [response for response in (handle_mcp_message(message) for message in payload) if response is not None]
        if not responses:
            return Response(status_code=202)
        return JSONResponse(responses)

    response = handle_mcp_message(payload)
    if response is None:
        return Response(status_code=202)
    return JSONResponse(response)
