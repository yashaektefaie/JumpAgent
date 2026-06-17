from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import duckdb

import jump_agent_api.app as api


class JumpAgentPrimitiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.production = self.root / "jump_production"
        self.zenodo = self.root / "jump_hub_zenodo"
        self.memos = self.root / "memos"
        (self.production / "interim").mkdir(parents=True)
        (self.production / "processed").mkdir(parents=True)
        (self.production / "profiles").mkdir(parents=True)
        self.zenodo.mkdir(parents=True)

        self.metadata_db = self.production / "interim" / "jump_metadata_augmented.duckdb"
        self.copairs_db = self.production / "processed" / "copairs_results.duckdb"
        self._build_metadata_db()
        self._build_copairs_db()
        self._build_zenodo_parquets()

        self.old_paths = (
            api.DATA_ROOT,
            api.PRODUCTION_ROOT,
            api.ZENODO_ROOT,
            api.INTERIM_DIR,
            api.METADATA_DB,
            api.COPAIRS_DB,
            api.PROFILES_DIR,
            api.PROCESSED_DIR,
            api.MEMOS_DIR,
        )
        api.DATA_ROOT = self.root
        api.PRODUCTION_ROOT = self.production
        api.ZENODO_ROOT = self.zenodo
        api.INTERIM_DIR = self.production / "interim"
        api.METADATA_DB = self.metadata_db
        api.COPAIRS_DB = self.copairs_db
        api.PROFILES_DIR = self.production / "profiles"
        api.PROCESSED_DIR = self.production / "processed"
        api.MEMOS_DIR = self.memos
        api._MATRIX_COLUMNS_CACHE.clear()

    def tearDown(self) -> None:
        (
            api.DATA_ROOT,
            api.PRODUCTION_ROOT,
            api.ZENODO_ROOT,
            api.INTERIM_DIR,
            api.METADATA_DB,
            api.COPAIRS_DB,
            api.PROFILES_DIR,
            api.PROCESSED_DIR,
            api.MEMOS_DIR,
        ) = self.old_paths
        api._MATRIX_COLUMNS_CACHE.clear()
        self.tmp.cleanup()

    def _build_metadata_db(self) -> None:
        con = duckdb.connect(str(self.metadata_db))
        con.execute(
            """
            CREATE TABLE compound (
                Metadata_JCP2022 VARCHAR,
                Metadata_InChIKey VARCHAR,
                Metadata_InChI VARCHAR,
                Metadata_SMILES VARCHAR
            )
            """
        )
        con.execute(
            """
            INSERT INTO compound VALUES
              ('JCP2022_A', 'AAAAAAAAAAAAAA-BBBBBBBBBB-C', 'InChI=A', 'CCO'),
              ('JCP2022_B', 'BBBBBBBBBBBBBB-CCCCCCCCCC-D', 'InChI=B', 'CCN'),
              ('JCP2022_C', 'CCCCCCCCCCCCCC-DDDDDDDDDD-E', 'InChI=C', 'CCC')
            """
        )
        con.execute(
            """
            CREATE TABLE compound_metadata AS
            SELECT
              Metadata_JCP2022,
              Metadata_InChIKey,
              Metadata_InChI,
              Metadata_SMILES,
              CASE WHEN Metadata_JCP2022 = 'JCP2022_A' THEN 'annotated-a' ELSE NULL END AS Metadata_repurposing_name,
              CASE WHEN Metadata_JCP2022 = 'JCP2022_A' THEN 'MAPK1' ELSE NULL END AS Metadata_repurposing_target,
              CASE WHEN Metadata_JCP2022 = 'JCP2022_A' THEN 'kinase inhibitor' ELSE NULL END AS Metadata_repurposing_moa,
              NULL::VARCHAR AS Metadata_repurposing_disease_area,
              NULL::VARCHAR AS Metadata_repurposing_clinical_phase,
              NULL::VARCHAR AS Metadata_repurposing_indication,
              CASE WHEN Metadata_JCP2022 = 'JCP2022_A' THEN 'P28482' ELSE NULL END AS Metadata_Uniprot_target,
              NULL::VARCHAR AS Metadata_txcst_active_assays,
              NULL::VARCHAR AS Metadata_chmprb_target_genes,
              NULL::VARCHAR AS Metadata_motive_gene_biokg,
              NULL::VARCHAR AS Metadata_motive_gene_opentargets,
              NULL::VARCHAR AS Metadata_motive_gene_primekg,
              100::INTEGER AS Metadata_median_cell_count,
              CASE WHEN Metadata_JCP2022 = 'JCP2022_C' THEN 310.0 ELSE 250.0 END AS Metadata_MW,
              CASE WHEN Metadata_JCP2022 = 'JCP2022_C' THEN 4.1 ELSE 2.3 END AS Metadata_LogP,
              40.0 AS Metadata_TPSA,
              1::INTEGER AS Metadata_HBD,
              2::INTEGER AS Metadata_HBA,
              3::INTEGER AS Metadata_RotatableBonds,
              1::INTEGER AS Metadata_NumRings,
              20::INTEGER AS Metadata_NumHeavyAtoms,
              1::INTEGER AS Metadata_NumAromaticRings,
              0.3 AS Metadata_FractionCSP3,
              0::INTEGER AS Metadata_Lipinski_Violations,
              0.7 AS Metadata_QED,
              CASE WHEN Metadata_JCP2022 IN ('JCP2022_A', 'JCP2022_B') THEN 'c1ccccc1' ELSE 'C1CCCCC1' END AS Metadata_MurckoScaffold,
              false AS Metadata_HasPAINS,
              true AS Metadata_ValidMol
            FROM compound
            """
        )
        con.execute(
            """
            CREATE TABLE gene_metadata (
                Metadata_JCP2022 VARCHAR,
                Metadata_perturbation_modality VARCHAR,
                Metadata_NCBI_Gene_ID VARCHAR,
                Metadata_Symbol VARCHAR,
                Metadata_Gene_Description VARCHAR,
                Metadata_Taxon_ID VARCHAR
            )
            """
        )
        con.execute("INSERT INTO gene_metadata VALUES ('JCP2022_G', 'crispr', '5594', 'MAPK1', 'mitogen activated protein kinase 1', '9606')")
        con.execute("CREATE TABLE compound_source (Metadata_JCP2022 VARCHAR, Metadata_Compound_Source VARCHAR)")
        con.execute("INSERT INTO compound_source VALUES ('JCP2022_A', 'source_1'), ('JCP2022_B', 'source_1'), ('JCP2022_C', 'source_2')")
        con.execute("CREATE TABLE perturbation (Metadata_JCP2022 VARCHAR, Metadata_perturbation_modality VARCHAR)")
        con.execute("INSERT INTO perturbation VALUES ('JCP2022_A', 'compound'), ('JCP2022_B', 'compound'), ('JCP2022_C', 'compound')")
        con.execute("CREATE TABLE well (Metadata_Source VARCHAR, Metadata_Plate VARCHAR, Metadata_Well VARCHAR, Metadata_JCP2022 VARCHAR)")
        con.execute("INSERT INTO well VALUES ('source_1', 'P1', 'A01', 'JCP2022_A'), ('source_1', 'P1', 'A02', 'JCP2022_B'), ('source_2', 'P2', 'A01', 'JCP2022_C')")
        con.execute("CREATE TABLE cell_counts (Metadata_Source VARCHAR, Metadata_Plate VARCHAR, Metadata_Well VARCHAR, Metadata_Batch VARCHAR, Metadata_Count_Cells INTEGER)")
        con.execute("INSERT INTO cell_counts VALUES ('source_1', 'P1', 'A01', 'B1', 101), ('source_1', 'P1', 'A02', 'B1', 99)")
        con.execute("CREATE TABLE compound_properties AS SELECT Metadata_JCP2022, Metadata_MW, Metadata_LogP, Metadata_TPSA, Metadata_MurckoScaffold, Metadata_HasPAINS, Metadata_ValidMol FROM compound_metadata")
        con.execute("CREATE TABLE repurposing_hub_annotations AS SELECT Metadata_JCP2022, Metadata_repurposing_name, Metadata_repurposing_target, Metadata_repurposing_moa, Metadata_repurposing_disease_area FROM compound_metadata WHERE Metadata_JCP2022 = 'JCP2022_A'")
        con.execute("CREATE TABLE chembl_protein_targets AS SELECT Metadata_JCP2022, Metadata_Uniprot_target FROM compound_metadata WHERE Metadata_JCP2022 = 'JCP2022_A'")
        for table in ["chemical_probes", "kinase_probes", "mitotox_annotations", "toxicity_pk_annotations", "toxcast_active_assays", "toxcast_annotations", "motive_annotations"]:
            con.execute(f"CREATE TABLE {table} (Metadata_JCP2022 VARCHAR)")
        con.close()

    def _build_copairs_db(self) -> None:
        con = duckdb.connect(str(self.copairs_db))
        con.execute(
            """
            CREATE TABLE activity_results (
                Metadata_JCP2022 VARCHAR,
                Metadata_Source VARCHAR,
                mean_average_precision DOUBLE,
                mean_normalized_average_precision DOUBLE,
                p_value DOUBLE,
                corrected_p_value DOUBLE,
                below_p BOOLEAN,
                below_corrected_p BOOLEAN,
                _dataset VARCHAR,
                _columns VARCHAR,
                _preprocessing VARCHAR,
                _filter VARCHAR,
                _activity_params VARCHAR
            )
            """
        )
        con.execute(
            """
            INSERT INTO activity_results VALUES
              ('JCP2022_A', NULL, 0.9, 0.80, 0.001, 0.01, true, true, 'compound_no_source7', 'feat_all', 'activity_no_target2', 'all_sources', 'default'),
              ('JCP2022_B', NULL, 0.7, 0.40, 0.020, 0.20, true, false, 'compound_no_source7', 'feat_all', 'activity_no_target2', 'all_sources', 'default'),
              ('JCP2022_C', NULL, 0.8, 0.70, 0.005, 0.03, true, true, 'compound_no_source7', 'feat_all', 'activity_no_target2', 'all_sources', 'default'),
              ('JCP2022_A', NULL, 0.8, 0.60, 0.010, 0.08, true, true, 'compound_DL_CPCNN_no_source7', 'feat_all', 'activity_no_target2', 'all_sources', 'default'),
              ('JCP2022_B', NULL, 0.9, 0.75, 0.002, 0.02, true, true, 'compound_DL_CPCNN_no_source7', 'feat_all', 'activity_no_target2', 'all_sources', 'default')
            """
        )
        con.execute(
            """
            CREATE TABLE consistency_results (
                group_value VARCHAR,
                mean_average_precision DOUBLE,
                mean_normalized_average_precision DOUBLE,
                p_value DOUBLE,
                corrected_p_value DOUBLE,
                below_p BOOLEAN,
                below_corrected_p BOOLEAN,
                n_perturbations INTEGER,
                _dataset VARCHAR,
                _columns VARCHAR,
                _preprocessing VARCHAR,
                _filter VARCHAR,
                _group_type VARCHAR,
                _distance VARCHAR,
                _activity_threshold DOUBLE
            )
            """
        )
        con.execute(
            """
            INSERT INTO consistency_results VALUES
              ('MAPK1', 0.8, 0.70, 0.001, 0.01, true, true, 2, 'compound_no_source7', 'feat_all', 'consistency_no_target2', 'all_sources', 'repurposing', 'cosine', NULL),
              ('MAPK1', 0.7, 0.60, 0.002, 0.02, true, true, 2, 'compound_DL_CPCNN_no_source7', 'feat_all', 'consistency_no_target2', 'all_sources', 'repurposing', 'cosine', NULL),
              ('MAPK1', 0.8, 0.70, 0.001, 0.01, true, true, 2, 'compound_no_source7', 'feat_all', 'consistency_no_target2_sweep', 'all_sources', 'repurposing', 'cosine', 0.05),
              ('MAPK1', 0.82, 0.72, 0.001, 0.01, true, true, 2, 'compound_no_source7', 'feat_all', 'consistency_no_target2_sweep', 'all_sources', 'repurposing', 'cosine', 0.10)
            """
        )
        con.close()

    def _build_zenodo_parquets(self) -> None:
        con = duckdb.connect()
        compound_path = str(self.zenodo / "compound.parquet")
        matrix_path = str(self.zenodo / "compound_cosinesim_full.parquet")
        features_path = str(self.zenodo / "compound_interpretable_features.parquet")
        gallery_path = str(self.zenodo / "compound_gallery.parquet")
        con.execute(
            f"""
            COPY (
              SELECT 'Compound A' AS "Perturbation", 'Compound B' AS "Match", 0.91 AS "Perturbation-Match Similarity",
                     'JCP2022_A' AS "JCP2022", 'JCP2022_B' AS "Match JCP2022", 'B' AS "Synonyms",
                     0.01 AS "Corrected p-value", true AS "Phenotypic activity",
                     0.20 AS "Corrected p-value Match", false AS "Phenotypic activity Match",
                     'repurposing' AS "Match resources"
              UNION ALL
              SELECT 'Compound A', 'Compound C', 0.50, 'JCP2022_A', 'JCP2022_C', 'C', 0.01, true, 0.03, true, 'none'
            ) TO '{compound_path}' (FORMAT PARQUET)
            """
        )
        con.execute(
            f"""
            COPY (
              SELECT 1.0 AS "JCP2022_A", 0.91 AS "JCP2022_B", 0.50 AS "JCP2022_C"
              UNION ALL SELECT 0.91, 1.0, 0.20
              UNION ALL SELECT 0.50, 0.20, 1.0
            ) TO '{matrix_path}' (FORMAT PARQUET)
            """
        )
        con.execute(
            f"""
            COPY (
              SELECT 'JCP2022_A' AS "JCP2022", 'Cells_AreaShape_Area' AS feature, 1.5 AS "|Cohen's d|"
              UNION ALL SELECT 'JCP2022_B', 'Nuclei_Texture', 1.1
            ) TO '{features_path}' (FORMAT PARQUET)
            """
        )
        con.execute(
            f"""
            COPY (
              SELECT 'JCP2022_A' AS "JCP2022", 's3://example/a.png' AS url
              UNION ALL SELECT 'JCP2022_B', 's3://example/b.png'
            ) TO '{gallery_path}' (FORMAT PARQUET)
            """
        )
        con.close()

    def test_neighborhood_workflow_composes_neighbors_metadata_features_and_gallery(self) -> None:
        result = api.workflow_neighborhood(api.NeighborhoodRequest(id="JCP2022_A", top_k=2))
        self.assertEqual(result["ids"], ["JCP2022_A", "JCP2022_B", "JCP2022_C"])
        self.assertEqual(len(result["neighbors"]), 2)
        self.assertEqual(len(result["entities"]["compounds"]), 3)
        self.assertGreaterEqual(len(result["features"]["results"]), 2)
        self.assertGreaterEqual(len(result["gallery"]["results"]), 2)

    def test_metadata_summary_groups_allowlisted_tables(self) -> None:
        result = api.metadata_summary(
            api.MetadataSummaryRequest(table="compound_source", group_by=["Metadata_Compound_Source"])
        )
        self.assertEqual(result["results"][0]["Metadata_Compound_Source"], "source_1")
        self.assertEqual(result["results"][0]["n"], 2)

    def test_activity_compare_reports_gained_active_calls(self) -> None:
        result = api.activity_compare(api.ActivityCompareRequest())
        self.assertEqual(result["summary"]["n_common"], 2)
        self.assertEqual(result["summary"]["n_gained_active"], 1)
        self.assertTrue(any(row["Metadata_JCP2022"] == "JCP2022_B" for row in result["results"]))

    def test_consistency_sweep_returns_threshold_summary(self) -> None:
        result = api.consistency_sweep(api.ConsistencySweepRequest())
        thresholds = [row["activity_threshold"] for row in result["summary"]]
        self.assertEqual(thresholds, [0.05, 0.10])

    def test_annotation_coverage_and_dark_matter(self) -> None:
        coverage = api.annotation_coverage(api.AnnotationCoverageRequest(group_by="compound_source", sources=["repurposing", "chembl"]))
        self.assertEqual(coverage["results"][0]["group_value"], "source_1")
        dark = api.dark_matter(api.DarkMatterRequest())
        dark_ids = [row["Metadata_JCP2022"] for row in dark["results"]]
        self.assertEqual(dark_ids, ["JCP2022_C"])

    def test_scaffold_series_and_compose(self) -> None:
        scaffold = api.scaffold_series(api.ScaffoldRequest(ids=["JCP2022_A"]))
        self.assertEqual({row["Metadata_JCP2022"] for row in scaffold["results"]}, {"JCP2022_A", "JCP2022_B"})
        composed = api.workflow_compose(
            api.WorkflowComposeRequest(
                steps=[
                    api.WorkflowStep(
                        name="similarity_neighbors",
                        save_as="neighbors",
                        params={"id": "JCP2022_A", "modality": "compound", "top_k": 1},
                    ),
                    api.WorkflowStep(
                        name="entities_summary",
                        save_as="neighbor_entities",
                        ids_from=api.WorkflowIdsFrom(step="neighbors", field="Match JCP2022"),
                    ),
                ]
            )
        )
        self.assertEqual(composed["results"]["neighbor_entities"]["compounds"][0]["Metadata_JCP2022"], "JCP2022_B")

    def test_workflow_catalog_and_compose_reject_unknown_steps(self) -> None:
        catalog = api.workflow_catalog()
        self.assertIn("similarity_neighbors", catalog["compose"]["allowed_steps"])
        self.assertIn("arbitrary Python execution is intentionally not supported", catalog["compose"]["security_model"])
        with self.assertRaises(api.HTTPException) as raised:
            api.workflow_compose(api.WorkflowComposeRequest(steps=[api.WorkflowStep(name="run_python", params={"code": "print(1)"})]))
        self.assertEqual(raised.exception.status_code, 400)

    def test_submit_and_list_memos(self) -> None:
        submitted = api.submit_memo(
            api.MemoSubmitRequest(
                title="Need organelle-selective workflow",
                memo="Please add a primitive that compares feature subsets by organelle.",
                category="workflow_request",
                priority="high",
                agent="unit-test",
                tags=["organelle features", "workflow_request"],
            )
        )
        self.assertTrue(submitted["memo"]["id"].startswith("memo_"))
        self.assertEqual(submitted["memo"]["tags"], ["organelle-features", "workflow_request"])
        listed = api.list_memos(category="workflow_request", priority="high")
        self.assertEqual(listed["total"], 1)
        self.assertEqual(listed["results"][0]["title"], "Need organelle-selective workflow")

    def test_mcp_initialize_and_tool_list(self) -> None:
        initialized = api.handle_mcp_message({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
        self.assertEqual(initialized["result"]["serverInfo"]["name"], "jump-agent")
        self.assertIn("tools", initialized["result"]["capabilities"])

        listed = api.handle_mcp_message({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
        tool_names = {tool["name"] for tool in listed["result"]["tools"]}
        self.assertIn("resolve_entity", tool_names)
        self.assertIn("nearest_neighbors", tool_names)
        self.assertIn("workflow_compose", tool_names)
        self.assertNotIn("submit_memo", tool_names)
        self.assertNotIn("list_memos", tool_names)

    def test_mcp_tool_call_returns_json_content(self) -> None:
        response = api.handle_mcp_message(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "resolve_entity", "arguments": {"q": "JCP2022_A"}},
            }
        )
        content = response["result"]["content"][0]
        self.assertEqual(content["type"], "text")
        body = json.loads(content["text"])
        self.assertEqual(body["query"], "JCP2022_A")
        self.assertEqual(body["results"][0]["id"], "JCP2022_A")

    def test_public_mcp_compose_rejects_memo_write_step(self) -> None:
        response = api.handle_mcp_message(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "workflow_compose",
                    "arguments": {
                        "steps": [
                            {
                                "name": "submit_memo",
                                "params": {
                                    "title": "Public write",
                                    "memo": "This write should be rejected by public MCP.",
                                },
                            }
                        ]
                    },
                },
            }
        )
        self.assertTrue(response["result"]["isError"])
        self.assertIn("Unsupported public MCP compose step", response["result"]["content"][0]["text"])


if __name__ == "__main__":
    unittest.main()
