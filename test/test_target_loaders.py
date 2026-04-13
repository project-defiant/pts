"""Unit tests for target PySpark loaders.

Each loader function is tested with a small reference dataset fixture.
The tests verify that:
1. The loader returns a valid DataFrame
2. Data is correctly loaded
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pyspark.sql import SparkSession


class TestEnsemblLoader:
    """Tests for Ensembl loader."""

    def test_load_ensembl_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_ensembl returns a DataFrame."""
        from pts.transformers.target.ensembl import load_ensembl

        # Create a temporary parquet file with minimal data
        with tempfile.TemporaryDirectory() as tmp_dir:
            test_data = spark.createDataFrame(
                [('ENSG000001', 1, 'GENE1', '1', 100, 200, 1)],
                'id STRING, version INT, name STRING, chromosome STRING, start INT, end INT, strand INT',
            )
            test_data.write.parquet(f'{tmp_dir}/test.parquet')

            result = load_ensembl(spark, f'{tmp_dir}/test.parquet')
            assert result is not None
            assert result.count() > 0


class TestHgncLoader:
    """Tests for HGNC loader."""

    def test_load_hgnc_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_hgnc returns a DataFrame."""
        from pts.transformers.target.hgnc import load_hgnc

        # Create a temporary JSON file with minimal data
        with tempfile.TemporaryDirectory() as tmp_dir:
            test_data = spark.createDataFrame(
                [('GENE1', 'Gene One', 'Approved', 'gene', 'category', '1', None, 'ENSG000001', 'HGNC:1', '123')],
                'approved_symbol STRING, approved_name STRING, status STRING, locus_type STRING, locus_group STRING, chromosome STRING, gene_family_id INT, ensembl_gene_id STRING, hgnc_id STRING, entrez_gene_id STRING',
            )
            test_data.write.mode('overwrite').json(f'{tmp_dir}/test.json')

            result = load_hgnc(spark, f'{tmp_dir}/test.json')
            assert result is not None
            assert result.count() > 0


class TestUniprotLoader:
    """Tests for UniProt loader."""

    def test_load_uniprot_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_uniprot returns a DataFrame."""
        from pts.transformers.target.uniprot import load_uniprot

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a TSV file with minimal data
            tsv_content = """P12345	PROteinA	Swiss-Prot	Protein A	GENE1	Homo sapiens	9606	150	abc123	ENSG000001	transcript1	protein1	refseq1	GENE1	HGNC:1	OMIM:1
Q98765	PROteinB	TrEMBL	Protein B	GENE2	Homo sapiens	9606	200	def456	ENSG000002	transcript2	protein2	refseq2	GENE2	HGNC:2	OMIM:2"""
            tsv_path = Path(tmp_dir) / 'test.tsv'
            tsv_path.write_text(tsv_content)

            result = load_uniprot(spark, str(tsv_path))
            assert result is not None
            assert result.count() > 0


class TestHpaLoader:
    """Tests for HPA loader."""

    def test_load_hpa_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_hpa returns a DataFrame."""
        from pts.transformers.target.hpa import load_hpa

        with tempfile.TemporaryDirectory() as tmp_dir:
            test_data = spark.createDataFrame(
                [('PM', 'PM', 'Plasma membrane')],
                'HPA_location STRING, termSL STRING, labelSL STRING',
            )
            test_data.write.parquet(f'{tmp_dir}/test.parquet')

            result = load_hpa(spark, f'{tmp_dir}/test.parquet')
            assert result is not None
            assert result.count() > 0


class TestGeneticConstraintsLoader:
    """Tests for genetic constraints loader."""

    def test_load_genetic_constraints_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_genetic_constraints returns a DataFrame."""
        from pts.transformers.target.genetic_constraints import load_genetic_constraints

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a TSV file with header
            tsv_content = """gene_idcanonical	transcript_type	syn.z_score	syn.exp	syn.obs	syn.oe	syn.oe_ci.lower	syn.oe_ci.upper	mis.z_score	mis.exp	mis.obs	mis.oe	mis.oe_ci.lower	mis.oe_ci.upper	lof.pLI	lof.exp	lof.obs	lof.oe	lof.oe_ci.lower	lof.oe_ci.upper	lof.oe_ci.upper_rank	lof.oe_ci.upper_bin_decile	lof.oe_ci.upper_bin_sextile
ENSG000001	true	protein_coding	1.5	100	50	0.5	0.4	0.6	2.0	100	60	0.6	0.5	0.7	0.9	100	40	0.4	0.3	0.5	10	5	3"""
            tsv_path = Path(tmp_dir) / 'test.tsv'
            tsv_path.write_text(tsv_content)

            result = load_genetic_constraints(spark, str(tsv_path))
            assert result is not None
            assert result.count() > 0


class TestTepLoader:
    """Tests for TEP loader."""

    def test_load_tep_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_tep returns a DataFrame."""
        from pts.transformers.target.tep import load_tep

        with tempfile.TemporaryDirectory() as tmp_dir:
            test_data = spark.createDataFrame(
                [('ENSG000001', 'Description', 'Therapeutic Area', 'http://example.com')],
                'targetFromSourceId STRING, description STRING, therapeuticArea STRING, url STRING',
            )
            test_data.write.mode('overwrite').json(f'{tmp_dir}/test.json.gz')

            result = load_tep(spark, f'{tmp_dir}/test.json.gz')
            assert result is not None
            assert result.count() > 0


class TestProjectScoresLoader:
    """Tests for Project Scores loader."""

    def test_load_project_scores_ids_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_project_scores_ids returns a DataFrame."""
        from pts.transformers.target.project_scores import load_project_scores_ids

        with tempfile.TemporaryDirectory() as tmp_dir:
            test_data = spark.createDataFrame(
                [('GENE1', 'COSMIC1', 'ENSG000001', '123', 'HGNC:1', 'GENE1', 'NM_001', 'P12345')],
                'gene_id STRING, cosmic_gene_symbol STRING, ensembl_gene_id STRING, entrez_id STRING, hgnc_id STRING, hgnc_symbol STRING, refseq_id STRING, uniprot_id STRING',
            )
            test_data.write.parquet(f'{tmp_dir}/test.parquet')

            result = load_project_scores_ids(spark, f'{tmp_dir}/test.parquet')
            assert result is not None
            assert result.count() > 0


class TestHomologyLoader:
    """Tests for homology loaders."""

    def test_load_homology_dict_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_homology_dict returns a DataFrame."""
        from pts.transformers.target.ortholog import load_ortholog_dict

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a TSV file with species dictionary
            tsv_content = """#name	species	taxonomy_id
Homo sapiens	homo_sapiens	9606
Mus musculus	mus_musculus	10090
Rattus norvegicus	rattus_norvegicus	10116"""
            tsv_path = Path(tmp_dir) / 'species.txt'
            tsv_path.write_text(tsv_content)

            result = load_ortholog_dict(spark, str(tsv_path))
            assert result is not None
            assert result.count() > 0

    def test_load_homology_coding_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_homology_coding returns a DataFrame."""
        from pts.transformers.target.ortholog import load_coding_proteins

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a TSV file with homology data
            tsv_content = """gene_stable_id	protein_stable_id	species	identity	homology_type	homology_gene_stable_id	homology_protein_stable_id	homology_species	homology_identity	dn	ds	goc_score	wga_coverage	is_high_confidence	homology_id
ENSG000001	ENSP000001	homo_sapiens	100	ortholog_one_to_one	ENSMUSG000001	ENSMUSP000001	mus_musculus	85	0.1	0.2	0.9	0.8	true	123"""
            tsv_path = Path(tmp_dir) / 'homology.tsv'
            tsv_path.write_text(tsv_content)

            result = load_coding_proteins(spark, str(tsv_path))
            assert result is not None
            assert result.count() > 0


class TestNcbiLoader:
    """Tests for NCBI loader."""

    def test_load_ncbi_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_ncbi returns a DataFrame."""
        from pts.transformers.target.ncbi import load_ncbi

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a gzipped TSV file
            tsv_content = """#tax_id	GeneID	Symbol	Synonyms	dbXrefs	chromosome	map_location	description	type_of_gene	Symbol_from_nomenclature_authority	Full_name_from_nomenclature_authority	Nomenclature_status	Other_designations	Modification_date
9606	1	GENE1	SYN1|SYN2	Ensembl:ENSG000001	1	1p36.33	Gene description	protein-coding	GENE1	Gene One	A	Other designations	20240101"""
            import gzip

            tsv_path = Path(tmp_dir) / 'test.tsv.gz'
            with gzip.open(tsv_path, 'wt') as f:
                f.write(tsv_content)

            result = load_ncbi(spark, str(tsv_path))
            assert result is not None
            assert result.count() > 0


class TestReactomeLoader:
    """Tests for Reactome loader."""

    def test_load_reactome_pathways_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_reactome_pathways returns a DataFrame."""
        from pts.transformers.target.reactome import load_reactome_pathways

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a TSV file with pathway data
            tsv_content = """ENSG000001	R-HSA-12345	https://reactome.org/content/detail/R-HSA-12345	Pathway Name	EventCode	Homo sapiens"""
            tsv_path = Path(tmp_dir) / 'pathways.tsv'
            tsv_path.write_text(tsv_content)

            result = load_reactome_pathways(spark, str(tsv_path))
            assert result is not None
            assert result.count() > 0
