"""Unit tests for target PySpark loaders.

Each loader function is tested with a small reference dataset fixture.
The tests verify that:
1. The loader returns a valid DataFrame
2. Data is correctly loaded
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import polars as pl
from pyspark.sql import SparkSession


class TestEnsemblLoader:
    """Tests for Ensembl loader."""

    def test_load_ensembl_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_ensembl returns a DataFrame."""
        from pts.transformers.target.ensembl import load_ensembl

        # Create a temporary parquet file with minimal data
        with tempfile.TemporaryDirectory() as tmp_dir:
            test_data = pl.DataFrame(
                [('ENSG000001', 1, 'GENE1', '1', 100, 200, 1)],
                schema=['id', 'version', 'name', 'chromosome', 'start', 'end', 'strand'],
                orient='row',
            )
            test_data.write_parquet(f'{tmp_dir}/test.parquet')

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
            test_data = pl.DataFrame(
                [('GENE1', 'Gene One', 'Approved', 'gene', 'category', '1', None, 'ENSG000001', 'HGNC:1', '123')],
                schema=[
                    'approved_symbol',
                    'approved_name',
                    'status',
                    'locus_type',
                    'locus_group',
                    'chromosome',
                    'gene_family_id',
                    'ensembl_gene_id',
                    'hgnc_id',
                    'entrez_gene_id',
                ],
                orient='row',
            )
            test_data.write_json(f'{tmp_dir}/test.json')

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
            data = [
                (
                    'P12345',
                    'PROteinA',
                    'Swiss-Prot',
                    'Protein A',
                    'GENE1',
                    'Homo sapiens',
                    '9606',
                    '150',
                    'abc123',
                    'ENSG000001',
                    'transcript1',
                    'protein1',
                    'refseq1',
                    'GENE1',
                    'HGNC:1',
                    'OMIM:1',
                ),
                (
                    'Q98765',
                    'PROteinB',
                    'TrEMBL',
                    'Protein B',
                    'GENE2',
                    'Homo sapiens',
                    '9606',
                    '200',
                    'def456',
                    'ENSG000002',
                    'transcript2',
                    'protein2',
                    'refseq2',
                    'GENE2',
                    'HGNC:2',
                    'OMIM:2',
                ),
            ]
            df = pl.DataFrame(
                data,
                schema=[
                    'accession',
                    'entry_name',
                    'reviewed',
                    'protein_name',
                    'gene_name',
                    'organism',
                    'tax_id',
                    'length',
                    'sequence',
                    'ensembl_gene_id',
                    'transcript_id',
                    'protein_id',
                    'refseq_id',
                    'gene_name_synonym',
                    'hgnc_id',
                    'omim_id',
                ],
                orient='row',
            )
            tsv_path = Path(tmp_dir) / 'test.tsv'
            df.write_csv(tsv_path, include_header=False, separator='\t')
            result = load_uniprot(spark, str(tsv_path))
            assert result is not None
            assert result.count() > 0


class TestHpaLoader:
    """Tests for HPA loader."""

    def test_load_hpa_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_hpa returns a DataFrame."""
        from pts.transformers.target.hpa import load_hpa

        with tempfile.TemporaryDirectory() as tmp_dir:
            test_data = pl.DataFrame(
                [('PM', 'PM', 'Plasma membrane')],
                schema=['HPA_location', 'termSL', 'labelSL'],
                orient='row',
            )
            test_data.write_parquet(f'{tmp_dir}/test.parquet')

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
            schema = [
                'gene_idcanonical',
                'transcript_type',
                'syn.z_score',
                'syn.exp',
                'syn.obs',
                'syn.oe',
                'syn.oe_ci.lower',
                'syn.oe_ci.upper',
                'mis.z_score',
                'mis.exp',
                'mis.obs',
                'mis.oe',
                'mis.oe_ci.lower',
                'mis.oe_ci.upper',
                'lof.pLI',
                'lof.exp',
                'lof.obs',
                'lof.oe',
                'lof.oe_ci.lower',
                'lof.oe_ci.upper',
                'lof.oe_ci.upper_rank',
                'lof.oe_ci.upper_bin_decile',
                'lof.oe_ci.upper_bin_sextile',
            ]
            data = [
                (
                    'ENSG000001',
                    'protein_coding',
                    1.5,
                    100,
                    50,
                    0.5,
                    0.4,
                    0.6,
                    2.0,
                    100,
                    60,
                    0.6,
                    0.5,
                    0.7,
                    0.9,
                    100,
                    40,
                    0.4,
                    0.3,
                    0.5,
                    10,
                    5,
                    3,
                ),
            ]

            df = pl.DataFrame(data, schema=schema, orient='row')
            tsv_path = Path(tmp_dir) / 'test.tsv'
            df.write_csv(tsv_path, include_header=True, separator='\t')
            result = load_genetic_constraints(spark, str(tsv_path))
            assert result is not None
            assert result.count() > 0


class TestTepLoader:
    """Tests for TEP loader."""

    def test_load_tep_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_tep returns a DataFrame."""
        from pts.transformers.target.tep import load_tep

        with tempfile.TemporaryDirectory() as tmp_dir:
            test_data = pl.DataFrame(
                [('ENSG000001', 'Description', 'Therapeutic Area', 'http://example.com')],
                schema=['targetFromSourceId', 'description', 'therapeuticArea', 'url'],
                orient='row',
            )
            test_data.write_ndjson(f'{tmp_dir}/test.json.gz', compression='gzip')

            result = load_tep(spark, f'{tmp_dir}/test.json.gz')
            assert result is not None
            assert result.count() > 0


class TestProjectScoresLoader:
    """Tests for Project Scores loader."""

    def test_load_project_scores_ids_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_project_scores_ids returns a DataFrame."""
        from pts.transformers.target.project_scores import load_project_scores_ids

        with tempfile.TemporaryDirectory() as tmp_dir:
            test_data = pl.DataFrame(
                [('GENE1', 'COSMIC1', 'ENSG000001', '123', 'HGNC:1', 'GENE1', 'NM_001', 'P12345')],
                schema=[
                    'gene_id',
                    'cosmic_gene_symbol',
                    'ensembl_gene_id',
                    'entrez_id',
                    'hgnc_id',
                    'hgnc_symbol',
                    'refseq_id',
                    'uniprot_id',
                ],
                orient='row',
            )
            test_data.write_parquet(f'{tmp_dir}/test.parquet')

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
            header = ['#name', 'species', 'taxonomy_id']
            data = [
                ('Homo sapiens', 'homo_sapiens', 9606),
                ('Mus musculus', 'mus_musculus', 10090),
                ('Rattus norvegicus', 'rattus_norvegicus', 10116),
            ]

            df = pl.DataFrame(data, schema=header, orient='row')
            tsv_path = Path(tmp_dir) / 'species.txt'
            df.write_csv(tsv_path, include_header=True, separator='\t')

            result = load_ortholog_dict(spark, str(tsv_path))
            assert result is not None
            assert result.count() > 0

    def test_load_homology_coding_returns_dataframe(self, spark: SparkSession) -> None:
        """Test that load_homology_coding returns a DataFrame."""
        from pts.transformers.target.ortholog import load_coding_proteins

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a TSV file with homology data
            header = [
                'gene_stable_id',
                'protein_stable_id',
                'species',
                'identity',
                'homology_type',
                'homology_gene_stable_id',
                'homology_protein_stable_id',
                'homology_species',
                'homology_identity',
                'dn',
                'ds',
                'goc_score',
                'wga_coverage',
                'is_high_confidence',
                'homology_id',
            ]
            data = [
                (
                    'ENSG000001',
                    'ENSP000001',
                    'homo_sapiens',
                    100,
                    'ortholog_one_to_one',
                    'ENSMUSG000001',
                    'ENSMUSP000001',
                    'mus_musculus',
                    85,
                    0.1,
                    0.2,
                    0.9,
                    0.8,
                    True,
                    123,
                ),
            ]
            df = pl.DataFrame(data, schema=header, orient='row')
            tsv_path = Path(tmp_dir) / 'homology.tsv'
            df.write_csv(tsv_path, include_header=True, separator='\t')

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
            header = [
                '#tax_id',
                'GeneID',
                'Symbol',
                'Synonyms',
                'dbXrefs',
                'chromosome',
                'map_location',
                'description',
                'type_of_gene',
                'Symbol_from_nomenclature_authority',
                'Full_name_from_nomenclature_authority',
                'Nomenclature_status',
                'Other_designations',
                'Modification_date',
            ]
            data = [
                (
                    9606,
                    1,
                    'GENE1',
                    'SYN1|SYN2',
                    'Ensembl:ENSG000001',
                    1,
                    '1p36.33',
                    'Gene description',
                    'protein-coding',
                    'GENE1',
                    'Gene One',
                    'A',
                    'Other designations',
                    '20240101',
                ),
            ]
            df = pl.DataFrame(data, schema=header, orient='row')

            tsv_path = Path(tmp_dir) / 'test.tsv.gz'
            df.write_csv(tsv_path, include_header=True, separator='\t', compression='gzip')

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
            data = [
                (
                    'ENSG000001',
                    'R-HSA-12345',
                    'https://reactome.org/content/detail/R-HSA-12345',
                    'Pathway Name',
                    'EventCode',
                    'homo_sapiens',
                ),
            ]

            header = [
                'ensembl_gene_id',
                'reactome_id',
                'url',
                'pathway_name',
                'event_code',
                'species',
            ]
            df = pl.DataFrame(data, schema=header, orient='row')
            tsv_path = Path(tmp_dir) / 'pathways.tsv'
            df.write_csv(tsv_path, include_header=False, separator='\t')

            result = load_reactome_pathways(spark, str(tsv_path))
            assert result is not None
            assert result.count() > 0
