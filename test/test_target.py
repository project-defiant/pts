"""Integration tests for target step.

These tests use MagicMock to verify the target step's pipeline logic
without loading actual data files.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame


class TestTargetPipeline:
    """Integration tests for the target pipeline."""

    @pytest.fixture
    def mock_session(self) -> MagicMock:
        """Create a mock Session."""
        session = MagicMock()
        session.spark = MagicMock()
        session.load_data = MagicMock(return_value=MagicMock(spec=DataFrame))
        return session

    def test_target_calls_all_loaders(self, mock_session: MagicMock) -> None:
        """Test that target calls all required loader functions."""
        from pts.pyspark import target

        # Setup return values for load_data
        mock_df = MagicMock(spec=DataFrame)
        mock_df.join = MagicMock(return_value=mock_df)
        mock_df.select = MagicMock(return_value=mock_df)
        mock_df.filter = MagicMock(return_value=mock_df)
        mock_df.dropDuplicates = MagicMock(return_value=mock_df)
        mock_df.distinct = MagicMock(return_value=mock_df)
        mock_df.groupBy = MagicMock(return_value=mock_df)
        mock_df.agg = MagicMock(return_value=mock_df)
        mock_df.persist = MagicMock(return_value=mock_df)
        mock_df.coalesce = MagicMock(return_value=mock_df)
        mock_df.write = MagicMock()
        mock_df.write.parquet = MagicMock()

        session = MagicMock()
        session.load_data = MagicMock(return_value=mock_df)

        source = {
            'ensembl': 'path/to/ensembl',
            'hgnc': 'path/to/hgnc',
            'uniprot': 'path/to/uniprot',
            'tec': 'path/to/tec',
            'project_scores_ids': 'path/to/project_scores_ids',
            'project_scores_essentiality_matrix': 'path/to/project_scores_matrix',
            'genetic_constraints': 'path/to/genetic_constraints',
            'homology_dictionary': 'path/to/homology_dict',
            'homology_coding_proteins': 'path/to/homology_coding',
            'homology_gene_dictionary': 'path/to/homology_gene_dict',
            'hpa': 'path/to/hpa',
            'hpa_sl': 'path/to/hpa_sl',
            'ncbi': 'path/to/ncbi',
            'reactome_etl': 'path/to/reactome_etl',
            'reactome_pathways': 'path/to/reactome_pathways',
            'safety_evidence': 'path/to/safety',
            'tractability': 'path/to/tractability',
            'drug_mechanism_of_action': 'path/to/moa',
            'disease': 'path/to/disease',
        }

        destination = 'path/to/output'

        with patch('pts.pyspark.target.Session', return_value=session):
            target.target(source, destination, {}, {})

        # Verify load_data was called for each data source
        assert session.load_data.call_count >= 15

    def test_target_calls_all_transformations(self, mock_session: MagicMock) -> None:
        """Test that target applies all required transformations."""
        from pts.pyspark import target

        # Create write mock with parquet method
        write_mock = MagicMock()
        write_mock.parquet = MagicMock()

        mock_df = MagicMock(spec=DataFrame)
        mock_df.join = MagicMock(return_value=mock_df)
        mock_df.select = MagicMock(return_value=mock_df)
        mock_df.filter = MagicMock(return_value=mock_df)
        mock_df.dropDuplicates = MagicMock(return_value=mock_df)
        mock_df.distinct = MagicMock(return_value=mock_df)
        mock_df.groupBy = MagicMock(return_value=mock_df)
        mock_df.agg = MagicMock(return_value=mock_df)
        mock_df.persist = MagicMock(return_value=mock_df)
        mock_df.transform = MagicMock(return_value=mock_df)
        mock_df.coalesce = MagicMock(return_value=mock_df)
        mock_df.write = write_mock

        session = MagicMock()
        session.load_data = MagicMock(return_value=mock_df)

        source = {
            'ensembl': 'path/to/ensembl',
            'hgnc': 'path/to/hgnc',
            'uniprot': 'path/to/uniprot',
            'tec': 'path/to/tec',
            'project_scores_ids': 'path/to/project_scores_ids',
            'project_scores_essentiality_matrix': 'path/to/project_scores_matrix',
            'genetic_constraints': 'path/to/genetic_constraints',
            'homology_dictionary': 'path/to/homology_dict',
            'homology_coding_proteins': 'path/to/homology_coding',
            'homology_gene_dictionary': 'path/to/homology_gene_dict',
            'hpa': 'path/to/hpa',
            'hpa_sl': 'path/to/hpa_sl',
            'ncbi': 'path/to/ncbi',
            'reactome_etl': 'path/to/reactome_etl',
            'reactome_pathways': 'path/to/reactome_pathways',
            'safety_evidence': 'path/to/safety',
            'tractability': 'path/to/tractability',
            'drug_mechanism_of_action': 'path/to/moa',
            'disease': 'path/to/disease',
        }

        destination = 'path/to/output'

        with patch('pts.pyspark.target.Session', return_value=session):
            target.target(source, destination, {}, {})

        # Verify output was written
        assert write_mock.parquet.called

    def test_target_with_gene_essentiality(self, mock_session: MagicMock) -> None:
        """Test that target adds gene essentiality when source is provided."""
        from pts.pyspark import target

        mock_df = MagicMock(spec=DataFrame)
        mock_df.join = MagicMock(return_value=mock_df)
        mock_df.select = MagicMock(return_value=mock_df)
        mock_df.filter = MagicMock(return_value=mock_df)
        mock_df.dropDuplicates = MagicMock(return_value=mock_df)
        mock_df.distinct = MagicMock(return_value=mock_df)
        mock_df.groupBy = MagicMock(return_value=mock_df)
        mock_df.agg = MagicMock(return_value=mock_df)
        mock_df.persist = MagicMock(return_value=mock_df)
        mock_df.coalesce = MagicMock(return_value=mock_df)
        mock_df.write = MagicMock()
        mock_df.write.parquet = MagicMock()

        session = MagicMock()
        session.load_data = MagicMock(return_value=mock_df)

        source = {
            'ensembl': 'path/to/ensembl',
            'hgnc': 'path/to/hgnc',
            'uniprot': 'path/to/uniprot',
            'tec': 'path/to/tec',
            'project_scores_ids': 'path/to/project_scores_ids',
            'project_scores_essentiality_matrix': 'path/to/project_scores_matrix',
            'genetic_constraints': 'path/to/genetic_constraints',
            'homology_dictionary': 'path/to/homology_dict',
            'homology_coding_proteins': 'path/to/homology_coding',
            'homology_gene_dictionary': 'path/to/homology_gene_dict',
            'hpa': 'path/to/hpa',
            'hpa_sl': 'path/to/hpa_sl',
            'ncbi': 'path/to/ncbi',
            'reactome_etl': 'path/to/reactome_etl',
            'reactome_pathways': 'path/to/reactome_pathways',
            'safety_evidence': 'path/to/safety',
            'tractability': 'path/to/tractability',
            'drug_mechanism_of_action': 'path/to/moa',
            'disease': 'path/to/disease',
            'gene_essentiality': 'path/to/gene_essentiality',
        }

        destination = 'path/to/output'

        with patch('pts.pyspark.target.Session', return_value=session):
            target.target(source, destination, {}, {})

        # Verify all loaders were called including gene_essentiality
        assert session.load_data.call_count >= 16


class TestTargetEssentialityPipeline:
    """Integration tests for the target essentiality pipeline."""

    @pytest.fixture
    def mock_session(self) -> MagicMock:
        """Create a mock Session."""
        session = MagicMock()
        session.spark = MagicMock()
        session.load_data = MagicMock(return_value=MagicMock(spec=DataFrame))
        return session

    def test_target_essentiality_calls_all_loaders(self, mock_session: MagicMock) -> None:
        """Test that target_essentiality calls all required loader functions."""
        from pts.pyspark import target_essentiality

        mock_df = MagicMock(spec=DataFrame)
        mock_df.join = MagicMock(return_value=mock_df)
        mock_df.select = MagicMock(return_value=mock_df)
        mock_df.filter = MagicMock(return_value=mock_df)
        mock_df.dropDuplicates = MagicMock(return_value=mock_df)
        mock_df.distinct = MagicMock(return_value=mock_df)
        mock_df.groupBy = MagicMock(return_value=mock_df)
        mock_df.agg = MagicMock(return_value=mock_df)
        mock_df.persist = MagicMock(return_value=mock_df)
        mock_df.coalesce = MagicMock(return_value=mock_df)
        mock_df.write = MagicMock()
        mock_df.write.parquet = MagicMock()

        session = MagicMock()
        session.load_data = MagicMock(return_value=mock_df)

        source = {
            'essentiality': 'path/to/essentiality',
            'target': 'path/to/target',
        }

        destination = 'path/to/output'

        with patch('pts.pyspark.target_essentiality.Session', return_value=session):
            target_essentiality.target_essentiality(source, destination, {}, {})

        # Verify load_data was called for each source
        assert session.load_data.call_count == 2

    def test_target_essentiality_writes_output(self, mock_session: MagicMock) -> None:
        """Test that target_essentiality writes output."""
        from pts.pyspark import target_essentiality

        mock_df = MagicMock(spec=DataFrame)
        mock_df.join = MagicMock(return_value=mock_df)
        mock_df.select = MagicMock(return_value=mock_df)
        mock_df.filter = MagicMock(return_value=mock_df)
        mock_df.dropDuplicates = MagicMock(return_value=mock_df)
        mock_df.distinct = MagicMock(return_value=mock_df)
        mock_df.groupBy = MagicMock(return_value=mock_df)
        mock_df.agg = MagicMock(return_value=mock_df)
        mock_df.persist = MagicMock(return_value=mock_df)
        mock_df.coalesce = MagicMock(return_value=mock_df)
        mock_df.write = MagicMock()
        mock_df.write.parquet = MagicMock()

        session = MagicMock()
        session.load_data = MagicMock(return_value=mock_df)

        source = {
            'essentiality': 'path/to/essentiality',
            'target': 'path/to/target',
        }

        destination = 'path/to/output'

        with patch('pts.pyspark.target_essentiality.Session', return_value=session):
            target_essentiality.target_essentiality(source, destination, {}, {})

        # Verify output was written
        assert mock_df.coalesce.return_value.write.parquet.called
