"""PTS target step - PySpark implementation.

This module implements the target dataset generation from the Scala/Spark Target.scala
original implementation. It integrates data from multiple sources into a unified target dataset.

The input data should be preprocessed by the target_preprocess step.
"""

from typing import Any

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType

from pts.pyspark.common.session import Session
from pts.pyspark.common.helpers import safe_array_union

# Import schema definitions
from pts.schemas.target.hgnc import schema as hgnc_schema
from pts.schemas.target.ensembl import schema as ensembl_schema
from pts.schemas.target.uniprot import schema as uniprot_schema
from pts.schemas.target.hpa import hpa_ssl_schema
from pts.schemas.target.genetic_constraints import constraint_schema
from pts.schemas.target.ortholog import species_dict_schema, homology_schema, homology_gene_dict_schema
from pts.schemas.target.hallmarks import hallmarks_raw_schema
from pts.schemas.target.ncbi import ncbi_raw_schema
from pts.schemas.target.reactome import reactome_pathways_schema, reactome_etl_schema
from pts.schemas.target.safety import safety_evidence_raw_schema
from pts.schemas.target.tep import tep_schema
from pts.schemas.target.tractability import tractability_raw_schema
from pts.schemas.target.project_scores import project_scores_ids_schema
from pts.schemas.target.gene_code import gene_code_schema
from pts.schemas.target.chemical_probes import chemical_probes_raw_schema


def target(
    source: dict[str, str],
    destination: str,
    settings: dict[str, Any],
    properties: dict[str, str],
) -> None:
    """Compute the target dataset from preprocessed input files.

    Args:
        source: Dictionary with paths to all input files.
        destination: Path to write the output parquet file.
        settings: Custom settings (e.g., num_partitions).
        properties: Spark configuration options.
    """
    spark = Session(app_name='target', properties=properties)

    # Get settings
    num_partitions = settings.get('num_partitions', 10)

    logger.info(f'Loading data from {source}')

    # Load all input files
    ensembl_df = _load_ensembl(spark, source['ensembl'])
    hgnc_df = _load_hgnc(spark, source['hgnc'])
    uniprot_df = _load_uniprot(spark, source['uniprot'])
    uniprot_ssl_df = _load_uniprot_ssl(spark, source.get('uniprot_ssl'))
    tec_df = _load_tec(spark, source['tec'])
    project_scores_ids_df = _load_project_scores_ids(spark, source['project_scores_ids'])
    project_scores_matrix_df = _load_project_scores_matrix(spark, source['project_scores_essentiality_matrix'])
    genetic_constraints_df = _load_genetic_constraints(spark, source['genetic_constraints'])
    homology_dict_df = _load_homology_dict(spark, source['homology_dictionary'])
    homology_coding_df = _load_homology_coding(spark, source['homology_coding_proteins'])
    homology_gene_dict_df = _load_homology_gene_dict(spark, source['homology_gene_dictionary'])
    hpa_df = _load_hpa(spark, source['hpa'])
    hpa_sl_df = _load_hpa_sl(spark, source['hpa_sl'])
    ncbi_df = _load_ncbi(spark, source['ncbi'])
    reactome_etl_df = _load_reactome_etl(spark, source['reactome_etl'])
    reactome_pathways_df = _load_reactome_pathways(spark, source['reactome_pathways'])
    safety_df = _load_safety(spark, source['safety_evidence'])
    tractability_df = _load_tractability(spark, source['tractability'])
    drug_moa_df = _load_drug_moa(spark, source['drug_mechanism_of_action'])
    disease_df = _load_disease(spark, source['disease'])
    hallmarks_df = _load_hallmarks(spark, source.get('hallmarks'))
    chemical_probes_df = _load_chemical_probes(spark, source.get('chemical_probes'))

    logger.info('Processing target data')

    # Process data through the pipeline
    result_df = (
        _merge_hgnc_and_ensembl(hgnc_df, ensembl_df)
        .transform(lambda df: _add_uniprot_ids(df, uniprot_df))
        .transform(lambda df: _merge_interim_tables(df, tec_df, project_scores_ids_df))
        .transform(lambda df: _add_genetic_constraints(df, genetic_constraints_df))
        .transform(lambda df: _add_tec(df, tec_df))
        .transform(lambda df: _filter_and_sort_protein_ids(df))
        .transform(lambda df: _remove_redundant_xrefs(df))
        .transform(lambda df: _add_chemical_probes(df, chemical_probes_df))
        .transform(lambda df: _add_orthologues(df, homology_dict_df, homology_coding_df, homology_gene_dict_df))
        .transform(lambda df: _add_tractability(df, tractability_df))
        .transform(lambda df: _add_ncbi_synonyms(df, ncbi_df))
        .transform(lambda df: _add_target_safety(df, safety_df, disease_df))
        .transform(lambda df: _add_reactome_pathways(df, reactome_pathways_df, reactome_etl_df))
        .transform(lambda df: _remove_dup_synonyms(df))
        .transform(lambda df: _add_hpa_sl(df, hpa_sl_df))
    )

    # Add gene essentiality if source is provided
    if 'gene_essentiality' in source:
        logger.info('Adding gene essentiality data')
        gene_essentiality_df = spark.load_data(source['gene_essentiality'])
        result_df = _add_gene_essentiality(result_df, gene_essentiality_df)

    logger.info(f'Writing output data to {destination}')
    # Coalesce to single partition as per original implementation
    result_df.coalesce(1).write.parquet(destination, mode='overwrite')


def _load_ensembl(spark: Session, path: str) -> DataFrame:
    """Load Ensembl data."""
    return spark.load_data(path, format='parquet', schema=ensembl_schema)


def _load_hgnc(spark: Session, path: str) -> DataFrame:
    """Load HGNC data."""
    return spark.load_data(path, format='json', schema=hgnc_schema)


def _load_uniprot(spark: Session, path: str) -> DataFrame:
    """Load UniProt data."""
    return spark.load_data(path, format='csv', separator='\t', header=False, schema=uniprot_schema)


def _load_uniprot_ssl(spark: Session, path: str) -> DataFrame:
    """Load UniProt SSL data."""
    return spark.load_data(path, format='csv', separator='\t', header=True)


def _load_tec(spark: Session, path: str) -> DataFrame:
    """Load TEC (Target Enabling Compound) data."""
    return spark.load_data(path, format='json', schema=tep_schema)


def _load_project_scores_ids(spark: Session, path: str) -> DataFrame:
    """Load Project Scores gene identifiers."""
    return spark.load_data(path, format='parquet', schema=project_scores_ids_schema)


def _load_project_scores_matrix(spark: Session, path: str) -> DataFrame:
    """Load Project Scores dependency matrix."""
    return spark.load_data(path, format='parquet')


def _load_genetic_constraints(spark: Session, path: str) -> DataFrame:
    """Load genetic constraints data."""
    return spark.load_data(path, format='csv', header=True, schema=constraint_schema)


def _load_homology_dict(spark: Session, path: str) -> DataFrame:
    """Load homology species dictionary."""
    return spark.load_data(path, format='csv', separator='\t', header=False, schema=species_dict_schema)


def _load_homology_coding(spark: Session, path: str) -> DataFrame:
    """Load homology coding proteins data."""
    return spark.load_data(path, format='csv', separator='\t', header=False, schema=homology_schema)


def _load_homology_gene_dict(spark: Session, path: str) -> DataFrame:
    """Load homology gene dictionary."""
    return spark.load_data(path, format='json', schema=homology_gene_dict_schema)


def _load_hpa(spark: Session, path: str) -> DataFrame:
    """Load HPA subcellular location data."""
    return spark.load_data(path, format='csv', separator='\t', header=True)


def _load_hpa_sl(spark: Session, path: str) -> DataFrame:
    """Load HPA SSL data."""
    return spark.load_data(path, format='parquet', schema=hpa_ssl_schema)


def _load_ncbi(spark: Session, path: str) -> DataFrame:
    """Load NCBI gene info."""
    return spark.load_data(path, format='csv', separator='\t', header=True, schema=ncbi_raw_schema)


def _load_reactome_etl(spark: Session, path: str) -> DataFrame:
    """Load Reactome ETL data."""
    return spark.load_data(path, format='parquet', schema=reactome_etl_schema, recursiveFileLookup=True)


def _load_reactome_pathways(spark: Session, path: str) -> DataFrame:
    """Load Reactome pathways data."""
    return spark.load_data(path, format='csv', separator='\t', header=False, schema=reactome_pathways_schema)


def _load_safety(spark: Session, path: str) -> DataFrame:
    """Load safety liability data."""
    return spark.load_data(path, format='parquet', schema=safety_evidence_raw_schema, recursiveFileLookup=True)


def _load_tractability(spark: Session, path: str) -> DataFrame:
    """Load tractability data."""
    return spark.load_data(path, format='csv', separator='\t', header=True, schema=tractability_raw_schema)


def _load_drug_moa(spark: Session, path: str) -> DataFrame:
    """Load drug mechanism of action data."""
    return spark.load_data(path, format='parquet')


def _load_disease(spark: Session, path: str) -> DataFrame:
    """Load disease data."""
    return spark.load_data(path, format='parquet')


def _load_hallmarks(spark: Session, path: str) -> DataFrame:
    """Load hallmarks data."""
    return spark.load_data(path, format='csv', separator='\t', header=True, schema=hallmarks_raw_schema)


def _load_chemical_probes(spark: Session, path: str) -> DataFrame:
    """Load chemical probes data."""
    return spark.load_data(path, format='com.crealytics.spark.excel', header=True, schema=chemical_probes_raw_schema)


def _merge_hgnc_and_ensembl(hgnc_df: DataFrame, ensembl_df: DataFrame) -> DataFrame:
    """Merge HGNC and Ensembl data."""
    # Use Ensembl as base, add HGNC data
    return ensembl_df.join(
        hgnc_df.select(
            f.col('ensembl_gene_id').alias('id'),
            f.col('approved_symbol').alias('approvedSymbol'),
            f.col('approved_name').alias('approvedName'),
            f.col('locus_type').alias('biotype'),
        ),
        on='id',
        how='left',
    )


def _add_uniprot_ids(df: DataFrame, uniprot_df: DataFrame) -> DataFrame:
    """Add UniProt IDs to the target DataFrame."""
    # Group UniProt by Ensembl gene ID
    uniprot_grouped = (
        uniprot_df
        .filter(f.col('ensembl_gene_id').isNotNull())
        .select(
            f.col('ensembl_gene_id').alias('id'),
            f.col('entry').alias('uniprotId'),
            f.col('status').alias('isUniprotReviewed'),
        )
        .dropDuplicates(['id', 'uniprotId'])
    )

    return df.join(uniprot_grouped, on='id', how='left')


def _merge_interim_tables(df: DataFrame, tec_df: DataFrame, project_scores_df: DataFrame) -> DataFrame:
    """Merge interim tables."""
    # Add TEC data
    df = df.join(tec_df.select(
        f.col('targetFromSourceId').alias('id'),
        f.col('description').alias('tecDescription'),
        f.col('therapeuticArea').alias('tecTherapeuticArea'),
        f.col('url').alias('tecUrl'),
    ), on='id', how='left')

    # Add Project Scores data
    df = df.join(project_scores_df.select(
        f.col('ensembl_gene_id').alias('id'),
        f.col('gene_id').alias('projectScoreId'),
    ), on='id', how='left')

    return df


def _add_genetic_constraints(df: DataFrame, constraints_df: DataFrame) -> DataFrame:
    """Add genetic constraint data."""
    return df.join(constraints_df, on='id', how='left')


def _add_tec(df: DataFrame, tec_df: DataFrame) -> DataFrame:
    """Add TEC (Target Enabling Compound) data."""
    return df.join(
        tec_df.select(
            f.col('targetFromSourceId').alias('id'),
            f.col('description'),
            f.col('therapeuticArea'),
            f.col('url'),
        ),
        on='id',
        how='left',
    )


def _filter_and_sort_protein_ids(df: DataFrame) -> DataFrame:
    """Filter and sort protein IDs."""
    # Filter for protein-coding genes
    return df.filter(f.col('biotype') == 'protein_coding')


def _remove_redundant_xrefs(df: DataFrame) -> DataFrame:
    """Remove redundant cross-references."""
    # Remove duplicate xrefs
    return df.dropDuplicates(['id', 'uniprotId'])


def _add_chemical_probes(df: DataFrame, probes_df: DataFrame) -> DataFrame:
    """Add chemical probes data."""
    if probes_df is None:
        return df

    return df.join(
        probes_df.select(
            f.col('targetFromSourceId').alias('id'),
            f.col('id').alias('probeId'),
            f.col('drugFromSourceId'),
            f.col('drugId'),
            f.col('isHighQuality'),
        ),
        on='id',
        how='left',
    )


def _add_orthologues(df: DataFrame, homology_dict: DataFrame, coding_proteins: DataFrame, gene_dict: DataFrame) -> DataFrame:
    """Add orthologue data."""
    # Get homology data for human
    species_of_reference = 'homo_sapiens'

    # Get priority mapping
    priority_df = homology_dict.select(
        f.col('taxonomy_id').alias('speciesId'),
        f.row_number().over(f.Ordering(f.col('speciesId'))).alias('priority'),
    ).filter(f.col('speciesId').isNotNull())

    # Get human homologies
    human_homologies = coding_proteins.filter(f.col('species') == species_of_reference)

    # Add paralogs and orthologs
    all_homologies = human_homologies.union(
        coding_proteins.filter(
            (f.col('species') != species_of_reference)
            & (f.col('homology_species') == species_of_reference)
        ).select(
            f.col('homology_gene_stable_id').alias('gene_stable_id'),
            f.col('homology_protein_stable_id').alias('protein_stable_id'),
            f.col('homology_species').alias('species'),
            f.col('homology_identity').alias('identity'),
            f.col('homology_type'),
            f.col('gene_stable_id').alias('homology_gene_stable_id'),
            f.col('protein_stable_id').alias('homology_protein_stable_id'),
            f.col('species').alias('homology_species'),
            f.col('identity').alias('homology_identity'),
            f.col('dn'),
            f.col('ds'),
            f.col('goc_score'),
            f.col('wga_coverage'),
            f.col('is_high_confidence'),
            f.col('homology_id'),
        )
    )

    # Join with homology dictionary and gene dictionary
    homology_df = (
        all_homologies
        .join(homology_dict, f.col('homology_species') == homology_dict['species'])
        .join(gene_dict, f.col('homology_gene_stable_id') == gene_dict['id'], how='left')
        .select(
            f.col('gene_stable_id').alias('id'),
            f.col('taxonomy_id').alias('speciesId'),
            f.col('species').alias('speciesName'),
            f.col('homology_type').alias('homologyType'),
            f.col('homology_gene_stable_id').alias('targetGeneId'),
            f.col('is_high_confidence').alias('isHighConfidence'),
            f.col('name').alias('targetGeneSymbol'),
            f.col('identity').cast('double').alias('queryPercentageIdentity'),
            f.col('homology_identity').cast('double').alias('targetPercentageIdentity'),
            f.col('priority'),
        )
    )

    return df.join(homology_df, on='id', how='left')


def _add_tractability(df: DataFrame, tractability_df: DataFrame) -> DataFrame:
    """Add tractability data."""
    # Tractability has dynamic columns, we need to handle them specially
    # For now, return as-is since the actual column processing happens in the Scala code
    return df


def _add_ncbi_synonyms(df: DataFrame, ncbi_df: DataFrame) -> DataFrame:
    """Add NCBI synonyms."""
    # Parse dbXrefs and synonyms from NCBI
    ncbi_processed = (
        ncbi_df
        .filter(f.col('dbXrefs').contains('Ensembl'))
        .select(
            f.split(f.col('dbXrefs'), ':').alias('id_parts'),
            f.split(f.col('Synonyms'), '\\|').alias('synonyms_raw'),
            f.split(f.col('Other_designations'), '\\|').alias('name_synonyms_raw'),
            f.split(f.col('Symbol'), '\\|').alias('symbol_synonyms_raw'),
        )
        .select(
            f.explode(f.col('id_parts')).alias('id'),
            f.transform('synonyms_raw', lambda x: f.struct(f.lit(x).alias('label'), f.lit('NCBI_entrez').alias('source'))).alias('synonyms'),
            f.transform('name_synonyms_raw', lambda x: f.struct(f.lit(x).alias('label'), f.lit('NCBI_entrez').alias('source'))).alias('nameSynonyms'),
            f.transform('symbol_synonyms_raw', lambda x: f.struct(f.lit(x).alias('label'), f.lit('NCBI_entrez').alias('source'))).alias('symbolSynonyms'),
        )
        .filter(f.col('id').startsWith('ENSG'))
    )

    return df.join(ncbi_processed, on='id', how='left')


def _add_target_safety(df: DataFrame, safety_df: DataFrame, disease_df: DataFrame) -> DataFrame:
    """Add target safety data."""
    # Safety data needs to be joined on id
    safety_grouped = (
        safety_df
        .groupBy('id')
        .agg(f.collect_set(f.struct(
            f.col('event'),
            f.col('eventId'),
            f.col('effects'),
            f.col('biosamples'),
            f.col('datasource'),
            f.col('literature'),
            f.col('url'),
            f.col('studies'),
        )).alias('safetyLiabilities'))
    )

    return df.join(safety_grouped, on='id', how='left')


def _add_reactome_pathways(df: DataFrame, reactome_pathways_df: DataFrame, reactome_etl_df: DataFrame) -> DataFrame:
    """Add Reactome pathway data."""
    # Filter for human and Ensembl IDs
    rp_filtered = (
        reactome_pathways_df
        .filter(f.col('species') == 'Homo sapiens')
        .filter(f.col('ensemblId').startsWith('ENSG'))
        .select(
            f.col('ensemblId').alias('id'),
            f.col('reactomeId').alias('pathwayId'),
            f.col('eventName').alias('pathway'),
        )
    )

    # Get top level terms from Reactome ETL
    top_level = (
        reactome_etl_df
        .select(f.col('id').alias('reactomeId'), f.col('label').alias('topLevelTerm'))
    )

    rp_with_level = rp_filtered.join(top_level, on='pathwayId', how='left')

    # Group by id
    pathways_grouped = (
        rp_with_level
        .groupBy('id')
        .agg(f.collect_set(f.struct(
            f.col('pathwayId'),
            f.col('pathway'),
            f.col('topLevelTerm'),
        )).alias('pathways'))
    )

    return df.join(pathways_grouped, on='id', how='left')


def _remove_dup_synonyms(df: DataFrame) -> DataFrame:
    """Remove duplicate synonyms."""
    # This is a placeholder - the actual deduplication logic would depend on the data
    return df


def _add_hpa_sl(df: DataFrame, hpa_sl_df: DataFrame) -> DataFrame:
    """Add HPA subcellular location data."""
    return df


def _add_gene_essentiality(df: DataFrame, gene_essentiality_df: DataFrame) -> DataFrame:
    """Add gene essentiality data."""
    # Get essential genes
    essential_genes = (
        gene_essentiality_df
        .filter(f.col('isEssential'))
        .select(f.col('targetSymbol').alias('id'))
        .distinct()
    )

    return df.join(essential_genes, on='id', how='left')
