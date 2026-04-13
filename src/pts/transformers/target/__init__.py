"""Loader functions for target-related datasets."""

from pts.transformers.target.chemical_probes import load_chemical_probes
from pts.transformers.target.ensembl import load_ensembl
from pts.transformers.target.gene_code import load_gene_code
from pts.transformers.target.gene_ontology import load_gene_ontology
from pts.transformers.target.genetic_constraints import load_genetic_constraints
from pts.transformers.target.hallmarks import load_hallmarks
from pts.transformers.target.hgnc import load_hgnc
from pts.transformers.target.hpa import load_hpa
from pts.transformers.target.ncbi import load_ncbi
from pts.transformers.target.ortholog import load_ortholog
from pts.transformers.target.project_scores import load_essentiality, load_project_scores
from pts.transformers.target.protein_classification import load_protein_classification
from pts.transformers.target.reactome import load_reactome
from pts.transformers.target.safety import load_safety
from pts.transformers.target.tep import load_tep
from pts.transformers.target.tractability import load_tractability
from pts.transformers.target.uniprot import load_uniprot, load_uniprot_ssl

__all__ = [
    'load_chemical_probes',
    'load_ensembl',
    'load_essentiality',
    'load_gene_code',
    'load_gene_ontology',
    'load_genetic_constraints',
    'load_hallmarks',
    'load_hgnc',
    'load_hpa',
    'load_ncbi',
    'load_ortholog',
    'load_project_scores',
    'load_protein_classification',
    'load_reactome',
    'load_safety',
    'load_tep',
    'load_tractability',
    'load_uniprot',
    'load_uniprot_ssl',
]
