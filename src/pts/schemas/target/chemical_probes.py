"""PySpark schema for chemical probes data."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

# Chemical probes schema (probes.xlsx)
chemical_probes_raw_schema: StructType = StructType([
    StructField('targetFromSourceId', StringType(), True),
    StructField('id', StringType(), True),
    StructField('drugFromSourceId', StringType(), True),
    StructField('drugId', StringType(), True),
    StructField('mechanismOfAction', StringType(), True),
    StructField('origin', StringType(), True),
    StructField('control', StringType(), True),
    StructField('isHighQuality', StringType(), True),
    StructField('probesDrugsScore', StringType(), True),
    StructField('probeMinerScore', StringType(), True),
    StructField('scoreInCells', StringType(), True),
    StructField('scoreInOrganisms', StringType(), True),
    StructField('urls', StringType(), True),
])

# URL structure
url_struct: StructType = StructType([
    StructField('niceName', StringType(), True),
    StructField('url', StringType(), True),
])

# Chemical probe structure
chemical_probe_struct: StructType = StructType([
    StructField('targetFromSourceId', StringType(), True),
    StructField('id', StringType(), True),
    StructField('drugFromSourceId', StringType(), True),
    StructField('drugId', StringType(), True),
    StructField('mechanismOfAction', ArrayType(StringType()), True),
    StructField('origin', ArrayType(StringType()), True),
    StructField('control', StringType(), True),
    StructField('isHighQuality', BooleanType(), True),
    StructField('probesDrugsScore', FloatType(), True),
    StructField('probeMinerScore', FloatType(), True),
    StructField('scoreInCells', FloatType(), True),
    StructField('scoreInOrganisms', FloatType(), True),
    StructField('urls', ArrayType(url_struct), True),
])

# Chemical probes with id (after processing)
chemical_probes_schema: StructType = StructType([
    StructField('id', StringType(), True),
    StructField('chemicalProbes', ArrayType(chemical_probe_struct), True),
])
