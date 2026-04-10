"""Helper functions for PySpark ETL transformations.

This module provides utilities for PySpark transformations equivalent to
Scala/Spark Helpers.scala.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f


def mk_flatten_array(cols: list[Column]) -> Column:
    """Flatten and union multiple array columns, removing nulls and duplicates.

    Equivalent to Helpers.mkFlattenArray from Scala.

    Args:
        cols: List of array columns to flatten and union

    Returns:
        PySpark Column for flattened, unioned arrays

    Examples:
        >>> expr = mk_flatten_array([f.col('left'), f.col('right')])
        >>> isinstance(expr, Column)
        True
    """
    combined = f.flatten(f.array(*cols))
    return f.array_distinct(f.filter(combined, lambda x: x.isNotNull()))


def safe_array_union(*cols: Column) -> Column:
    """Union multiple array columns, treating nulls as empty arrays.

    Equivalent to Helpers.safeArrayUnion from Scala.

    Args:
        *cols: Array columns to union

    Returns:
        PySpark Column for safe array union

    Examples:
        >>> expr = safe_array_union(f.col('a'), f.col('b'), f.col('c'))
        >>> isinstance(expr, Column)
        True
    """
    filled = [f.coalesce(c, f.array()) for c in cols]
    combined = f.flatten(f.array(*filled))
    return f.array_distinct(f.filter(combined, lambda x: x.isNotNull()))


def transpose_dataframe(df: DataFrame, by: list[str]) -> DataFrame:
    """Transpose DataFrame columns to rows.

    Equivalent to Helpers.transposeDataframe from Scala.

    Args:
        df: Input DataFrame
        by: Columns to keep as identifiers (pivot columns)

    Returns:
        Transposed DataFrame with key/val columns

    Examples:
        >>> df = spark.createDataFrame([(1, 'A', 'B')], ['id', 'c1', 'c2'])
        >>> transpose_dataframe(df, ['id']).columns
        ['id', 'key', 'val']
    """
    non_by_cols = [c for c in df.columns if c not in by]

    # Create array of structs for key-value pairs
    kv_array = f.array(*[f.struct(f.lit(c).alias('key'), f.col(c).alias('val')) for c in non_by_cols])

    return df.select(*[f.col(c) for c in by], f.explode(kv_array).alias('_kvs')).select(
        *[f.col(c) for c in by], f.col('_kvs.key').alias('key'), f.col('_kvs.val').alias('val')
    )


def union_dataframes_different_schema(dataframes: list[DataFrame]) -> DataFrame:
    """Union DataFrames with potentially different schemas.

    Equivalent to Helpers.unionDataframeDifferentSchema from Scala.
    Fills missing columns with nulls.

    Args:
        dataframes: List of DataFrames to union

    Returns:
        Unioned DataFrame with all columns

    Examples:
        >>> df1 = spark.createDataFrame([(1,)], ['id'])
        >>> df2 = spark.createDataFrame([('x',)], ['name'])
        >>> union_dataframes_different_schema([df1, df2]).columns
        ['id', 'name']
        >>> union_dataframes_different_schema([]) is None
        True
    """
    if not dataframes:
        return dataframes[0] if dataframes else None

    if len(dataframes) == 1:
        return dataframes[0]

    all_columns = []
    for df in dataframes:
        for col in df.columns:
            if col not in all_columns:
                all_columns.append(col)

    aligned = []
    for df in dataframes:
        missing = set(all_columns) - set(df.columns)
        for col in missing:
            df = df.withColumn(col, f.lit(None))
        aligned.append(df.select(*all_columns))

    return aligned[0].unionByName(*aligned[1:])


def flatten_concat(*col_names: str) -> Column:
    """Flatten and concatenate multiple columns into a single array.

    Equivalent to Helpers.flattenCat from Scala.
    Removes nulls, trims whitespace, and removes duplicates.

    Args:
        *col_names: Column names to flatten and concatenate

    Returns:
        PySpark Column for flattened concatenation

    Examples:
        >>> isinstance(flatten_concat('a', 'b'), Column)
        True
        >>> isinstance(flatten_concat(), Column)
        True
    """
    if not col_names:
        return f.array()

    combined = f.flatten(f.array(*[f.col(c) for c in col_names]))
    filtered = f.filter(combined, lambda x: (x.isNotNull()) & (f.length(f.trim(x)) > 0))
    return f.array_distinct(f.filter(f.transform(filtered, lambda x: f.trim(x)), lambda x: x.isNotNull()))


def nest(df: DataFrame, included_columns: list[str], collect_under: str) -> DataFrame:
    """Nest multiple columns into a struct column.

    Equivalent to Helpers.nest from Scala.

    Args:
        df: Input DataFrame
        included_columns: Columns to nest into a struct
        collect_under: Name for the new struct column

    Returns:
        DataFrame with nested struct column

    Examples:
        >>> df = spark.createDataFrame([(1, 'A', 'x')], ['id', 'name', 'type'])
        >>> nest(df, ['name', 'type'], 'details').columns
        ['id', 'details']
    """
    temp_name = f'_temp_{collect_under}'

    struct_expr = f.struct(*[f.col(c) for c in included_columns])

    return df.withColumn(temp_name, struct_expr).drop(*included_columns).withColumnRenamed(temp_name, collect_under)


def validate_df(required_columns: set[str], data_frame: DataFrame) -> None:
    """Validate that all required columns are present in DataFrame.

    Equivalent to Helpers.validateDF from Scala.

    Args:
        required_columns: Set of required column names
        data_frame: DataFrame to validate

    Raises:
        AssertionError: If required columns are missing

    Examples:
        >>> df = spark.createDataFrame([(1, "A")], ["id", "name"])
        >>> validate_df({"id", "name"}, df)
        >>> validate_df({"id", "missing"}, df)
        Traceback (most recent call last):
        ...
        AssertionError: One or more required columns {'missing'} not found in dataFrame columns: ['id', 'name']
    """
    actual_columns = set(data_frame.columns)
    missing = required_columns - actual_columns

    if missing:
        msg = f'One or more required columns {missing} not found in dataFrame columns: {data_frame.columns}'
        raise AssertionError(msg)


def snake_to_lower_camel(s: str) -> str:
    """Convert snake_case to lowerCamelCase.

    Equivalent to Helpers.snakeToLowerCamelSchema function.

    Args:
        s: Snake case string

    Returns:
        Lower camel case string

    Examples:
        >>> snake_to_lower_camel('hello_world')
        'helloWorld'
        >>> snake_to_lower_camel('alreadycamel')
        'alreadycamel'
    """
    parts = s.split('_')
    return parts[0] + ''.join(part.capitalize() for part in parts[1:])


def rename_columns_snake_to_camel(df: DataFrame) -> DataFrame:
    """Rename all columns in a DataFrame from snake_case to lowerCamelCase.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with renamed columns

    Examples:
        >>> df = spark.createDataFrame([(1, "Alice")], ["target_id", "gene_name"])
        >>> rename_columns_snake_to_camel(df).columns
        ['targetId', 'geneName']
    """
    new_columns = {col: snake_to_lower_camel(col) for col in df.columns}
    return df.select(*[f.col(c).alias(new_columns.get(c, c)) for c in df.columns])


def replace_spaces_schema(df: DataFrame) -> DataFrame:
    """Replace spaces in column names with underscores.

    Equivalent to Helpers.replaceSpacesSchema from Scala.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with space-free column names

    Examples:
        >>> df = spark.createDataFrame([(1, "A")], ["target id", "gene symbol"])
        >>> replace_spaces_schema(df).columns
        ['target_id', 'gene_symbol']
    """
    new_columns = {col: col.replace(' ', '_') for col in df.columns}
    return df.select(*[f.col(c).alias(new_columns[c]) for c in df.columns])
