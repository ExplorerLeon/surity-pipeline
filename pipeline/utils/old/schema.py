def pandas_dataframe_to_bq_schema(df):
    """
    Create a BigQuery schema from a Pandas DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame for which you want to generate a BigQuery schema.

    Returns:
    list: A list of dictionaries representing the BigQuery schema.
    """
    bq_schema = []

    for column_name, dtype in df.dtypes.items():
        field_type = None

        if dtype == 'int64':
            field_type = 'INTEGER'
        elif dtype == 'float64':
            field_type = 'FLOAT'
        elif dtype == 'bool':
            field_type = 'BOOLEAN'
        elif dtype == 'datetime64[ns]':
            field_type = 'TIMESTAMP'
        elif dtype == 'object':
            field_type = 'STRING'

        if field_type:
            bq_schema.append({'name': column_name, 'type': field_type})

    return bq_schema
