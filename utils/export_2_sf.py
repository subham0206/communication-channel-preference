import pandas as pd
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def create_table_sf_results(cur, dataframe, tablename, db, schema):
    # Generate the CREATE TABLE SQL statement
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {db}.{schema}.{tablename} (\n"

    for col in dataframe.columns:
        column_name = col.upper()
        if dataframe[col].dtype.name in ["int", "int64"]:
            create_table_sql += f"{column_name} INT"
        elif dataframe[col].dtype.name == "object":
            create_table_sql += f"{column_name} VARCHAR(16777216)"
        elif dataframe[col].dtype.name == "datetime64[ns]":
            create_table_sql += f"{column_name} DATETIME"
        elif dataframe[col].dtype.name == "float64":
            create_table_sql += f"{column_name} FLOAT8"
        elif dataframe[col].dtype.name == "bool":
            create_table_sql += f"{column_name} BOOLEAN"
        else:
            create_table_sql += f"{column_name} VARCHAR(16777216)"

        if col != dataframe.columns[-1]:
            create_table_sql += ",\n"
        else:
            create_table_sql += "\n)"

    logger.info(f"create_table_sql={create_table_sql}")
    cur.execute(create_table_sql)
    logger.info(f"Table {tablename} created or already exists.")
    return create_table_sql


def create_table_sf_agg_data(
    cur, dataframe, tablename, db, schema
):  # NMEDWPRD_DB.mktsand.
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {db}.{schema}.{tablename}\n ("

    for col in dataframe.columns:
        column_name = col.upper()

        if dataframe[col].dtype.name in ["int", "int64"]:
            create_table_sql += f"{column_name} INT"
        elif dataframe[col].dtype.name == "object":
            create_table_sql += f"{column_name} VARCHAR(16777216)"
        elif dataframe[col].dtype.name == "datetime64[ns]":
            create_table_sql += f"{column_name} DATETIME"
        elif dataframe[col].dtype.name == "float64":
            create_table_sql += f"{column_name} FLOAT8"
        elif dataframe[col].dtype.name == "bool":
            create_table_sql += f"{column_name} BOOLEAN"
        else:
            create_table_sql += f"{column_name} VARCHAR(16777216)"

        # Add comma if not the last column
        if col != dataframe.columns[-1]:
            create_table_sql += ",\n"
        else:
            create_table_sql += ")"

    print(f"create_table_sql={create_table_sql}")
    cur.execute(create_table_sql)
    print(f"snowflake_table={tablename}")
    truncate_sql = f"TRUNCATE TABLE IF EXISTS {db}.{schema}.{tablename}"
    cur.execute(truncate_sql)
    print("table created")
    return create_table_sql


def insert_data_into_table(cur, dataframe, tablename, db, schema, batch_size=1000):
    # Initialize the insert SQL statement
    insert_sql = f"INSERT INTO {db}.{schema}.{tablename} VALUES "

    # Get the number of rows in the DataFrame
    num_rows = len(dataframe)

    # Calculate the number of batches
    num_batches = (num_rows + batch_size - 1) // batch_size

    # Iterate over each batch
    for i in range(num_batches):
        # Calculate the start and end index for the current batch
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, num_rows)

        # Get the current batch of data
        batch_data = dataframe.iloc[start_idx:end_idx]

        # Generate the SQL insert statement for the current batch
        values_list = []
        for _, row in batch_data.iterrows():
            values = []
            for value in row.values:
                if isinstance(value, str):
                    values.append(f"'{value}'")
                else:
                    values.append(str(value))
            values_list.append("(" + ", ".join(values) + ")")
        batch_insert_sql = insert_sql + ", ".join(values_list)

        # Execute the SQL insert statement for the current batch
        cur.execute(batch_insert_sql)
        # Calculate percentage completion
        processed_rows = start_idx + len(batch_data)
        percentage_completed = (processed_rows / num_rows) * 100
        percentage_remaining = 100 - percentage_completed

        # Print progress information
        print(
            f"Batch Insertion Progress: {percentage_completed:.2f}% completed, {percentage_remaining:.2f}% remaining"
        )

    # Commit the transaction after all batches have been inserted
    cur.connection.commit()
    print("Data inserted into the table.")
    return insert_sql
