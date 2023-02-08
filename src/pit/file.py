import argparse
import json
import os
import sys
import typing as t
from datetime import timedelta

import duckdb
import pandas as pd

from .config import Config, ConfigAttribute


class File:
    """
    A class for loading and dumping data in JSON and Parquet formats using DuckDB and pandas.
    """

    @staticmethod
    def connection(config: dict = dict()) -> duckdb.DuckDBPyConnection:
        """
        Connect to S3 if credentials are provided
        :return: DuckDBPyConnection
        :rtype: duckdb.DuckDBPyConnection
        """
        # Create a connection to the database
        connection = duckdb.connect(':memory:')

        # Connect to S3 if credentials are provided
        if config.get('AWS_ACCESS_KEY_ID'):
            # Install httpfs extension
            if config.get('IS_OFFLINE'):
                connection.execute("INSTALL httpfs;")

            # Load httpfs extension
            connection.execute("LOAD httpfs;")

            # Connect to S3
            connection.execute(f"SET s3_region='{config.get('AWS_REGION')}';")
            connection.execute(
                f"SET s3_access_key_id='{config.get('AWS_ACCESS_KEY_ID')}';")
            connection.execute(
                f"SET s3_secret_access_key='{config.get('AWS_SECRET_ACCESS_KEY')}';")

            if config.get('s3_session_token'):
                connection.execute(
                    f"SET s3_session_token='{config.get('AWS_SESSION_TOKEN')}';")

        # Return duckdb connection
        return connection

    @staticmethod
    def dump(path: str, data: str, config: dict = dict()):
        """
        Dumps a json object to a parquet file
        :param path: path of the parquet file
        :type path: str
        :param data: json object to be dumped
        :type data: dict
        """

        # Create a connection to storage
        with File.connection(config) as connection:

            # Convert JSON object to pandas dataframe
            df: pd.DataFrame = pd.read_json(
                data, orient='records', dtype=False)

            # Export the table as a Parquet file
            connection.query(
                f"COPY df TO '{path}' (FORMAT PARQUET)")

    @staticmethod
    def retrieve(paths: list[str], config: dict = dict()) -> list[dict]:
        """
        Loads one or more parquet files and returns it as json object
        :param paths: paths of parquet files
        :type paths: list[str]
        :param config: configuration
        :type config: dict
        :return: merged records
        :rtype: list[dict]
        """

        # Default result is empty list
        res: list[dict] = []

        # Convert paths to a string
        path_str: str = ', '.join(
            ['\'' + path + '\'' for path in paths])

        # Create a connection to storage
        with File.connection(config) as connection:

            # Loading parquet files into duckdb
            rel = connection.from_query(f"SELECT * FROM {path_str}")

            # Will be available in the next release of DuckDB
            # rel = connection.from_parquet(paths)

            # Convert rel to dataframe
            df: pd.DataFrame = rel.to_df()

            # Convert rel to list of dictionary
            res = json.loads(rel.to_df().to_json(orient='records'))

        # Return the result
        return res

    @staticmethod
    def search(paths: list[str], column_value_pairs: dict[str, str], config: dict = dict()) -> list[dict]:
        """
        Search specific values in specific columns of one or more parquet files
        :param paths: paths of parquet files
        :type paths: list[str]
        :param column_value_pairs: column value pairs to search for
        :type column_value_pairs: dict[str, str]
        :param config: configuration
        :type config: dict
        :return: merged records that match the search criteria
        :rtype: list[dict]
        """

        # Default result is empty list
        res: list[dict] = []

        # Convert paths to a string
        path_str: str = ', '.join(
            ['\'' + path + '\'' for path in paths])

        # Create a connection to storage
        with File.connection(config) as connection:

            # Loading parquet files into duckdb
            rel = connection.from_query(f"SELECT * FROM {path_str}")

            # Will be available in the next release of DuckDB
            # rel = connection.from_parquet(paths)

            # Get unique columns with types of rel object
            rel_collums: dict[str, str] = dict(
                set(zip(rel.columns, rel.types)))

            # Create query to get only unique columns from rel object
            query: str = f'SELECT {",".join(rel_collums.keys())} FROM rel_view'

            # Get rel object with only unique columns
            rel = rel.query('rel_view', query)

            # Filter rel for each column value pair
            rel = rel.filter(' AND '.join(
                [f"{key}='{column_value_pairs[key]}'" for key in column_value_pairs.keys()]))

            # Convert rel to list of dictionary
            res = json.loads(rel.to_df().to_json(orient='records'))

        # Return the result
        return res
