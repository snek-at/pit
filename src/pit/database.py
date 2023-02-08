import argparse
import json
import os
import sys
import typing as t
from datetime import timedelta

import duckdb
import pandas as pd

from .config import Config, ConfigAttribute


class Database:
    """
    A class that interacts with a database stored in memory using DuckDB.
    """

    def __init__(self):
        """
        Initialize a connection to the database stored in memory.
        """
        self.connection = duckdb.connect(':memory:')
        self.cursor = self.connection.cursor()

    def execute(self, query: str):
        """
        Execute a query on the database.
        :param query: query to execute
        :type query: str
        """
        self.cursor.execute(query)

    def query(self, query: str):
        """
        Execute a query on the database.
        :param query: query to execute
        :type query: str
        """
        return self.connection.execute(query)

    def create_table(self, table: str, data: dict):
        """
        Create a table in the database.
        :param table: name of the table
        :type table: str
        :param data: json object to be dumped
        :type data: dict
        """
        # Convert JSON object to pandas dataframe
        df: pd.DataFrame = pd.read_json(data, orient='records', dtype=False)

        # Export the table as a Parquet file
        self.connection.execute(
            f"CREATE TABLE {table} AS SELECT * FROM df")

    def dump(self, path: str, table: str, data: dict):
        """
        Dumps a json object to a parquet file
        :param path: path of the parquet file
        :type path: str
        :param table: name of the table
        :type table: str
        :param data: json object to be dumped
        :type data: dict
        """
        # Convert JSON object to pandas dataframe
        df: pd.DataFrame = pd.read_json(data, orient='records', dtype=False)

        # Export the table as a Parquet file
        self.connection.execute(
            f"COPY '{table}' TO '{path}' (FORMAT PARQUET)")

    def retrieve(self, table: str) -> dict:
        """
        Loads a parquet file and returns it as json object
        :param path: path of the parquet file
        :type path: str
        :return: json object
        :rtype: dict
        """
        # Loading parquet file into duckdb
        df: pd.DataFrame = self.connection.execute(
            f"SELECT * FROM '{table}'"
        ).to_df()

        # Convert the dataframe to dictionary
        res = json.loads(df.to_json(orient='records'))

        # Return the result
        return res

    def search(self, table: str, column_value_pairs: dict[str, str]) -> dict:
        """
        Search specific values in specific columns of the parquet file
        :param path: path of the parquet file
        :type table: str
        :param column_value_pairs: list of tuple where each tuple contains the
                                   the column name and the value to search
        :type column_value_pairs: list of tuple
        :return: the rows that match the search criteria
        :rtype: dict
        """

        # Create the query
        query: str = f"SELECT * FROM '{table}' WHERE "
        # Converte column_value_pairs "<column>=<value>" to join them with AND
        query += " AND ".join(
            [f"{key}='{column_value_pairs[key]}'" for key in column_value_pairs.keys()])

        # Execute the query
        df: pd.DataFrame = duckdb.query(query).to_df()

        # Convert the dataframe to dictionary
        res = json.loads(df.to_json(orient='records'))

        # Return the result
        return res

    def close_connection(self):
        """
        Closes the connection to DuckDB
        """
        self.cursor.close()
        self.connection.close()
