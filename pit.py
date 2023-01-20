import json
import sys

import duckdb
import pandas as pd

# Help messages
# Make sure to update the help messages if you change the commands
# or the arguments
DUMP_COMMAND_HELP = "The dump command takes json data and save it to a "\
                    "parquet file\n" \
                    "Usage: python script.py dump path/to/file.parquet data.json"
RETRIEVE_COMMAND_HELP = "The load command loads a parquet file and returns "\
                        "the data as json object\n" \
                        "Usage: python script.py load path/to/file.parquet"
SEARCH_COMMAND_HELP = "The search command search the specific column and " \
                      "value in the parquet file and returns the data as json " \
                      "object\n "\
                      "Usage: python script.py search path/to/file.parquet column_name1=value1 column_name2=value2"


def main(*args: str) -> None:
    """
    main function is used to load the parquet file and returns it as json object
    :param args: command and file name
    :type args: str
    :return: None
    """
    # Create an instance of the File class
    file = File()
    # call commands method and pass command and file name
    json_obj = commands(file, *args)
    # print the json object
    print(json.dumps(json_obj))
    # close the connection
    commands(file, "close_connection")


class File:
    """
    A class for loading and dumping data in JSON and Parquet formats using DuckDB and pandas.
    """

    def __init__(self):
        """
        Initializes the connection to DuckDB
        """
        self.conn = duckdb.connect()

    def dump(self, path: str, json_data: dict):
        """
        Dumps a json object to a parquet file
        :param path: path of the parquet file
        :type path: str
        :param json_data: json object to be dumped
        :type json_data: dict
        """
        # Convert JSON object to pandas dataframe
        df = pd.read_json(json_data, orient='records', dtype=False)

        # Create a temporary table in DuckDB
        # df.to_sql('json', self.conn, if_exists='replace',
        #           method='multi', index=False)

        # Create table with all alias and associated user for authentication.
        # Format:
        # user_id | alias   |
        # UUID    | VARCHAR |
        self.conn.execute(" \
            CREATE TABLE 'json' AS \
            SELECT * \
            FROM df \
        ")

        # Export the table as a Parquet file
        self.conn.execute(
            f"COPY json TO '{path}' (FORMAT PARQUET)")

    def retrieve(self, path: str) -> dict:
        """
        Loads a parquet file and returns it as json object
        :param path: path of the parquet file
        :type path: str
        :return: json object
        :rtype: dict
        """
        # Loading parquet file into duckdb
        df = duckdb.query(
            f"SELECT * FROM '{path}'"
        ).to_df()

        # Convert the dataframe to a JSON object
        res_json = json.loads(df.to_json(orient='records'))

        return res_json

    def search(self, path: str, *column_value_pairs: str) -> dict:
        """
        Search specific values in specific columns of the parquet file
        :param path: path of the parquet file
        :type path: str
        :param column_value_pairs: list of tuple where each tuple contains the the column name and the value to search
        :type column_value_pairs: list of tuple
        :return: the rows that match the search criteria
        :rtype: dict
        """
        # print(column_value_pairs)
        # Converte column_value_pairs to a list of tuple
        # column_value_pairs_obj = [tuple(pair) for pair in
        #                           column_value_pairs.split('=')]
        # Create the query

        query = f"SELECT * FROM '{path}' WHERE "
        # Converte column_value_pairs "<column>=<value>" to join them with AND
        query += " AND ".join(
            [f"{pair.split('=')[0]}='{pair.split('=')[1]}'" for pair in column_value_pairs])

        # Execute the query
        df = duckdb.query(query).to_df()
        json_data = df.to_json(orient='records')
        return json.loads(json_data)

    def close_connection(self):
        """
        Closes the connection to DuckDB
        """
        self.conn.close()

    def reset(self, json_data: dict, filename: str):
        self.conn.execute(" \
            CREATE TABLE _blacklist( \
                postId VARCHAR PRIMARY KEY \
            ); \
        ")
        self.conn.execute(" \
            INSERT INTO _blacklist \
            VALUES ('387019078721465_217073037359578'), ('2'), ('3'); \
        ")

        # Export the table as a Parquet file
        self.conn.execute(
            f"COPY _blacklist TO '{filename}' (FORMAT PARQUET)")


def help_function(cmd: str) -> None:
    """
    Prints the help information for the specified command
    :param cmd: command for which help is needed
    :type cmd: str
    """
    {
        'dump': lambda: print(DUMP_COMMAND_HELP),
        'retrieve': lambda: print(RETRIEVE_COMMAND_HELP),
        'search': lambda: print(SEARCH_COMMAND_HELP),
    }.get(cmd, lambda: print(f"{cmd} command not found"))()


def commands(cls, cmd: str, *args) -> any:  # None | dict
    """
    Executes the specified command
    :param cls: class object
    :type cls: class
    :param cmd: command to be executed
    :type cmd: str
    :param args: arguments needed by the command
    :type args: tuple
    :return: None or json object
    :rtype: None | dict
    """
    return {
        'retrieve': lambda: cls.retrieve(*args),
        'dump': lambda: cls.dump(*args),
        'search': lambda: cls.search(*args),
        'close_connection': lambda: cls.close_connection(),
        'help': lambda: help_function(cmd),
    }.get(cmd, lambda: "Invalid Command")()


if __name__ == '__main__':
    main(*sys.argv[1:])
