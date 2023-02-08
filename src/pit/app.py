import argparse
import json
import os
import sys
import typing as t
from datetime import timedelta

import duckdb
import pandas as pd

from .config import Config, ConfigAttribute
from .scaffold import Scaffold

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
                      "Usage: python script.py search path/to/file.parquet "\
                      "column_name1=value1 column_name2=value2"


class Pit(Scaffold):

    #: Default configuration parameters.
    default_config = dict(
        {
            "ENV": None,
            "DEBUG": None,
        }
    )

    def __init__(
        self,
        import_name: str,
        model_folder: t.Optional[t.Union[str, os.PathLike]] = "static",
        instance_path: t.Optional[str] = None,
        instance_relative_config: bool = False,
        root_path: t.Optional[str] = None,
    ):
        super().__init__(
            import_name=import_name,
            model_folder=model_folder,
            root_path=root_path,
        )

        #: The configuration dictionary as :class:`Config`.  This behaves
        #: exactly like a regular dictionary but supports additional methods
        #: to load a config from files.
        self.config = self.make_config(instance_relative_config)

        #: a place where extensions can store application specific state.  For
        #: example this is where an extension could store database engines and
        #: similar things.
        #:
        #: The key must match the name of the extension module. For example in
        #: case of a "Flask-Foo" extension in `flask_foo`, the key would be
        #: ``'foo'``.
        #:
        #: .. versionadded:: 0.7
        self.extensions: dict = {}

    def main(self, *args: str, **kwargs: str) -> None:
        """
        main function is used to load the parquet file and returns it as json object
        :param args: command and file name
        :type args: str
        :return: None
        """

        # Call commands method and pass command and file name
        res: dict = self.commands(*args[1:], cmd=args[0], **kwargs)

        # print the json object
        print(json.dumps(res))

    def make_config(self, instance_relative: bool = False) -> Config:
        """Used to create the config attribute by the Flask constructor.
        The `instance_relative` parameter is passed in from the constructor
        of Flask (there named `instance_relative_config`) and indicates if
        the config should be relative to the instance path or the root path
        of the application.
        .. versionadded:: 0.8
        """
        root_path = self.root_path

        defaults = dict(self.default_config)

        return self.config_class(defaults)

    @staticmethod
    def help_function(cmd: str) -> None:
        """
        Prints the help information for the specified command
        : param cmd: command for which help is needed
        : type cmd: str
        """
        {
            'dump': lambda: print(DUMP_COMMAND_HELP),
            'retrieve': lambda: print(RETRIEVE_COMMAND_HELP),
            'search': lambda: print(SEARCH_COMMAND_HELP),
        }.get(cmd, lambda: print(f"{cmd} command not found"))()

    def commands(self, *args, obj: object = None, cmd: str = "help", **kwargs) -> any:  # None | dict
        """
        Executes the specified command
        : param self: Pit instance
        : type self: Self@Pit
        : param cmd: command to be executed
        : type cmd: str
        : param args: arguments needed by the command
        : type args: tuple
        : return: None or json object
        : rtype: None | dict
        """
        # If self is None, create a new Database object
        # if isinstance(obj, Database):
        #     database: Database = obj
        # elif isinstance(obj, type(None)):
        #     pass
        # else:
        #     raise TypeError("Object must be a Database object or None")

        database = self.database_class()

        if kwargs["mode"] == "file":
            return {
                'dump': lambda: self.file_class.dump(path=kwargs["paths"][0], data=kwargs["data"], config=self.config),
                'retrieve': lambda: self.file_class.retrieve(paths=kwargs["paths"], config=self.config),
                'search': lambda: self.file_class.search(paths=kwargs["paths"], column_value_pairs=kwargs["column_value_pairs"], config=self.config),
                'help': lambda: self.help_function(cmd),
            }.get(cmd, lambda: "Invalid Command")()
        elif kwargs["mode"] == "database":
            return {
                'dump': lambda: database.dump(kwargs["data"]),
                'retrieve': lambda: database.retrieve(args[0]),
                'search': lambda: database.search(args[0], *args[1:]),
                'help': lambda: self.help_function(cmd),
            }.get(cmd, lambda: "Invalid Command")()
        else:
            # Throw an error if the mode is invalid. Mode can be either file or database
            raise ValueError(
                "Invalid mode. Mode can be either file or database")
