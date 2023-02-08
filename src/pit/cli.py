import argparse


class SplitArgs(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        # Transform values from "key1=value1,key2=value2" to array of dictionaries
        # [{'key1', 'value1'}, {'key2', 'value2'}]
        # res = [dict([value.split('=')]) for value in values.split(',')]

        res = dict([value.split('=') for value in values.split(' ')])

        # Set the values to the namespace
        setattr(namespace, self.dest, res)


def cli():
    """
    cli function is used to parse the command line arguments
    :return: None
    """
    # Create an instance of the argparse.ArgumentParser class
    parser = argparse.ArgumentParser()

    # Create an instance of the argparse.ArgumentParser class
    parser = argparse.ArgumentParser()

    # Add the subcommands to the parser
    commands_group = parser.add_subparsers(dest='command')
    dump_parser = commands_group.add_parser('dump')
    dump_parser.add_argument('paths', nargs='+', type=str)
    # dump_parser.add_argument('--file', type=str, required=True)
    dump_parser.add_argument('--data', type=str, required=True)
    dump_parser.add_argument(
        '--mode', type=str, choices=['file', 'database'], default='file')
    retrieve_parser = commands_group.add_parser('retrieve')
    retrieve_parser.add_argument('paths', nargs='+', type=str)
    retrieve_parser.add_argument(
        '--mode', type=str, choices=['file', 'database'], default='file')
    # retrieve_exclusive_group = retrieve_parser.add_mutually_exclusive_group(
    #     required=True)
    # retrieve_exclusive_group.add_argument('--file', type=str)
    # retrieve_exclusive_group.add_argument('--directory', type=str)
    search_parser = commands_group.add_parser('search')
    search_parser.add_argument('paths', nargs='+', type=str)
    search_parser.add_argument(
        '--mode', type=str, choices=['file', 'database'], default='file')
    # search_exclusive_group = search_parser.add_mutually_exclusive_group(
    #     required=True)
    # search_exclusive_group.add_argument('--file', type=str)
    # search_exclusive_group.add_argument(
    #     '--directory', type=str, action='append')
    search_parser.add_argument(
        '--column_value_pairs', action=SplitArgs, required=True)

    # Parse the arguments
    return parser.parse_args()
