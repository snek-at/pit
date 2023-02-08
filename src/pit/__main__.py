from sys import argv

from .app import Pit
from .cli import cli

# Ensure that the script is being run directly
if __name__ == '__main__':
    # Call the main function and pass the commandline arguments
    Pit(__name__).main(*argv[1:], **vars(cli()))
