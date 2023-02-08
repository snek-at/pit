import os
import typing as t

from .cli import cli
from .config import Config
from .database import Database
from .file import File
from .helpers import get_root_path


class Scaffold:
    """Common behavior shared between :class:`~flask.Flask` and
    :class:`~flask.blueprints.Blueprint`.
    :param import_name: The import name of the module where this object
        is defined. Usually :attr:`__name__` should be used.
    :param static_folder: Path to a folder of static files to serve.
        If this is set, a static route will be added.
    :param static_url_path: URL prefix for the static route.
    :param template_folder: Path to a folder containing template files.
        for rendering. If this is set, a Jinja loader will be added.
    :param root_path: The path that static, template, and resource files
        are relative to. Typically not set, it is discovered based on
        the ``import_name``.
    .. versionadded:: 2.0
    """

    name: str
    _model_folder: t.Optional[str] = None

    #: The class that is used for the ``config`` attribute of this app.
    #: Defaults to :class:`~flask.Config`.
    #:
    #: Example use cases for a custom class:
    #:
    #: 1. Default values for certain config options.
    #: 2. Access to config values through attributes in addition to keys.
    #:
    #: .. versionadded:: 0.11
    config_class = Config
    file_class = File
    database_class = Database

    cli = cli

    def __init__(
        self,
        import_name: str,
        model_folder: t.Optional[t.Union[str, os.PathLike]] = None,
        root_path: t.Optional[str] = None,
    ):
        #: The name of the package or module that this object belongs
        #: to. Do not change this once it is set by the constructor.
        self.import_name = import_name
        self.name = import_name

        self.model_folder = model_folder  # type: ignore

        if root_path is None:
            root_path = get_root_path(self.import_name)

        #: Absolute path to the package on the filesystem. Used to look
        #: up resources contained in the package.
        self.root_path = root_path

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.name!r}>"

    @property
    def model_folder(self) -> t.Optional[str]:
        """The absolute path to the configured static folder. ``None``
        if no model folder is set.
        """
        if self._model_folder is not None:
            return os.path.join(self.root_path, self._model_folder)
        else:
            return None

    @model_folder.setter
    def model_folder(self, value: t.Optional[t.Union[str, os.PathLike]]) -> None:
        if value is not None:
            value = os.fspath(value).rstrip(r"\/")

        self._model_folder = value

    @property
    def has_model_folder(self) -> bool:
        """``True`` if :attr:`static_folder` is set.
        .. versionadded:: 0.5
        """
        return self.model_folder is not None
