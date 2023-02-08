import errno
import json
import os
import types
import typing as t


class ConfigAttribute:
    """Makes an attribute forward to the config"""

    def __init__(self, name: str, get_converter: t.Optional[t.Callable] = None) -> None:
        self.__name__ = name
        self.get_converter = get_converter

    def __get__(self, obj: t.Any, owner: t.Any = None) -> t.Any:
        if obj is None:
            return self
        rv = obj.config[self.__name__]
        if self.get_converter is not None:
            rv = self.get_converter(rv)
        return rv

    def __set__(self, obj: t.Any, value: t.Any) -> None:
        obj.config[self.__name__] = value


class Config(dict):
    """Works exactly like a dict but provides ways to fill it from files
    or special dictionaries.  There are two common patterns to populate the
    config.
    Either you can fill the config from a config file::
        app.config.from_pyfile('yourconfig.cfg')
    Or alternatively you can define the configuration options in the
    module that calls :meth:`from_object` or provide an import path to
    a module that should be loaded.  It is also possible to tell it to
    use the same module and with that provide the configuration values
    just before the call::
        DEBUG = True
        SECRET_KEY = 'development key'
        app.config.from_object(__name__)
    In both cases (loading from any Python file or loading from modules),
    only uppercase keys are added to the config.  This makes it possible to use
    lowercase values in the config file for temporary values that are not added
    to the config or to define the config keys in the same file that implements
    the application.
    Probably the most interesting way to load configurations is from an
    environment variable pointing to a file::
        app.config.from_envvar('YOURAPPLICATION_SETTINGS')
    In this case before launching the application you have to set this
    environment variable to the file you want to use.  On Linux and OS X
    use the export statement::
        export YOURAPPLICATION_SETTINGS='/path/to/config/file'
    On windows use `set` instead.
    :param root_path: path to which files are read relative from.  When the
                      config object is created by the application, this is
                      the application's :attr:`~flask.Flask.root_path`.
    :param defaults: an optional dictionary of default values
    """

    def __init__(self, defaults: t.Optional[dict] = None) -> None:
        super().__init__(defaults or {})


    def from_prefixed_env(
        self, prefix: str = "PIT", *, loads: t.Callable[[str], t.Any] = json.loads
    ) -> bool:
        """Load any environment variables that start with ``FLASK_``,
        dropping the prefix from the env key for the config key. Values
        are passed through a loading function to attempt to convert them
        to more specific types than strings.
        Keys are loaded in :func:`sorted` order.
        The default loading function attempts to parse values as any
        valid JSON type, including dicts and lists.
        Specific items in nested dicts can be set by separating the
        keys with double underscores (``__``). If an intermediate key
        doesn't exist, it will be initialized to an empty dict.
        :param prefix: Load env vars that start with this prefix,
            separated with an underscore (``_``).
        :param loads: Pass each string value to this function and use
            the returned value as the config value. If any error is
            raised it is ignored and the value remains a string. The
            default is :func:`json.loads`.
        .. versionadded:: 2.1
        """
        prefix = f"{prefix}_"
        len_prefix = len(prefix)

        for key in sorted(os.environ):
            if not key.startswith(prefix):
                continue

            value = os.environ[key]

            try:
                value = loads(value)
            except Exception:
                # Keep the value as a string if loading failed.
                pass

            # Change to key.removeprefix(prefix) on Python >= 3.9.
            key = key[len_prefix:]

            if "__" not in key:
                # A non-nested key, set directly.
                self[key] = value
                continue

            # Traverse nested dictionaries with keys separated by "__".
            current = self
            *parts, tail = key.split("__")

            for part in parts:
                # If an intermediate dict does not exist, create it.
                if part not in current:
                    current[part] = {}

                current = current[part]

            current[tail] = value

        return True


    def from_object(self, obj: t.Union[object, str]) -> None:
        """Updates the values from the given object.  An object can be of one
        of the following two types:
        -   a string: in this case the object with that name will be imported
        -   an actual object reference: that object is used directly
        Objects are usually either modules or classes. :meth:`from_object`
        loads only the uppercase attributes of the module/class. A ``dict``
        object will not work with :meth:`from_object` because the keys of a
        ``dict`` are not attributes of the ``dict`` class.
        Example of module-based configuration::
            app.config.from_object('yourapplication.default_config')
            from yourapplication import default_config
            app.config.from_object(default_config)
        Nothing is done to the object before loading. If the object is a
        class and has ``@property`` attributes, it needs to be
        instantiated before being passed to this method.
        You should not use this function to load the actual configuration but
        rather configuration defaults.  The actual config should be loaded
        with :meth:`from_pyfile` and ideally from a location not within the
        package because the package might be installed system wide.
        See :ref:`config-dev-prod` for an example of class-based configuration
        using :meth:`from_object`.
        :param obj: an import name or object
        """
        # if isinstance(obj, str):
        #     obj = import_string(obj)
        for key in dir(obj):
            if key.isupper():
                self[key] = getattr(obj, key)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {dict.__repr__(self)}>"
