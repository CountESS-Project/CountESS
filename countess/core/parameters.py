import hashlib
import os.path
import re
from typing import Any, Iterable, Mapping, Optional

PARAM_DIGEST_HASH = "sha256"


class BaseParam:
    """Represents the parameters which can be set on a plugin."""

    label: str = ""
    value: Any = None

    def copy(self):
        """Plugins declare their parameters with instances of BaseParam, these
        need to be copied so that multiple Plugins (and multiple rows in an
        ArrayParam) can have distinct values"""
        raise NotImplementedError(f"Implement {self.__class__.__name__}.copy()")

    def set_value(self, value):
        self.value = value
        return self

    def get_parameters(self, key, base_dir="."):
        return ((key, self.value),)

    def get_hash_value(self):
        digest = hashlib.new(PARAM_DIGEST_HASH)
        digest.update(repr(self.value).encode("utf-8"))
        return digest.hexdigest()

    def set_column_choices(self, choices):
        pass


class SimpleParam(BaseParam):
    """A SimpleParam has a single value"""

    var_type: type = type(None)
    read_only: bool = False

    def __init__(self, label: str, value=None, read_only: bool = False):
        self.label = label
        if value is not None:
            self.value = value
        if read_only:
            self.read_only = True

    def clean_value(self, value):
        return self.var_type(value)

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = self.clean_value(value)

    @value.deleter
    def value(self):
        self._value = None

    def copy(self):
        return self.__class__(self.label, self.value, self.read_only)


class BooleanParam(SimpleParam):
    var_type = bool
    _value: bool = False

    def clean_value(self, value):
        if isinstance(value, str):
            if value in ("true", "True", "1"):
                return True
            if value in ("false", "False", "0"):
                return False
            raise ValueError(f"Can't convert {value} to boolean")
        return bool(value)


class IntegerParam(SimpleParam):
    var_type = int
    _value: int = 0


class FloatParam(SimpleParam):
    var_type = float
    _value: float = 0.0


class StringParam(SimpleParam):
    var_type = str
    _value: str = ""


class TextParam(StringParam):
    def clean_value(self, value):
        return re.sub("\n\n\n+", "\n\n", value)


class StringCharacterSetParam(StringParam):
    """A StringParam limited to characters from `character_set`.
    Call `clean_value` to get an acceptable value"""

    character_set: set[str] = set()

    def __init__(
        self,
        label: str,
        value=None,
        read_only: bool = False,
        *,
        character_set: Optional[set[str]] = None,
    ):
        super().__init__(label, value, read_only)
        if character_set is not None:
            self.character_set = character_set

    def clean_character(self, c: str):
        if c in self.character_set:
            return c
        elif c.upper() in self.character_set:
            return c.upper()
        else:
            return ""

    def clean_value(self, value: Any):
        value_str = str(value)
        x = "".join([self.clean_character(c) for c in value_str])
        return x

    def copy(self):
        return self.__class__(
            self.label, self.value, self.read_only, character_set=self.character_set
        )


class FileParam(StringParam):
    """A StringParam for holding a filename.  Defaults to `read_only` because
    it really should be populated from a file dialog or simiar."""

    file_types = [("Any", "*")]

    _hash = None

    def __init__(self, label: str, value=None, read_only: bool = True, file_types=None):
        super().__init__(label, value, read_only)
        if file_types is not None:
            self.file_types = file_types

    def get_file_hash(self):
        if not self.value:
            return "0"
        try:
            with open(self.value, "rb") as file:
                try:
                    # Python 3.11
                    digest = hashlib.file_digest(file, PARAM_DIGEST_HASH)
                except AttributeError:
                    digest = hashlib.new(PARAM_DIGEST_HASH)
                    while True:
                        data = file.read()
                        if not data:
                            break
                        digest.update(data)
            return digest.hexdigest()
        except IOError:
            return "0"

    def get_parameters(self, key, base_dir="."):
        return ((key, os.path.relpath(self.value, base_dir)),)

    def copy(self):
        return self.__class__(self.label, self.value, self.read_only, file_types=self.file_types)

    def get_hash_value(self) -> str:
        # For reproducability, we don't actually care about the filename, just
        # its hash.
        if self._hash is None:
            self._hash = self.get_file_hash()
        return self._hash


class FileSaveParam(StringParam):
    file_types = [("Any", "*")]

    def __init__(self, label: str, value=None, read_only: bool = False, file_types=None):
        super().__init__(label, value, read_only)
        if file_types is not None:
            self.file_types = file_types

    def clean_value(self, value: str | tuple | list, file_types=None):
        try:
            return os.path.relpath(str(value))
        except ValueError:
            return value


class ChoiceParam(BaseParam):
    """A drop-down menu parameter choosing between options.
    Defaults to 'None'"""

    _value: Optional[str] = None

    def __init__(
        self,
        label: str,
        value: Optional[str] = None,
        choices: Optional[Iterable[str]] = None,
    ):
        self.label = label
        self._value = value
        self.choices = list(choices or [])

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        if value in self.choices:
            self._value = value
        else:
            self._value = None

    def set_choices(self, choices: Iterable[str]):
        self.choices = list(choices)
        if len(self.choices) == 1:
            self._value = list(choices)[0]
        elif self.value not in self.choices:
            self._value = None

    def copy(self):
        return self.__class__(self.label, self.value, self.choices)


class DataTypeChoiceParam(ChoiceParam):
    DATA_TYPES: Mapping[str, Optional[tuple[type, Any]]] = {
        "string": (str, None),
        "number": (float, None),
        "integer": (int, 0),
        "boolean": (bool, None),
    }

    def __init__(
        self, label: str, value: Optional[str] = None, choices: Optional[Iterable[str]] = None
    ):
        if not choices:
            choices = list(self.DATA_TYPES.keys())
        super().__init__(label, value, choices)

    def get_selected_type(self):
        if self.value is None:
            return None
        else:
            return self.DATA_TYPES[self.value][0]

    def cast_value(self, value):
        if self.value is None:
            return None
        if value is None:
            return self.DATA_TYPES[self.value][1]
        else:
            return self.DATA_TYPES[self.value][0](value)


class DataTypeOrNoneChoiceParam(DataTypeChoiceParam):
    NONE_VALUE = "— NONE —"

    DATA_TYPES = {
        "string": (str, None),
        "number": (float, None),
        "integer": (int, 0),
        "boolean": (bool, None),
        NONE_VALUE: None,
    }

    def __init__(
        self, label: str, value: Optional[str] = None, choices: Optional[Iterable[str]] = None
    ):
        if not choices:
            choices = list(self.DATA_TYPES.keys())
        if value is None:
            value = self.NONE_VALUE
        super().__init__(label, value, choices)

    def get_selected_type(self):
        if self.value == self.NONE_VALUE:
            return None
        else:
            return super().get_selected_type()

    def cast_value(self, value):
        if self.value == self.NONE_VALUE:
            return None
        else:
            return super().cast_value(value)

    def is_none(self):
        return self.value == self.NONE_VALUE


class ColumnChoiceParam(ChoiceParam):
    """A ChoiceParam which DaskTransformPlugin knows
    it should automatically update with a list of columns"""

    def set_column_choices(self, choices):
        self.set_choices(list(choices))

class ColumnOrIndexChoiceParam(ColumnChoiceParam):
    INDEX_VALUE = "— INDEX —"

    def set_choices(self, choices: Iterable[str]):
        super().set_choices([self.INDEX_VALUE] + list(choices))

    def is_index(self):
        return self.value == self.INDEX_VALUE


class ColumnOrNoneChoiceParam(ColumnChoiceParam):
    NONE_VALUE = "— NONE —"

    def set_choices(self, choices: Iterable[str]):
        super().set_choices([self.NONE_VALUE] + list(choices))

    def is_none(self):
        return self.value == self.NONE_VALUE


class ArrayParam(BaseParam):
    """An ArrayParam contains zero or more copies of `param`, which can be a
    SimpleParam or a MultiParam."""

    # XXX the only real use for this is as an array of MultiParams so I think
    # maybe we should have a TabularParam which combines ArrayParam and
    # MultiParam more directly."""

    params: list[BaseParam] = []

    def __init__(
        self,
        label: str,
        param: BaseParam,
        read_only: bool = False,
        min_size: int = 0,
        max_size: Optional[int] = None,
    ):
        self.label = label
        self.param = param
        self.read_only = read_only

        self.params = [param.copy() for n in range(0, min_size)]
        self.relabel()
        self.min_size = min_size
        self.max_size = max_size

    def add_row(self):
        # XXX probably should throw an exception instead of just ignoring
        if self.max_size is None or len(self.params) < self.max_size:
            pp = self.param.copy()
            self.params.append(pp)
            self.relabel()
            return pp
        return None

    def del_row(self, position: int):
        assert 0 <= position < len(self.params)

        self.params.pop(position)
        if len(self.params) < self.min_size:
            self.params.append(self.param.copy())
        self.relabel()

    def del_subparam(self, param: BaseParam):
        self.params.remove(param)
        self.relabel()

    def relabel(self):
        for n, param in enumerate(self.params):
            param.label = self.param.label + f" {n+1}"

    def copy(self) -> "ArrayParam":
        return self.__class__(self.label, self.param, self.read_only, self.min_size, self.max_size)

    def __len__(self):
        return len(self.params)

    def __getitem__(self, key):
        while len(self.params) <= int(key):
            self.add_row()
        return self.params[int(key)]

    def __contains__(self, item):
        return item in self.params

    def __iter__(self):
        return self.params.__iter__()

    @property
    def value(self):
        return [p.value for p in self.params]

    @value.setter
    def value(self, value):
        # if setting to a dictionary, keep only the values in order
        # and forget about the numbering.

        if isinstance(value, dict):
            values = sorted([(int(k), v) for k, v in value.items()])
            value = [v[1] for v in values]

        self.params = [self.param.copy().set_value(v) for v in value]

    @value.deleter
    def value(self):
        self.params = []

    def get_parameters(self, key, base_dir="."):
        for n, p in enumerate(self.params):
            yield from p.get_parameters(f"{key}.{n}", base_dir)

    def get_hash_value(self):
        digest = hashlib.new(PARAM_DIGEST_HASH)
        for p in self.params:
            digest.update(p.get_hash_value().encode("utf-8"))
        return digest.hexdigest()

    def set_column_choices(self, choices):
        self.param.set_column_choices(choices)
        for p in self.params:
            p.set_column_choices(choices)

class FileArrayParam(ArrayParam):
    """FileArrayParam is an ArrayParam arranged per-file.  Using this class
    really just marks it as expecting to be populated from an open file
    dialog."""

    def find_fileparam(self):
        if isinstance(self.param, FileParam):
            return self.param
        if isinstance(self.param, MultiParam):
            for pp in self.param.params.values():
                if isinstance(pp, FileParam):
                    return pp
        raise TypeError("FileArrayParam needs a FileParam inside it")

    @property
    def file_types(self):
        return self.find_fileparam().file_types

    def add_files(self, filenames):
        # XXX slightly daft way of doing it.  It is setting the filename
        # of the 'template' param, and then copying that template to
        # make the new param.
        for filename in filenames:
            self.find_fileparam().value = filename
            self.add_row()


class MultiParam(BaseParam):
    params: Mapping[str, BaseParam] = {}

    def __init__(self, label: str, params: Mapping[str, BaseParam]):
        self.label = label
        self.params = params

    def copy(self) -> "MultiParam":
        pp = dict(((k, p.copy()) for k, p in self.params.items()))
        return self.__class__(self.label, pp)

    def __getitem__(self, key):
        return self.params[key]

    def keys(self):
        return self.params.keys()

    def values(self):
        return self.params.values()

    def items(self):
        return self.params.items()

    def __getattr__(self, name):
        try:
            return self.params[name]
        except KeyError as exc:
            raise AttributeError(name=name, obj=self) from exc

    def __contains__(self, item):
        return item in self.params

    def __iter__(self):
        return self.params.__iter__()

    @property
    def value(self):
        return dict((k, p.value) for k, p in self.params.items())

    @value.setter
    def value(self, value):
        for k, v in value.items():
            self.params[k].value = v

    @value.deleter
    def value(self):
        for p in self.params.values():
            del p.value

    def get_parameters(self, key, base_dir="."):
        for k, p in self.params.items():
            yield from p.get_parameters(f"{key}.{k}", base_dir)

    def get_hash_value(self):
        digest = hashlib.new(PARAM_DIGEST_HASH)
        for k, p in self.params.items():
            digest.update((k + "\0" + p.get_hash_value()).encode("utf-8"))
        return digest.hexdigest()

    def set_column_choices(self, choices):
        for p in self.params.values():
            p.set_column_choices(choices)
