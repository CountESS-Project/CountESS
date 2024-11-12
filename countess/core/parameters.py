import hashlib
import math
import os.path
import re
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Type, Union

import pandas as pd

from countess.utils.pandas import get_all_columns

PARAM_DIGEST_HASH = "sha256"


def make_prefix_groups(strings: List[str]) -> Dict[str, List[str]]:
    groups: Dict[str, List[str]] = {}
    for s in strings:
        if m := re.match(r"(.*?_+)([^_]+)$", s):
            groups.setdefault(m.group(1), []).append(m.group(2))
    return {k: v for k, v in groups.items() if len(v) > 1}


class BaseParam:
    """Represents the parameters which can be set on a plugin."""

    label: str = ""
    hide: bool = False
    read_only: bool = False  # XXX deprecated
    value: Any = None  # XXX deprecated

    def __init__(self, label):
        self.label = label

    def copy(self):
        """Plugins declare their parameters with instances of BaseParam, these
        need to be copied so that multiple Plugins (and multiple rows in an
        ArrayParam) can have distinct values"""
        raise NotImplementedError(f"Implement {self.__class__.__name__}.copy()")

    def copy_and_set_value(self, value):
        new = self.copy()
        new.value = value
        return new

    def get_parameters(self, key, base_dir="."):
        raise NotImplementedError(f"Implement {self.__class__.__name__}.get_parameters()")

    def get_hash_value(self):
        raise NotImplementedError(f"Implement {self.__class__.__name__}.get_hash_value()")

    def set_column_choices(self, choices):
        pass


class ScalarParam(BaseParam):
    """A ScalarParam has a single value (use one of the subclasses below to give
    it a type)."""

    _value: Any = None

    def __init__(self, label, default=None):
        super().__init__(label)
        self.default = default
        self.reset_value()

    def get_value(self) -> Any:
        return self._value

    def set_value(self, value: Any):
        self._value = value

    def reset_value(self):
        self._value = self.default

    value = property(
        lambda self: self.get_value(), lambda self, value: self.set_value(value), lambda self: self.reset_value()
    )

    def copy(self):
        return self.__class__(self.label, self._value)

    def get_parameters(self, key, base_dir="."):
        return ((key, self.value),)

    def get_hash_value(self):
        digest = hashlib.new(PARAM_DIGEST_HASH)
        digest.update(repr(self.value).encode("utf-8"))
        return digest.hexdigest()


class ScalarWithOperatorsParam(ScalarParam):
    # Operator Methods which apply to both StringParams and
    # NumericParams (but not BooleanParam)

    def __add__(self, other):
        return self._value + other

    def __radd__(self, other):
        return other + self._value

    def __str__(self):
        return str(self._value)

    def __eq__(self, other):
        return self._value == other

    def __ne__(self, other):
        return self._value != other

    def __gt__(self, other):
        return self._value > other

    def __ge__(self, other):
        return self._value >= other

    def __lt__(self, other):
        return self._value < other

    def __le__(self, other):
        return self._value <= other


class StringParam(ScalarWithOperatorsParam):
    """A parameter representing a single string value.  A number
    of builtin methods are reproduced here to allow the parameter to be
    used pretty much like a normal string. In some circumstances it may
    be necessary to explicitly cast the parameter or use `parameter.value`
    property to get the value inside the parameter."""

    _value: Optional[str] = None

    def set_value(self, value: Any):
        self._value = str(value or "")

    # Operator methods which apply only to strings

    def __len__(self):
        return len(self._value) if self._value is not None else 0

    def __contains__(self, other):
        return other in self._value

    def __hash__(self):
        return hash(self._value)


class TextParam(StringParam):
    """This is mostly just a convenience for the GUI, it marks this as a
    long text field and also removes extra blank lines"""

    def set_value(self, value):
        self._value = re.sub("\n\n\n+", "\n\n", value or "")


class NumericParam(ScalarWithOperatorsParam):
    """A parameter representing a single numeric value.  A large number
    of builtin methods are reproduced here to allow the parameter to be
    used pretty much like a normal number. In some circumstances it may
    be necessary to explicitly cast the parameter or use `parameter.value`
    property to get the value inside the parameter."""

    var_type: type = type(None)

    def set_value(self, value):
        try:
            self._value = self.var_type(value)
        except (TypeError, ValueError):
            self.reset_value()

    # Operator methods which apply only to numerics

    def __sub__(self, other):
        return self._value - other

    def __rsub__(self, other):
        return other - self._value

    def __mul__(self, other):
        return self._value * other

    def __rmul__(self, other):
        return other * self._value

    def __int__(self):
        return int(self._value)

    def __float__(self):
        return float(self._value)

    def __abs__(self):
        return abs(self._value)

    def __pos__(self):
        return self._value

    def __neg__(self):
        return 0 - (self._value)

    # XXX should include many more numeric operator methods here, see
    # https://docs.python.org/3/reference/datamodel.html#emulating-numeric-types
    #   matmul, truediv, floordiv, mod, divmod, pow, lshift, rshift, and, xor, or,
    #   rmatmul, rtruediv, rfloordiv, rmod, rdivmod, rpow, rlshift, rrshift, rand, rxor, ror,
    #   neg, pos, abs, invert, complex, index, round, trunc, floor, ceil, format
    # it seems like there should be a smarter way to do this but doing it the
    # dumb way works with mypy and pylint.


class IntegerParam(NumericParam):
    _value: int = 0
    var_type = int


class FloatParam(NumericParam):
    _value: float = 0.0
    var_type = float


class DecimalParam(NumericParam):
    _value: Decimal = Decimal(0)
    var_type = Decimal


class BooleanParam(ScalarParam):
    _value: bool = False
    var_type = bool

    def set_value(self, value):
        if isinstance(value, str):
            if value in ("t", "T", "true", "True", "1"):
                self._value = True
            elif value in ("f", "F", "false", "False", "0"):
                self._value = False
            else:
                raise ValueError(f"Can't convert {value} to boolean")
        else:
            self._value = bool(value)

    def __bool__(self):
        return self._value or False

    def __str__(self):
        return str(self._value)

    # XXX are there other operator methods which need to be implemented here?


class StringCharacterSetParam(StringParam):
    """A StringParam limited to characters from `character_set`.
    Call `clean_value` to get an acceptable value"""

    character_set: set[str] = set()

    def __init__(
        self,
        label: str,
        value=None,
        *,
        character_set: Optional[set[str]] = None,
    ):
        super().__init__(label, value)
        if character_set is not None:
            self.character_set = character_set

    def clean_character(self, c: str):
        if c in self.character_set:
            return c
        elif c.upper() in self.character_set:
            return c.upper()
        else:
            return ""

    def set_value(self, value: Any):
        value_str = str(value or "")
        self._value = "".join([self.clean_character(c) for c in value_str])

    def copy(self) -> "StringCharacterSetParam":
        return self.__class__(self.label, self.value, character_set=self.character_set)


class FileParam(StringParam):
    """A StringParam for holding a filename."""

    file_types = [("Any", "*")]

    _hash = None

    def __init__(self, label: str, value=None, file_types=None):
        super().__init__(label, value)
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
                except AttributeError:  # pragma: no cover
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
        if self.value:
            if base_dir:
                path = os.path.relpath(self.value, base_dir)
            else:
                path = os.path.abspath(self.value)
        else:
            path = None

        return [(key, path)]

    def copy(self) -> "FileParam":
        return self.__class__(self.label, self.value, file_types=self.file_types)

    def get_hash_value(self) -> str:
        # For reproducability, we don't actually care about the filename, just
        # its hash.
        if self._hash is None:
            self._hash = self.get_file_hash()
        return self._hash


class FileSaveParam(StringParam):
    file_types = [("Any", "*")]

    def __init__(self, label: str, value=None, file_types=None):
        super().__init__(label, value)
        if file_types is not None:
            self.file_types = file_types


class DictChoiceParam(ScalarWithOperatorsParam):
    """A drop-down menu parameter choosing between options.
    Takes a mapping of choices where the key is the choice
    and the value is the displayed value."""

    _value: str = ""
    _choice: str = ""
    choices: dict[str, str]
    reverse: dict[str, str]

    def __init__(self, label: str, value: Optional[str] = None, choices: Optional[dict[str, str]] = None):
        super().__init__(label)
        self.set_choices(choices or {})
        self.set_value(value)

    def clean_value(self, value):
        return value

    def set_value(self, value):
        if value in self.reverse:
            self._choice = self.reverse[value]
            self._value = value
        elif value in self.choices:
            self._choice = value
            self._value = self.choices[value]
        else:
            self.set_default()

    def get_choice(self):
        return self._choice

    def set_choice(self, choice):
        if choice in self.choices:
            self._choice = choice
            self._value = self.choices[choice]
        else:
            self.set_default()

    choice = property(get_choice, set_choice)

    def set_choices(self, choices: dict[str, str]):
        self.choices = dict(choices)
        self.reverse = {v: k for k, v in choices.items()}
        if self._choice in self.choices:
            self._value = self.choices[self._choice]
        elif self._value in self.reverse:
            self._choice = self.reverse[self._value]
        else:
            self.set_default()

    def get_values(self):
        return list(self.choices.values())

    def set_default(self):
        if self.choices:
            self._choice, self._value = list(self.choices.items())[0]
        else:
            self._choice = ""
            self._value = ""

    def get_parameters(self, key, base_dir="."):
        return ((key, self._choice),)

    def copy(self) -> "DictChoiceParam":
        return self.__class__(self.label, self.value, self.choices)


class ChoiceParam(ScalarWithOperatorsParam):
    """A drop-down menu parameter choosing between options.
    Defaults to 'None'"""

    DEFAULT_VALUE = ""

    _value: Optional[str] = None
    _choice: Optional[int] = None
    choices: list[str] = []

    def __init__(
        self,
        label: str,
        value: Optional[str] = None,
        choices: Optional[Iterable[str]] = None,
    ):
        super().__init__(label)
        self.value = value if value is not None else self.DEFAULT_VALUE
        self.choices = list(choices or [])

    def clean_value(self, value):
        return value

    def set_value(self, value):
        if value is None:
            self._value = self.DEFAULT_VALUE
        else:
            self._value = self.clean_value(value)
        try:
            self._choice = self.choices.index(value)
        except ValueError:
            self._choice = None

    def get_choice(self):
        return self._choice

    def set_choice(self, choice):
        if choice is not None and 0 <= choice < len(self.choices):
            self._choice = choice
            self._value = self.choices[choice]
        else:
            self._choice = None
            self._value = self.DEFAULT_VALUE

    choice = property(get_choice, set_choice)

    def set_choices(self, choices: Iterable[str]):
        self.choices = list(choices)
        if self.choices:
            if self._value not in self.choices:
                self._value = self.choices[0]
                self._choice = 0
        else:
            self._value = self.DEFAULT_VALUE
            self._choice = None

    def get_values(self):
        return self.choices

    def copy(self) -> "ChoiceParam":
        return self.__class__(self.label, self.value, self.choices)


class DataTypeChoiceParam(ChoiceParam):
    DATA_TYPES: Mapping[str, tuple[type, Any, Type[ScalarParam]]] = {
        "string": (str, "", StringParam),
        "number": (float, math.nan, FloatParam),
        "integer": (int, 0, IntegerParam),
        "boolean": (bool, False, BooleanParam),
    }

    def __init__(self, label: str, value: Optional[str] = None, choices: Optional[Iterable[str]] = None):
        if not choices:
            choices = list(self.DATA_TYPES.keys())
        super().__init__(label, value, choices)

    def get_selected_type(self):
        try:
            return self.DATA_TYPES[self.value][0]
        except KeyError:
            return None

    def cast_value(self, value):
        if value is not None:
            try:
                return self.DATA_TYPES[self.value][0](value)
            except ValueError:
                pass
        return self.DATA_TYPES[self.value][1]

    def get_parameter(self, label: str, value=None) -> BaseParam:
        return self.DATA_TYPES[self.value][2](label, value)


class DataTypeOrNoneChoiceParam(DataTypeChoiceParam):
    DEFAULT_VALUE = "— NONE —"

    def __init__(self, label: str, value: Optional[str] = None, choices: Optional[Iterable[str]] = None):
        if not choices:
            choices = list(self.DATA_TYPES.keys()) + [self.DEFAULT_VALUE]
        super().__init__(label, value, choices)

    def get_selected_type(self):
        if self.value == self.DEFAULT_VALUE:
            return None
        else:
            return super().get_selected_type()

    def cast_value(self, value):
        if self.value == self.DEFAULT_VALUE:
            return None
        else:
            return super().cast_value(value)

    def is_none(self):
        return self.value == self.DEFAULT_VALUE

    def is_not_none(self):
        return self.value != self.DEFAULT_VALUE


def _dataframe_get_column(df: pd.DataFrame, col: str):
    if col in df.columns:
        return df[col]
    elif col == df.index.name:
        return df.index.to_series()
    elif hasattr(df.index, "names") and col in df.index.names:
        return df.index.to_frame()[col]
    else:
        raise ValueError(f"Column {col} not found")


class ColumnChoiceParam(ChoiceParam):
    """A ChoiceParam which DaskTransformPlugin knows
    it should automatically update with a list of columns"""

    def set_column_choices(self, choices):
        self.set_choices(list(choices))

    def get_column(self, df):
        return _dataframe_get_column(df, self.value)


class ColumnOrNoneChoiceParam(ColumnChoiceParam):
    DEFAULT_VALUE = "— NONE —"

    def set_choices(self, choices: Iterable[str]):
        super().set_choices([self.DEFAULT_VALUE] + list(choices))

    def is_none(self):
        return self.value == self.DEFAULT_VALUE

    def is_not_none(self):
        return self.value != self.DEFAULT_VALUE

    def get_column(self, df):
        if self.value == self.DEFAULT_VALUE:
            return None
        else:
            return _dataframe_get_column(df, self.value)


class ColumnGroupChoiceParam(ChoiceParam):
    def set_column_choices(self, choices):
        self.set_choices([n + "*" for n in make_prefix_groups(choices).keys()])

    def get_column_prefix(self):
        return self.value.removesuffix("*")

    def get_column_names(self, df):
        prefix = self.get_column_prefix()
        column_names = get_all_columns(df).keys()
        return [n for n in column_names if n.startswith(prefix)]

    def get_column_suffixes(self, df):
        prefix = self.get_column_prefix()
        return [n.removeprefix(prefix) for n in self.get_column_names(df)]


class ColumnGroupOrNoneChoiceParam(ColumnGroupChoiceParam):
    DEFAULT_VALUE = "— NONE —"

    def set_choices(self, choices: Iterable[str]):
        super().set_choices([self.DEFAULT_VALUE] + list(choices))

    def is_none(self):
        return self.value == self.DEFAULT_VALUE

    def is_not_none(self):
        return self.value != self.DEFAULT_VALUE

    def get_column_prefix(self):
        if self.is_none():
            return None
        return super().get_column_prefix()

    def get_column_names(self, df):
        if self.is_none():
            return []
        return super().get_column_names(df)


class ColumnOrIndexChoiceParam(ColumnChoiceParam):
    DEFAULT_VALUE = "— INDEX —"

    def set_choices(self, choices: Iterable[str]):
        super().set_choices([self.DEFAULT_VALUE] + list(choices))

    def is_index(self):
        return self.value == self.DEFAULT_VALUE

    def is_not_index(self):
        return self.value != self.DEFAULT_VALUE

    def get_column(self, df):
        if self.value == self.DEFAULT_VALUE:
            return df.index.to_series()
        else:
            return _dataframe_get_column(df, self.value)


class ColumnOrStringParam(ColumnChoiceParam):
    DEFAULT_VALUE: Any = ""
    PREFIX = "— "

    def set_column_choices(self, choices):
        self.set_choices([self.PREFIX + c for c in choices])

    def get_column_name(self) -> Optional[str]:
        if type(self.value) is str and self.value.startswith(self.PREFIX):
            return self.value[len(self.PREFIX) :]
        return None

    def get_value_from_dict(self, data: dict) -> str:
        if type(self.value) is str and self.value.startswith(self.PREFIX):
            return data[self.value[len(self.PREFIX) :]]
        else:
            return self.value

    def get_column_or_value(self, df: pd.DataFrame, numeric: bool) -> Union[float, str, pd.Series, None]:
        try:
            if type(self.value) is str and self.value.startswith(self.PREFIX):
                col = df[self.value[len(self.PREFIX) :]]
                return col.astype(float if numeric else str)
            else:
                return float(self.value) if numeric else str(self.value)
        except ValueError:
            return None

    def set_choices(self, choices: Iterable[str]):
        self.choices = list(choices)
        if (
            self._value is not None
            and type(self._value) is str
            and self._value.startswith(self.PREFIX)
            and self._value not in self.choices
        ):
            self._value = self.DEFAULT_VALUE
            self._choice = None


class ColumnOrIntegerParam(ColumnOrStringParam):
    DEFAULT_VALUE: int = 0

    def __init__(
        self,
        label: str,
        value: Optional[int] = 0,
        choices: Optional[Iterable[str]] = None,
    ):
        super().__init__(label, choices=choices)
        self.value = value

    def clean_value(self, value):
        if type(value) is str and value.startswith(self.PREFIX):
            return value
        try:
            return int(value)
        except ValueError:
            return self.DEFAULT_VALUE


class HasSubParametersMixin:
    """Mixin class which handles access to the parameters inside Plugins
    and MultiParams."""

    label: str

    def __init__(self, *a, **k) -> None:
        self.params: Mapping[str, BaseParam] = {}

        super().__init__(*a, **k)

        # Allow new django-esque declarations via subclasses.
        # parameters hold values so they are always copied to preven
        # multiple parameters of the same type interfering.
        found = set()
        for cls in reversed(self.__class__.__mro__):
            for name in cls.__dict__:
                if isinstance(getattr(self, name), BaseParam) and name not in found:
                    self.__dict__[name] = self.params[name] = getattr(self, name).copy()
                    found.add(name)

    def __setattr__(self, name: str, value: None) -> None:
        """Intercepts attempts to set parameters to a value and turns them into parameter.set_value.
        Any other kind of attribute assignment is passed through."""

        if hasattr(self, "params") and name in self.params and not isinstance(value, BaseParam):
            param = self.params[name]
            assert isinstance(param, ScalarParam)
            param.set_value(value)
        else:
            super().__setattr__(name, value)

    def get_parameters(self, key, base_dir=".") -> Iterable[Tuple[str, str]]:
        for subkey, param in self.params.items():
            yield from param.get_parameters(f"{key}.{subkey}" if key else subkey, base_dir)

    def set_parameter(self, key: str, value: Union[bool, int, float, str], base_dir: str = "."):
        if "." in key:
            key, subkey = key.split(".", 1)
            param = self.params[key]
            if subkey == "_label":
                param.label = str(value)
            else:
                assert isinstance(param, (HasSubParametersMixin, ArrayParam))
                param.set_parameter(subkey, value, base_dir)
        else:
            param = self.params[key]
            if isinstance(param, (FileParam, FileSaveParam)) and value is not None:
                # XXX this is a clumsy approach?
                param.set_value(os.path.join(base_dir, str(value)))

            elif isinstance(param, ScalarParam):
                param.set_value(value)

    def set_column_choices(self, choices):
        for p in self.params.values():
            p.set_column_choices(choices)

    # XXX allows dict-like access to params but not sure if this is a bad idea.

    def __getitem__(self, key):
        return self.params[key]

    def __setitem__(self, key, value):
        self.params[key].set_value(value)

    def __contains__(self, key):
        return key in self.params


class ArrayParam(BaseParam):
    """An ArrayParam contains zero or more copies of `param`, which can be a
    SimpleParam or a MultiParam."""

    # XXX the only real use for this is as an array of MultiParams so I think
    # maybe we should have a TabularParam which combines ArrayParam and
    # MultiParam more directly."""

    # XXX it's a bit suss that this isn't a HasSubParametersMixin
    # because that does most stuff already: perhaps the 'array' should
    # actually be a dict, which would also deal with sparse keys elegantly.

    params: list[BaseParam]

    def __init__(
        self,
        label: str,
        param: BaseParam,
        min_size: int = 0,
        max_size: Optional[int] = None,
        read_only: Optional[bool] = None,
    ):
        super().__init__(label)
        self.param = param.copy()
        self.min_size = min_size
        self.max_size = max_size
        if read_only is not None:
            self.read_only = read_only
        self.params = [param.copy() for n in range(0, min_size)]
        self.relabel()

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
        self.relabel()

    def del_subparam(self, param: BaseParam):
        self.params.remove(param)
        self.relabel()

    def relabel(self):
        while len(self.params) < self.min_size:
            self.params.append(self.param.copy())
        for n, param in enumerate(self.params):
            param.label = self.param.label + f" {n+1}"

    def copy(self) -> "ArrayParam":
        return self.__class__(self.label, self.param, self.min_size, self.max_size, self.read_only)

    def __len__(self):
        return len(self.params)

    def __getitem__(self, key):
        return self.params[int(key)]

    def __setitem__(self, key, value):
        self.params[key].value = value

    def __contains__(self, item):
        return item in self.params

    def __iter__(self):
        return self.params.__iter__()

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

    def set_parameter(self, key: str, value: Union[bool, int, float, str], base_dir: str = "."):
        if "." in key:
            key, subkey = key.split(".", 1)
            while int(key) >= len(self.params):
                self.params.append(self.param.copy())
            param = self.params[int(key)]

            if subkey == "_label":
                param.label = str(value)
            else:
                assert isinstance(param, (HasSubParametersMixin, ArrayParam))
                param.set_parameter(subkey, value, base_dir)

        elif self.max_size is None or int(key) < self.max_size:
            while int(key) >= len(self.params):
                self.params.append(self.param.copy())
            param = self.params[int(key)]

            if isinstance(param, (FileParam, FileSaveParam)) and value is not None:
                # XXX this is a clumsy approach?
                param.set_value(os.path.join(base_dir, str(value)))
            elif isinstance(param, ScalarParam):
                param.set_value(value)


class PerColumnArrayParam(ArrayParam):
    """An ArrayParam where each value in the array corresponds to a column
    in the input dataframe, as set by set_column_choices."""

    read_only = True

    def get_parameters(self, key, base_dir="."):
        for n, p in enumerate(self.params):
            yield f"{key}.{n}._label", p.label
            yield from p.get_parameters(f"{key}.{n}", base_dir)

    def set_column_choices(self, choices):
        params_by_label = {p.label: p for p in self.params}
        self.params = []
        for label in choices:
            if label in params_by_label:
                self.params.append(params_by_label[label])
            else:
                self.params.append(self.param.copy())
                self.params[-1].label = label
        super().set_column_choices(choices)

    def get_column_params(self):
        for p in self.params:
            yield p.label, p


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

    @file_types.setter
    def file_types(self, file_types: Sequence[Tuple[str, Union[str, List[str]]]]):
        self.find_fileparam().file_types = file_types

    def add_files(self, filenames):
        # XXX slightly daft way of doing it.  It is setting the filename
        # of the 'template' self.param, and then copying that template to
        # make the new row param.
        for filename in filenames:
            self.find_fileparam().value = filename
            self.add_row()


class MultiParam(HasSubParametersMixin, BaseParam):
    params: Mapping[str, BaseParam] = {}

    def __init__(self, label: str, params: Optional[Mapping[str, BaseParam]] = None):
        super().__init__(label)
        self.params = dict((k, v.copy()) for k, v in params.items()) if params else {}

        # Allow new django-esque declarations via subclasses
        for k, p in self.__class__.__dict__.items():
            if isinstance(p, BaseParam):
                if k not in self.params:
                    self.params[k] = p.copy()
                self.__dict__[k] = self.params[k]

    def copy(self) -> "MultiParam":
        return self.__class__(self.label, self.params)

    def __iter__(self):
        return self.params.__iter__()

    def keys(self):
        return self.params.keys()

    def values(self):
        return self.params.values()

    def items(self):
        return self.params.items()

    # attribute-like accessors

    def __getattr__(self, name):
        try:
            return self.params[name]
        except KeyError as exc:
            try:
                raise AttributeError(name=name, obj=self) from exc
            except TypeError:  # pragma: no cover
                # 3.9 doesn't have name and obj attributes
                raise AttributeError() from exc

    def __setattr__(self, name, value):
        """Intercepts attempts to set parameters to a value and turns them into parameter.set_value.
        Any other kind of attribute assignment is passed through."""
        target_attr = getattr(self, name, None)
        if isinstance(target_attr, BaseParam) and not isinstance(value, BaseParam):
            target_attr.set_value(value)
        else:
            super().__setattr__(name, value)

    def get_parameters(self, key, base_dir="."):
        for k, p in self.params.items():
            yield from p.get_parameters(f"{key}.{k}", base_dir)

    def get_hash_value(self):
        digest = hashlib.new(PARAM_DIGEST_HASH)
        for k, p in self.params.items():
            digest.update((str(k) + "\0" + p.get_hash_value()).encode("utf-8"))
        return digest.hexdigest()


class TabularMultiParam(MultiParam):
    """This is just used to drop a hint to the GUI as to how the MultiParam
    is to be presented ... as a hierarchy or as a table ..."""


class FramedMultiParam(MultiParam):
    """This is just used to drop a hint to the GUI to display the MultiParam
    in its own frame"""
