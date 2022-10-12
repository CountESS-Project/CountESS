from typing import Mapping, Optional, Any


class BaseParam:

    label: str = ""
    value: Any=None

    def copy(self, suffix: str = ""):
        raise NotImplementedError(f"Implement {self.__class__.__name__}.copy()")


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

    def copy(self, suffix: str = ""):
        return self.__class__(self.label + suffix, self.value, self.read_only)


class BooleanParam(SimpleParam):
    var_type = bool
    _value: bool = False


class IntegerParam(SimpleParam):
    var_type = int
    _value: int = 0


class FloatParam(SimpleParam):
    var_type = float
    _value: float = 0.0


class StringParam(SimpleParam):
    var_type = str
    _value: str = ""


class StringCharacterSetParam(StringParam):

    character_set: set[str] = set()

    def __init__(self, label: str, value=None, read_only: bool=False, *, character_set: Optional[set[str]]=None):
        super().__init__(label, value, read_only)
        if character_set is not None:
            self.character_set = character_set

    def clean_character(self, c: str):
        if c in self.character_set: return c
        elif c.upper() in self.character_set: return c.upper()
        else: return ''

    def clean_value(self, value: any):
        value_str = str(value)
        x = ''.join([self.clean_character(c) for c in value_str])
        return x

    def copy(self, suffix: str = ""):
        return self.__class__(self.label + suffix, self.value, self.read_only, character_set=self.character_set)


class FileParam(StringParam):
    
    file_types = [("Any", "*")]
    
    def __init__(self, label: str, value=None, read_only: bool=True, file_types=None):
        super().__init__(label, value, read_only)
        if file_types is not None:
            self.file_types = file_types

    def copy(self, suffix: str = ""):
        return self.__class__(self.label + suffix, self.value, self.read_only, file_types=self.file_types)


class ChoiceParam(BaseParam):

    value: Optional[str]

    def __init__(
        self, label: str, value: Optional[str] = None, choices: list[str] = None
    ):
        self.label = label
        self.value = value
        self.choices = choices or []

    def set_value(self, value):
        assert value is None or value in self.choices
        self.value = value

    def copy(self, suffix: str = ""):
        return ChoiceParam(self.label + suffix, self.value, self.choices)


class ArrayParam(BaseParam):

    params: list[BaseParam] = []

    def __init__(self, label: str, param: BaseParam, size: int = 0):
        self.label = label
        self.param = param
        self.params = [param.copy(f" {n+1}") for n in range(0, size)]

    def add_row(self):
        pp = self.param.copy(f" {len(self.params)+1}")
        self.params.append(pp)
        return pp

    def del_row(self, position: int):
        self.params.pop(position)
        self.relabel()
    
    def del_subparam(self, param: BaseParam):
        self.params.remove(param)
        self.relabel()

    def relabel(self):
        for n, param in enumerate(self.params):
            param.label = self.param.label + f" {n+1}"

    def copy(self, suffix: str = "") -> 'ArrayParam':
        return ArrayParam(self.label + suffix, self.param, len(self.params))

    def __len__(self):
        return len(self.params)

    def __getitem__(self, key):
        return self.params[key]

    def __contains__(self, item):
        return item in self.params

    def __iter__(self):
        return self.params.__iter__()


class MultiParam(BaseParam):

    params: Mapping[str, BaseParam] = {}

    def __init__(self, label: str, params: Mapping[str, BaseParam]):
        self.label = label
        self.params = params

    def copy(self, suffix: str = "") -> 'MultiParam':
        pp = dict(((k, p.copy()) for k, p in self.params.items()))
        return MultiParam(self.label + suffix, pp)

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
        except KeyError:
            raise AttributeError(name=name, obj=self)

    def __contains__(self, item):
        return item in self.params

    def __iter__(self):
        return self.params.__iter__()

