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
            self.value = self.var_type(value)
        if read_only:
            self.read_only = True

    def set_value(self, value):
        self.value = self.var_type(value)

    def copy(self, suffix: str = ""):
        return self.__class__(self.label + suffix, self.value, self.read_only)


class BooleanParam(SimpleParam):
    var_type = bool
    value: bool = False


class IntegerParam(SimpleParam):
    var_type = int
    value: int = 0


class FloatParam(SimpleParam):
    var_type = float
    value: float = 0.0


class StringParam(SimpleParam):
    var_type = str
    value: str = ""


class FileParam(StringParam):
    read_only = True


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


# XXX not in use yet


class ArrayParam(BaseParam):

    params: list[BaseParam] = []

    def __init__(self, label: str, param: BaseParam, size: int = 0):
        self.label = label
        self.param = param
        self.params = [param.copy(f" {n+1}") for n in range(0, size)]

    def add_row(self):
        self = len(self.params)
        self.params.append(self.param.copy(f" {n+1}"))

    def del_row(self, position: int):
        self.params.pop(position)
        for n, param in enumerate(self.params):
            param.label = self.param.label + f" {n+1}"

    def copy(self, suffix: str = ""):
        return ArrayParam(self.label + suffix, self.param, len(self.params))


class MultiParam(BaseParam):

    params: Mapping[str, BaseParam] = {}

    def __init__(self, label: str, params: Mapping[str, BaseParam]):
        self.label = label
        self.params = params

    def copy(self, suffix: str = ""):
        pp = dict(((k, p.copy()) for k, p in self.params.items()))
        mp = MultiParam(self.label + suffix, pp)
