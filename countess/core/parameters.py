class BaseParam:
    
    var_type = type(None)
    
    def __init__(self, label, default=None, choices=None, read_only=False):
        self.label = label
        if default is not None: self.default = default
        self.choices = choices        
        self.read_only = read_only
        
        self.value = default
                
    def copy(self):
        return self.__class__(self.label, self.default, self.choices, self.read_only)

    def set_value(self, value):
        self.value = self.var_type(value)

class BooleanParam(BaseParam):
    var_type = bool
    default = False
    
class IntegerParam(BaseParam):
    var_type = int
    default = 0

class FloatParam(BaseParam):
    var_type = float
    default = 0.0
    
class StringParam(BaseParam):
    var_type = str
    default = ''

class FileParam(StringParam):
    var_type = str

class TableParam(BaseParam):
    var_type = list
    
    def __init__(self, columns):
        pass
    
    def add_row(self):
        pass
        
    def del_row(self, row_number):
        pass
    
