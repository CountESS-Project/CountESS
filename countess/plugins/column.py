import pandas as pd

from countess import VERSION
from countess.core.parameters import (
    BooleanParam,
    DataTypeOrNoneChoiceParam,
    PerColumnArrayParam,
    StringParam,
    TabularMultiParam,
)
from countess.core.plugins import PandasSimplePlugin


class _ColumnMultiParam(TabularMultiParam):
    rename = StringParam("Name")
    datatype = DataTypeOrNoneChoiceParam("Column Type")
    index = BooleanParam("Index?")


class ColumnToolPlugin(PandasSimplePlugin):
    name = "DataFrame Column Tool"
    description = "Alter Columns of a DataFrame"
    version = VERSION

    columns = PerColumnArrayParam("Columns", _ColumnMultiParam("Column"))

    def process_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        column_parameters = list(zip(self.input_columns, self.columns))

        drop_columns = [column_name for column_name, parameter in column_parameters if parameter.datatype.is_none()]

        type_columns = {
            column_name: parameter.datatype.get_selected_type()
            for column_name, parameter in column_parameters
            if parameter.datatype.is_not_none()
        }

        index_columns = [
            column_name
            for column_name, parameter in column_parameters
            if parameter.index.value and parameter.datatype.is_not_none()
        ]

        rename_columns = {
            column_name: parameter.rename.value.strip()
            for column_name, parameter in column_parameters
            if parameter.rename.value and parameter.rename.value.strip() and parameter.datatype.is_not_none()
        }

        dataframe = dataframe.reset_index(drop=dataframe.index.names == [None])

        dataframe = dataframe.drop(columns=drop_columns).astype(type_columns)

        if index_columns:
            dataframe = dataframe.set_index(index_columns)

        return dataframe.rename(columns=rename_columns)
