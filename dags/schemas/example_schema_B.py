import pandera as pa
import pandas as pd
from pandera.typing import Index, DataFrame, Series


# Define a schema for the CSV data
class ExampleSchemaB(pa.DataFrameModel):
    client_id: Series[str]  # Client ID should be a string
    data_path: Series[str]  # Data path should be a string
    value: Series[float]  # Value should be a float, it can be optional if desired
