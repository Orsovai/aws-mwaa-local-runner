from schemas import (
    ExampleSchemaA,
    ExampleSchemaB,
)  # Importing the schemas for validation
from scripts.data_io import (
    MYWAY,
)  # Import the MYWAY class for data manipulation and validation


def prepare_example_data(**kwargs):
    """
    This function prepares example data by reading a CSV file, validating it against a schema,
    manipulating the data, and then writing it to a new CSV file. The function also demonstrates
    reading the manipulated data back from a folder based on the current run context in Airflow.

    :param kwargs: Arbitrary keyword arguments passed from Airflow's context, such as `run_id`.
    """

    # Initialize the MYWAY class with ExampleSchemaA for reading (Schema A)
    # and ExampleSchemaB for writing (Schema B). These schemas will be used for validation.
    read_schema = MYWAY(
        ExampleSchemaA, **kwargs
    )  # Schema for reading and validating input data
    write_schema = MYWAY(
        ExampleSchemaB, **kwargs
    )  # Schema for writing and validating output data

    # Read the data from the CSV file located at the given path.
    # This will validate the data according to ExampleSchemaA.
    df = read_schema.read_csv("/usr/local/airflow/data/test_data.csv")

    # Print the DataFrame to check its contents after reading.
    print(df)

    # Write the DataFrame to a new CSV file, validated by ExampleSchemaB.
    # This step ensures the data conforms to the expected format before being written to disk.
    write_schema.write_csv("test_data_new.csv", df)

    # Read the manipulated data from the CSV file generated above.
    # This demonstrates how to read the newly written file from the current run folder in Airflow.
    df = read_schema.read_csv("test_data_new.csv", use_run_folder=True)

    # Print the DataFrame again to verify that it was correctly read from the run folder.
    print(df)
