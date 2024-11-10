import pandas as pd
import pandera as pa
import os
from datetime import datetime


class MYWAY:
    def __init__(self, schema: pa.DataFrameModel, **kwargs):
        """
        Initializes the ValidatedDataFrame class with a specified schema.

        :param schema: The Pandera DataFrameModel schema to use for validation.
        """
        self.schema = schema
        self.df = None
        self.run_id = kwargs.get("run_id", None)  # Extract the run_id from kwargs

    def read_csv(self, filepath: str, use_run_folder: bool = False) -> pd.DataFrame:
        """
        Reads a CSV file into a pandas DataFrame.

        :param filepath: Path to the CSV file or relative path to be generated using current run.
        :param use_run_folder: Whether to read from the current run's folder based on `run_id`.
        :return: pandas DataFrame containing the data.
        """
        if use_run_folder and self.run_id:
            # Construct the file path dynamically based on the current run folder
            current_date = datetime.now()
            base_dir = "/usr/local/airflow/runs/"  # Root folder where run-specific directories are stored

            # Dynamically construct the path to the current run's folder
            file_path = os.path.join(
                base_dir,
                str(current_date.year),  # Year folder
                str(current_date.month).zfill(2),  # Month folder (with leading zero)
                str(current_date.day).zfill(2),  # Day folder (with leading zero)
                self.run_id,  # Run ID folder
                filepath,  # The CSV file within the run folder
            )

            # Ensure the file exists in the constructed path
            if not os.path.exists(file_path):
                raise FileNotFoundError(
                    f"The file {file_path} does not exist for the current run."
                )
            print(f"Reading file from: {file_path}")
            self.df = pd.read_csv(file_path)

        else:
            # If not using the current run's folder, read from the provided absolute path
            print(f"Reading file from provided path: {filepath}")
            self.df = pd.read_csv(filepath)

        # Validate and raise exception if invalid
        try:
            self.validate()
            print("Data is valid!")
        except pa.errors.SchemaError as e:
            print(f"Validation failed: {e}")
            raise e
        return self.df

    def validate(self) -> pd.DataFrame:
        """
        Validates the DataFrame using the provided schema.

        :return: A validated pandas DataFrame.
        :raises: pandera.errors.SchemaError if the validation fails.
        """
        if self.df is None:
            raise ValueError("No data to validate. Please read the data first.")

        # Validate the DataFrame
        return self.schema.validate(self.df)

    def write_csv(self, filepath: str, df: pd.DataFrame) -> None:
        """Writes the provided DataFrame to a new CSV file after validating it with the Schema."""
        try:
            # Validate the DataFrame with the provided schema (Schema B)
            df = self.schema.validate(df)
            print("Data is valid!")

            # Dynamically generate the file path for the current run
            # Get the current date (year, month, day)
            current_date = datetime.now()

            # Define the base directory where you want to store your files
            base_dir = "/usr/local/airflow/runs/"

            # Construct the file path dynamically based on the current date and run_id
            write_path = os.path.join(
                base_dir,
                str(current_date.year),  # Year folder
                str(current_date.month).zfill(2),  # Month folder (with leading zero)
                str(current_date.day).zfill(2),  # Day folder (with leading zero)
                self.run_id,  # Run ID folder (retrieved from the context)
            )

            # Ensure that the directory exists
            os.makedirs(write_path, exist_ok=True)

            # Define the final file path where you want to save your CSV (or other data)
            final_file_path = os.path.join(write_path, filepath)

            # Write the validated DataFrame to CSV
            df.to_csv(final_file_path, index=False)
            print(f"Data written to {final_file_path}")

        except pa.errors.SchemaError as e:
            print(f"Validation failed: {e}")
            raise e
