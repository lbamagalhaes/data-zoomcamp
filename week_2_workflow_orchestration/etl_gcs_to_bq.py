from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"Number of rows processed: {len(df)}")
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame, color, if_exists) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    if color == 'yellow':
        destination_table = 'dezoomcamp.rides'
    
    if color == 'green':
        destination_table = 'dezoomcamp.rides_green'
    
    df.to_gbq(
        destination_table="dezoomcamp.rides_green",
        project_id="ny-rides-lbamagalhaes",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists=if_exists,
    )


@flow()
def etl_gcs_to_bq(months: list = [2,3], color: str = 'yellow', year: str = '2019', if_exists: str = 'append'):
    """Main ETL flow to load data into Big Query"""
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = transform(path)
        write_bq(df, color, if_exists)


if __name__ == "__main__":
    etl_gcs_to_bq()