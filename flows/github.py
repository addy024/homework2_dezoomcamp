from pathlib import Path 
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df



@flow(log_prints=True)
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    dataset_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz"

    df = fetch(dataset_url)
    print(len(df))



if __name__ == "__main__":
    etl_web_to_gcs()