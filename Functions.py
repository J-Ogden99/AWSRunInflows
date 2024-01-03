import boto3
import cdsapi
import calendar
from model-workflows import generate_namelist
from basin-inflow.inflow import create_inflow_file
from tdxhydro-postprocessing.tdxhydrorapic.weights import make_weight_table_from_netcdf
import datetime
from glob import glob


c = cdsapi.Client()
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)


def download_era5():
    today = datetime.date.today()

    this_year = today.strftime("%Y")
    for year in range(1950, this_year):
        for month in range(1, 13):
            c.retrieve(
                'reanalysis-era5-land',
                {
                    'format': 'netcdf.zip',
                    'variable': 'runoff',
                    'year': year,
                    'month': str(month).zfill(2),
                    'day': [str(x).zfill(2) for x in range(1, calendar.monthrange(year, month)[1] + 1)],
                    'time': [f'{x:02d}:00' for x in range(0, 24)],
                },
                target=f'lsm_data/{year}_{str(month).zfill(2)}_era5land_hourly.netcdf.zip'
            )

    for year in range(1940, this_year):
        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': 'runoff',
                'year': year,
                'month': [str(x).zfill(2) for x in range(1, 13)],
                'day': [str(x).zfill(2) for x in range(1, 32)],
                'time': [f'{x:02d}:00' for x in range(0, 24)],
            },
            target=f'lsm_data/{year}_era5_hourly.nc'
        )


def generate_inflows():
    # Read LSM Data that's been downloaded
    lsm_data = 'lsm_data/*_era5_hourly.nc'
    input_dir = 'wt_tables'
    inflow_dir = 'inflows'
    create_inflow_file(lsm_data, input_dir, inflow_dir)
    return


def run_rapid():
    return


def push_results():
    return
