from fastapi import FastAPI
from typing import List, Dict, Union
from datetime import datetime
import numpy as np
import pandas as pd

app = FastAPI()


def load_data():
    return pd.read_csv('usw00023169-temperature-degreef.csv')


data = load_data()

metadata = {
    'completeness': None,
    'validity': None,
    'accuracy': None,
    'last_processing_time': None,
    'timeliness': None,
    'effective_date': None,
    'valid_date': None,
}


def validate_date_format(date):
    try:
        pd.to_datetime(date, format='%Y%m%d', errors='raise')
        return True
    except Exception:
        return False


def validate_float_format(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


def update_metadata():
    now = datetime.now()
    metadata['last_processing_time'] = now.strftime("%m/%d/%Y, %H:%M:%S")

    # Completeness: percentage of non-null entries
    metadata['completeness'] = data.count().sum() / np.product(data.shape)

    # Validity: percentage of entries that are correctly formatted
    metadata['validity'] = data.applymap(lambda x: validate_float_format(
        x) if isinstance(x, float) else validate_date_format(x)).mean().mean()

    # Timeliness: time difference between now and the most recent date in the data
    most_recent_date = pd.to_datetime(data['date'], format='%Y%m%d').max()
    metadata['timeliness'] = (now - most_recent_date).days

    # Bitemporal data: effective and valid dates
    metadata['effective_date'] = most_recent_date.strftime('%Y-%m-%d')
    metadata['valid_date'] = now.strftime('%Y-%m-%d')


update_metadata()


@app.get('/metadata', response_model=Dict[str, Union[str, float, None]])
async def get_metadata():
    return metadata


@app.get('/sample', response_model=List[Dict[str, Union[str, float]]])
async def get_sample():
    sample = data.sample(5).to_dict(orient='records')
    return sample
