import uuid
from datetime import datetime

SERVICE_ADDRESS = "http://localhost:8000"

def create_metadata(actual_time, processing_duration, data_str):
    total_rows = len(data_str.split('\n'))
    missing_data_points = data_str.count(', ,') + data_str.count(',,')
    
    # Mocking the validity and accuracy for the experiment
    completeness = 100 * (total_rows - missing_data_points) / total_rows
    validity = 100 * (total_rows - missing_data_points) / total_rows
    accuracy = 100 - (missing_data_points / total_rows * 100)

    return {
        "serviceAddress": SERVICE_ADDRESS,
        "serviceName": "Weather domain data",
        "uniqueIdentifier": str(uuid.uuid4()),
        "completeness": completeness,
        "validity": validity,
        "accuracy": accuracy,
        "actualTime": actual_time,  # when the data became valid or was created
        "processingTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # when the data was ingested or updated
        "processingDuration": f"{processing_duration:.2f} seconds"  # how long it took to process the data
    }