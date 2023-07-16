from fastapi import FastAPI
from pyspark.sql import SparkSession

app = FastAPI()

# Create a PySpark session
spark = SparkSession.builder \
    .appName("DomainA") \
    .getOrCreate()

@app.get("/")
async def root():
    return {"message": "Welcome to Domain A"}

@app.get("/v1/data")
async def get_domain_data(): 
    
