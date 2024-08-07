from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
import pandas as pd
import requests
import json
from io import StringIO
import asyncio

app = FastAPI()

@app.get("/")
def root():
    return {"message": "This API aims to create a seamless integration of weather station data from the Hong Kong Observatory (HKO) and the Common Spatial Data Infrastructure (CSDI). The goal is to georeference the weather station data and transform it into a GeoJSON format for real-time fetching, visualization, and analysis."}

async def fetch_csv_data(url: str) -> pd.DataFrame:
    response = await asyncio.to_thread(requests.get, url)
    response.raise_for_status()
    csv_data = StringIO(response.text)
    return pd.read_csv(csv_data)

async def process_occupancy_data(bbox: Optional[str], limit: Optional[int]) -> pd.DataFrame:
    occupancy_url = "https://resource.data.one.gov.hk/td/psiparkingspaces/occupancystatus/occupancystatus.csv"
    parkingspaces_url = "parkingspaces.csv"

    # Fetch data concurrently
    occupancy_df = await fetch_csv_data(occupancy_url)
    occupancy_df.rename(columns={'ï»¿ParkingSpaceId': 'ParkingSpaceId'}, inplace=True)

    # Since parkingspaces.csv is a local file, use a synchronous call
    parkingspaces_df = pd.read_csv('parkingspaces.csv', skiprows=2)
    parkingspaces_df.rename(columns={'ParkingSpa': 'ParkingSpaceId'}, inplace=True)

    # Merge data
    merged_df = pd.merge(occupancy_df, parkingspaces_df, on='ParkingSpaceId')
    merged_df = merged_df[
        ['ParkingSpaceId', 'SectionOfStreet', 'ParkingMeterStatus', 'OccupancyStatus', 'OccupancyDateChanged',
         'Latitude', 'Longitude', 'VehicleType', 'LPP', 'OperatingPeriod', 'TimeUnit', 'PaymentUnit']]

    # Apply bounding box filter
    if bbox:
        min_lon, min_lat, max_lon, max_lat = map(float, bbox.split(','))
        merged_df = merged_df.query('@min_lon <= Longitude <= @max_lon and @min_lat <= Latitude <= @max_lat')

    # Apply limit
    if limit:
        merged_df = merged_df.head(limit)

    return merged_df

def create_geojson(merged_df: pd.DataFrame) -> dict:
    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row['Longitude'], row['Latitude']]
            },
            "properties": row.drop(['Longitude', 'Latitude']).to_dict()
        }
        for _, row in merged_df.iterrows()
    ]
    
    return {
        "type": "FeatureCollection",
        "features": features
    }

@app.get("/td/meter")
async def process_data(bbox: Optional[str] = None, limit: Optional[int] = None):
    try:
        merged_df = await process_occupancy_data(bbox, limit)
        geojson_data = await asyncio.to_thread(create_geojson, merged_df)
        return JSONResponse(content=geojson_data, media_type="application/geo+json")

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
