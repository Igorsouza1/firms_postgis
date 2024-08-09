import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from utils.utils import convert_acq_time

def download_csv(url: str, filename: str) -> None:
    print('Baixando dados CSV')
    response = requests.get(url)
    response.raise_for_status()
    with open(filename, 'wb') as file:
        file.write(response.content)

def process_csv_to_gdf(csv_filename: str) -> gpd.GeoDataFrame:
    print('Transformando dados em GEODATA')
    df = pd.read_csv(csv_filename)
    df['hora_deteccao'] = df['acq_time'].apply(convert_acq_time)
    geometry = [Point(xy) for xy in zip(df.longitude, df.latitude)]
    firms_gdf = gpd.GeoDataFrame(df, geometry=geometry)
    firms_gdf.set_crs(epsg=4326, inplace=True)
    firms_gdf = firms_gdf.to_crs(epsg=4674)
    return firms_gdf

def filter_gdf_by_geojson(firms_gdf: gpd.GeoDataFrame, geojson_file: str) -> gpd.GeoDataFrame:
    print('Filtrando dados dentro do pantanal')
    geojson_gdf = gpd.read_file(geojson_file)
    geojson_gdf = geojson_gdf.to_crs(epsg=4674)
    return firms_gdf[firms_gdf.geometry.within(geojson_gdf.unary_union)].drop_duplicates(subset=['latitude', 'longitude'])
