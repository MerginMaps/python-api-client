LOGIN="admin3"
PASSWORD="topsecret"
URL="http://localhost:8080"


import pandas as pd
import geopandas as gpd
import os
import mergin

client = mergin.MerginClient(
    url=URL,
    login=LOGIN,
    password=PASSWORD
)

PROJECT="mergin/vienna"


LOCAL_FOLDER="/tmp/project3"
# os.rmdir(LOCAL_FOLDER)

client.download_project(PROJECT, LOCAL_FOLDER)

# # Get the data from the CSV (use just sample of data)
# csv_file = os.path.join(LOCAL_FOLDER, "vienna_trees_gansehauffel.csv")
# csv_df = pd.read_csv(csv_file, nrows=20, dtype={"health": str})
# # Convert geometry in WKT format to GeoDataFrame
# gdf = gpd.GeoDataFrame(csv_df, geometry=gpd.GeoSeries.from_wkt(csv_df.geometry))
# print(gdf.head())
# # Save the GeoDataFrame to a Geopackage
# gdf.to_file(
#     os.path.join(LOCAL_FOLDER, "Ready_to_survey_trees.gpkg"), 
#     layer="Ready_to_survey_trees", driver="GPKG",
# )

# print (client.upload_chunks_cache.cache)
# client.push_project(LOCAL_FOLDER)