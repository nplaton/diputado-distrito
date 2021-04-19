from pathlib import Path

import pandas as pd
import geopandas as gpd
from keplergl import KeplerGl

from config_assembler import PROVINCE_PARAMS_ASSEMBLER

GEOM_SIMPLIFICATION = 0.0005

# base path
base_path = Path().absolute()

district_map = []
province_map = []
municipality_map = []

PROVINCE_PARAMS_ASSEMBLER = [{
    "province_id": "09",
    "province_name": "Burgos",
    "map_iteration": 0}
]

for map_config in PROVINCE_PARAMS_ASSEMBLER:
    province_base_path = base_path.joinpath('data', map_config['province_id'])
    data_directory = province_base_path.joinpath('districts_iteration_{}.json'.format(map_config['map_iteration']))

    districts = gpd.read_file(data_directory)

    # Add variables
    districts['province_id'] = map_config['province_id']
    districts['province_name'] = map_config['province_name']
    districts['map_iteration'] = map_config['map_iteration']

    variables = ['pob_t_total', 'target_pob', 'geometry', 'province_id', 'province_name', 'map_iteration']
    district_map.append(districts[variables])

    # Add other layers
    province_map.append(gpd.read_parquet(province_base_path.joinpath('province.parquet')))
    municipality_map.append(gpd.read_parquet(province_base_path.joinpath('municipality.parquet')))


# Concatenate all districts into one dataframe
output_district = gpd.GeoDataFrame(pd.concat(district_map, ignore_index=True)).reset_index()\
    .rename(columns={'index': 'district'})

# Simplify geometry
output_district['geometry'] = output_district['geometry'].simplify(tolerance=GEOM_SIMPLIFICATION)

# Concatenate all provinces into one dataframe
province_map = gpd.GeoDataFrame(pd.concat(province_map, ignore_index=True))
# Concatenate all provinces into one dataframe
municipality_map = gpd.GeoDataFrame(pd.concat(municipality_map, ignore_index=True))

# Feature Engineer dataframe
# Create maps
KeplerGl(height=800, width=1000).save_to_html(
    {'district': output_district, 'province': province_map, 'municipality': municipality_map},
    file_name=base_path.joinpath('maps', f'districts-spain-simplification-{GEOM_SIMPLIFICATION}.html'),
    center_map=True
)

# Output districts to excel
output_district = output_district.drop('geometry', axis=1)
pd.DataFrame(output_district).to_excel('assembled_districts_spain.xlsm', index=False)
