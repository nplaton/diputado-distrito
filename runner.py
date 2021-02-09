import subprocess
from pathlib import Path

import papermill as pm

PROVINCE_MAPPER = {
    '02': 'Albacete',
    '03': 'Alicante/Alacant',
    '04': 'Almería',
    '01': 'Araba/Álava',
    '33': 'Asturias',
    '05': 'Ávila',
    '06': 'Badajoz',
    '07': 'Balears, Illes',
    '08': 'Barcelona',
    '48': 'Bizkaia',
    '09': 'Burgos',
    '10': 'Cáceres',
    '11': 'Cádiz',
    '39': 'Cantabria',
    '12': 'Castellón/Castelló',
    '13': 'Ciudad Real',
    '14': 'Córdoba',
    '15': 'Coruña, A',
    '16': 'Cuenca',
    '20': 'Gipuzkoa',
    '17': 'Girona',
    '18': 'Granada',
    '19': 'Guadalajara',
    '21': 'Huelva',
    '22': 'Huesca',
    '23': 'Jaén',
    '24': 'León',
    '25': 'Lleida',
    '27': 'Lugo',
    '28': 'Madrid',
    '29': 'Málaga',
    '30': 'Murcia',
    '31': 'Navarra',
    '32': 'Ourense',
    '34': 'Palencia',
    '35': 'Palmas, Las',
    '36': 'Pontevedra',
    '26': 'Rioja, La',
    '37': 'Salamanca',
    '38': 'Santa Cruz de Tenerife',
    '40': 'Segovia',
    '41': 'Sevilla',
    '42': 'Soria',
    '43': 'Tarragona',
    '44': 'Teruel',
    '45': 'Toledo',
    '46': 'Valencia/València',
    '47': 'Valladolid',
    '49': 'Zamora',
    '50': 'Zaragoza',
    '51': 'Ceuta',
    '52': 'Melilla'
}

subprocess.call(['mkdir', 'notebooks'])
subprocess.call(['mkdir', 'maps'])
subprocess.call(['mkdir', 'data'])


for province_id, province_name in PROVINCE_MAPPER.items():

    print(f'\nStarting GerryChain for {province_name}')

    # Perform ETL
    pm.execute_notebook(
        etl_notebook_name,
        etl_notebook_output.as_posix(),
        parameters=dict(
            SIMPLE_NAME=simple_name,
            MODEL_NAME=model,
            temporal_parameters=TEMPORAL_PARAMETERS,
            geographical_destination_parameters=geographical_destination_parameters,
            geographical_origin_parameters=geographical_origin_parameters,
            raw_data_parameters=RAW_DATA_PARAMETERS,
            client_data=client_data,
            pos_id=pos_id,
            extra_catchment_areas=ADD_EXTRA_CATCHMENT_AREAS
        )
    )