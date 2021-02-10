import subprocess
import sys
import time

import papermill as pm
import unidecode


PROVINCE_MAPPER = {
    '02': 'Albacete',
    '03': 'Alicante',
    '04': 'Almería',
    '01': 'Álava',
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
    '12': 'Castellón',
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
    '46': 'Valencia',
    '47': 'Valladolid',
    '49': 'Zamora',
    '50': 'Zaragoza',
    '51': 'Ceuta',
    '52': 'Melilla'
}

# When being run in colab please set to False
QUERY_POSTGRES = True

# Define seed
SEED = 42

# GerryChain parameters

# Wanted district size in population
TARGET = 33000
# Variable which we are using to create district over target value
VARIABLE = "pob_t_total"
# Parameter for spanning tree method, set to 2 is fine
NODE_REPEATS = 2
# Epson tolerance for map to initialize
INIT_EPSILON = 0.4
# Error tolerance for districts
EPSILION = 0.3
# Maximum number of possible values
TOTAL_STEPS = 56

# From the TOTAL_STEPS obtain the top maps that minimize the population best
TOP_ITERATION = 10

# Create needed folders for run
subprocess.call(['mkdir', 'notebooks'])
subprocess.call(['mkdir', 'maps'])
subprocess.call(['mkdir', 'data'])


for i, (province_id, province_name) in enumerate(PROVINCE_MAPPER.items()):

    print(f'\nStarting GerryChain for {province_name}')
    print(f'\nExecuted {i + 1}/{len(PROVINCE_MAPPER)}')
    province_name.split('/')[0].lower()

    province_name = unidecode.unidecode(province_name.split('/')[0].lower())

    try:
        # Perform ETL
        pm.execute_notebook(
            input_path='diputado-distrito.ipynb',
            output_path=f'notebooks/diputado-distrito-{province_name}.ipynb',
            parameters=dict(
                QUERY_POSTGRES=QUERY_POSTGRES,
                SEED=SEED,
                PROVINCE_ID=province_id,
                TARGET=TARGET,
                VARIABLE=VARIABLE,
                NODE_REPEATS=NODE_REPEATS,
                INIT_EPSILON=INIT_EPSILON,
                EPSILION=EPSILION,
                TOTAL_STEPS=TOTAL_STEPS,
                TOP_ITERATION=TOP_ITERATION
            ),
            start_timeout=60,
            execute_timeout=90
        )

    except:
        print(
            'Jupyter notebook {} failed with the following error: \n{}'.format(
                f'notebooks/diputado-distrito-{province_name}.ipynb',
                sys.exc_info()[0]
            )
        )
        time.sleep(2)
