import subprocess
import sys
import random
import pickle

import papermill as pm
import unidecode
import pandas as pd

from config import PROVINCE_PARAMS_MAPPER


def run_notebook(params, province_name, track_status, run_counter):

    try:
        # Perform ETL
        pm.execute_notebook(
            input_path='diputado-distrito.ipynb',
            output_path='notebooks/diputado-distrito-{}.ipynb'.format(province_name),
            parameters=params,
            start_timeout=60,
            execute_timeout=90
        )
        track_status.setdefault('status', []).append('succesful')

    except:

        run_counter += 1
        if run_counter <= 10:
            # Let's try change the seed
            province_config['seed'] = random.randint(0, 1000)
            run_notebook(
                params=province_config, province_name=province_name, track_status=track_status, run_counter=run_counter
            )
        else:
            print(
                'Jupyter notebook {} failed with the following error: \n{}'.format(
                    f'notebooks/diputado-distrito-{province_name}.ipynb',
                    sys.exc_info()[0]
                )
            )
            track_status.setdefault('status', []).append('failed')

    return track_status, params


# Create needed folders for run
subprocess.call(['mkdir', 'notebooks'])
subprocess.call(['mkdir', 'maps'])
subprocess.call(['mkdir', 'data'])

config_tracker = []
track_status = {}
for i, province_config in enumerate(PROVINCE_PARAMS_MAPPER[5:]):

    run_counter = 0
    province_id = province_config['province_id']
    province_name = province_config['province_name']

    print(f'\nStarting GerryChain for {province_name}')
    print(f'\nExecuted {i + 1}/{len(PROVINCE_PARAMS_MAPPER)}')
    province_name.split('/')[0].lower()

    province_name = unidecode.unidecode(province_name.split('/')[0].lower())

    track_status.setdefault('province_id', []).append(province_id)
    track_status.setdefault('province_name', []).append(province_name)

    track_status, used_config = run_notebook(
        params=province_config,
        province_name=province_name,
        track_status=track_status,
        run_counter=run_counter
    )

    config_tracker.append(used_config)
    print(config_tracker)

# Save status in an excel
pd.DataFrame(track_status).to_excel('province_status.xlsm', index=False)

# Save used config
f = open('tracked_config.pkl', "wb")
pickle.dump(config_tracker, f)
f.close()
