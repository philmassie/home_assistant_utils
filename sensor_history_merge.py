# https://github.com/janmolemans/hass_utils/blob/51403be6a1f180c02c6e66e1bdc75e493192006e/merge_sensor_statistics.ipynb

import pandas as pd
import sqlite3
import pandas as pd

con = sqlite3.connect("home-assistant_v2.db")

def update_id(old_sensor_id, new_sensor_id, table):
    assert table in ["statistics", "statistics_short_term", "states"]

    log.warning(f"Updating {old_sensor_id} to {new_sensor_id} in {table}")

    try:

        if table == "states":
            # States
            ts_field = "last_updated_ts"

            old_meta_id = pd.read_sql_query(f"""SELECT metadata_id 
                                                FROM states_meta
                                                WHERE entity_id = '{old_sensor_id}';""", con).loc[0,"metadata_id"]
            
            new_meta_id = pd.read_sql_query(f"""SELECT metadata_id 
                                                FROM states_meta
                                                WHERE entity_id = '{new_sensor_id}';""", con).loc[0,"metadata_id"]
        else:
            # Statistics
            ts_field = "start_ts"

            old_meta_id = pd.read_sql_query(f"""SELECT id 
                                                FROM statistics_meta
                                                WHERE statistic_id = '{old_sensor_id}';""", con).loc[0,"id"]   
            
            new_meta_id = pd.read_sql_query(f"""SELECT id 
                                                FROM statistics_meta
                                                WHERE statistic_id = '{new_sensor_id}';""", con).loc[0,"id"]

        new_ts_min = pd.read_sql_query(f"""SELECT MIN({ts_field}) as ts_min 
                                        FROM {table} 
                                        WHERE metadata_id = '{new_meta_id}';""", con).loc[0,"ts_min"]
            
        stmnt = f"""UPDATE {table} 
            SET metadata_id = {new_meta_id}
            WHERE metadata_id = {old_meta_id}
            AND {ts_field} < {new_ts_min}
            """
        
        cur = con.cursor()
        cur.execute(stmnt)
        con.commit()    

    except Exception as e:
        log.warning(e)
        pass

# mapping of the sensors, statistics of the left ones will be merged into the right ones
mapping={
    "sensor.solarman_ac_temperature": "sensor.solarman_temperature",
    "sensor.solarman_current_l1": "sensor.solarman_internal_l1_current",
    "sensor.solarman_current_l2": "sensor.solarman_internal_l2_current",
    "sensor.solarman_inverter_l1_power": "sensor.solarman_internal_l1_power",
    "sensor.solarman_inverter_l2_power": "sensor.solarman_internal_l2_power",
    "sensor.solarman_grid_current_l1": "sensor.solarman_grid_l1_current",
    "sensor.solarman_grid_current_l2": "sensor.solarman_grid_l2_current",
    "sensor.solarman_grid_voltage_l1": "sensor.solarman_grid_l1_voltage",
    "sensor.solarman_grid_voltage_l2": "sensor.solarman_grid_l2_voltage",
    "sensor.solarman_total_grid_production": "sensor.solarman_total_energy",
    }

for old_sensor_id, new_sensor_id in mapping.items():
    for table in ["statistics", "statistics_short_term", "states"]:  
        update_id(old_sensor_id, new_sensor_id, table)




