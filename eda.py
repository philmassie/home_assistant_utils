import sqlalchemy
import pandas as pd
import datetime as dt
# import pytz
from time import time
pd.options.plotting.backend = "plotly"

db_path = 'sqlite:///home-assistant_v2.db'

def to_utc_ts(dt_now):
    return(dt_now.timestamp())

def query_states(entities, ts_lim_utc=dt.datetime(1900, 1, 1)):

    entity_list = ', '.join([f'"{w}"' for w in entities])
    ts_lim_utc=to_utc_ts(ts_lim_utc)
    sql = f"""
        SELECT a.state_id, a.state, a.last_updated_ts, a.old_state_id, a.attributes_id , a.metadata_id, a.last_reported_ts, b.entity_id
        FROM (
            SELECT *
            FROM states 
            WHERE metadata_id IN (
                SELECT metadata_id
                FROM states_meta 
                WHERE entity_id in ({entity_list})
            )
            AND last_updated_ts >= {ts_lim_utc}
        )
        AS a
        INNER JOIN (
            SELECT metadata_id , entity_id
            FROM states_meta
        )
        AS b
        ON
        a.metadata_id = b.metadata_id
        """
    dbEngine=sqlalchemy.create_engine(db_path)
    pdf = pd.read_sql(sql, dbEngine)

    pdf["state"] = pd.to_numeric(pdf["state"], errors='coerce')
    pdf.dropna(inplace=True)

    pdf["dt"] = (pd.to_datetime(pdf["last_updated_ts"].round(0), unit='s') + pd.DateOffset(hours=2))

    return(pdf)


def query_statistics(entities, ts_lim_utc=dt.datetime(1900, 1, 1), table="statistics"):

    entity_list = ', '.join([f'"{w}"' for w in entities])
    ts_lim_utc=to_utc_ts(ts_lim_utc)
    sql = f"""
        SELECT b.statistic_id, a.max, a.min, a.mean, a.created_ts
        FROM (
            SELECT metadata_id, created_ts, max, min, mean
            FROM {table}
            WHERE metadata_id IN (
                SELECT id 
                FROM statistics_meta 
                WHERE statistic_id in ({entity_list})
            )
            AND created_ts > {ts_lim_utc}
        )
        AS a
        INNER JOIN (
            SELECT *
            FROM statistics_meta
        )
        AS b
        ON
        a.metadata_id = b.id;
        """
    dbEngine=sqlalchemy.create_engine(db_path)
    pdf = pd.read_sql(sql, dbEngine)

    pdf["max"] = pd.to_numeric(pdf["max"], errors='coerce')
    pdf["min"] = pd.to_numeric(pdf["min"], errors='coerce')
    pdf["mean"] = pd.to_numeric(pdf["mean"], errors='coerce')
    pdf.dropna(inplace=True)

    pdf["dt"] = (pd.to_datetime(pdf["created_ts"].round(0), unit='s') + pd.DateOffset(hours=2))

    return(pdf)

# States xample
dt_now = dt.datetime.now()
entities = ["sensor.solarman_current_l1"]
pdf = query_states(entities)
pdf

# Energy use 00:00 - 6am daily mean
entities = ["sensor.solarman_load_power"]
pdf = query_statistics(entities)

pdf.set_index("dt", inplace=True)
pdf = pdf.between_time("00:00", "06:00")\
    .reset_index()
pdf["d"] = pdf["dt"].dt.date

daily_mean = pdf.groupby("d")["mean"].mean()
daily_mean.to_pickle("daily_mean_power.pkl")



# entity merge test
old_sensor_id = 'sensor.solarman_ac_temperature'
new_sensor_id = 'sensor.solarman_temperature'

pdf_stats = query_statistics([old_sensor_id, new_sensor_id])
pdf_stats.pivot(index='dt', columns='statistic_id', values='mean').plot()

pdf_stats_st = query_statistics([old_sensor_id, new_sensor_id], table="statistics_short_term")
pdf_stats_st.pivot(index='dt', columns='statistic_id', values='mean').plot()

pdf_states = query_states([old_sensor_id, new_sensor_id])
pdf_states.pivot(index='dt', columns='entity_id', values='state').plot()





