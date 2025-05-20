import pandas as pd
import numpy as np
from dateutil import parser
import random
import functions.geom_processing as gp

# Fonction pour construire la colonne "join"
def build_join_label(row):
    parts = [row['label']]
    
    # Partie app
    if pd.notnull(row['tStampAppDay']):
        parts.append(f"app={row['tStampAppDay']}")
    else:
        parts.append(f"appBefore={row['tStampAppBeforeDay']}&appAfter={row['tStampAppAfterDay']}")

    # Partie dis
    if pd.notnull(row['tStampDisDay']):
        parts.append(f"dis={row['tStampDisDay']}")
    else:
        parts.append(f"disBefore={row['tStampDisBeforeDay']}&disAfter={row['tStampDisAfterDay']}")

    return "&".join(parts)

################# Generate a random geometry for each street number #################

def get_random_geometry_for_street_number(values, epsg_code, max_distance=5):
    # For each version value, generate a random geometry within 5 meters of the centroid
    crs_to_uri = gp.get_crs_dict().get(epsg_code)
    geom_transformers = gp.get_useful_transformers_for_to_crs(epsg_code, ["EPSG:4326", "EPSG:3857", "EPSG:2154"])
    new_values = {}
    for version, version_value_list in values.items():
        geom = gp.get_point_around_wkt_literal_geoms(version_value_list, crs_to_uri, geom_transformers, max_distance=max_distance)
        wkt_geom = geom.strip()
        new_values[version] = geom

    return new_values

################# Generate new valid time contained in old valid time for each version value #################

def generate_random_dates_for_versions(versions):
    # Génération des dates aléatoires
    for idx, row in versions.iterrows():
        start_time_col_name = 'startTime'
        end_time_col_name = 'endTime'

        # Get the start and end time from the row
        # Note: Assumes the columns are named 'startTime' and 'endTime'
        # If the columns are named differently, change the following lines accordingly
        start = parser.parse(row[start_time_col_name])
        end = parser.parse(row[end_time_col_name])

        # Convertir en timestamps pour générer aléatoirement
        start_ts = start.timestamp()
        end_ts = end.timestamp()

        # Générer deux timestamps aléatoires, puis les convertir
        t1_ts = random.uniform(start_ts, end_ts)
        t2_ts = random.uniform(start_ts, end_ts)

        # Trier pour garantir t1 < t2
        t1, t2 = sorted([pd.to_datetime(t1_ts, unit='s'), pd.to_datetime(t2_ts, unit='s')])
        start = t1.date().isoformat() + 'T00:00:00Z'
        end = t2.date().isoformat() + 'T00:00:00Z'

        versions.at[idx, start_time_col_name] = start
        versions.at[idx, end_time_col_name] = end


################# Generate new changes coherent with generated ones #################

def generate_random_dates_for_changes(changes):
    # Génération des dates aléatoires
    for idx, row in changes.iterrows():
        time_col_name = 'time'
        time_after_col_name = 'timeAfter'
        time_before_col_name = 'timeBefore'

        # Get the start and end time from the row
        # Note: Assumes the columns are named 'startTime' and 'endTime'
        # If the columns are named differently, change the following lines accordingly
        
        try:
            time = parser.parse(row[time_col_name])
        except:
            time = None

        try:
            time_after = parser.parse(row[time_after_col_name])
        except:
            time_after = None

        try:
            time_before = parser.parse(row[time_before_col_name])
        except:
            time_before = None

        if time is not None:
            final_time = time

        elif None not in [time_before, time_after]:
            final_time = random.uniform(time_after, time_before)

        elif time_after is not None:
            final_time = time_after
        
        elif time_before is not None:
            final_time = time_before
        
        else:
            final_time = np.nan

        if final_time is not None:
            final_time = final_time.date().isoformat() + 'T00:00:00Z'

        changes.at[idx, time_col_name] = final_time