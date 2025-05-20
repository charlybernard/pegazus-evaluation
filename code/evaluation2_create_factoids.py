import random
import json
import numpy as np
from dateutil import parser
import pandas as pd
from rdflib import URIRef
import functions.geom_processing as gp
import functions.create_factoids_descriptions as cfd
import functions.data_from_sparql_queries as dfsq
import functions.evaluation_aux as ea

data_folder = "../data/eval_2/"
data_sources_folder = "../data/sources/"
graphdb_url = URIRef("http://localhost:7200")
repository_name = "addresses_from_factoids"
facts_named_graph_name = "facts"

epsg_code = "EPSG:2154"
max_distance = 5

# Remplace par les chemins vers tes fichiers
sn_labels_file = data_folder + "streetnumber_labels_final_graph.csv"
sn_attr_version_valid_times_file = data_folder + "streetnumber_attr_geom_version_valid_times_final_graph.csv"
sn_attr_version_values_file = data_folder + "streetnumber_attr_geom_version_values_final_graph.csv"
sn_attr_change_valid_times_file = data_folder + "streetnumber_attr_geom_change_valid_times_final_graph.csv"
sn_version_descriptions_file = data_sources_folder + "fragmentary_states_streetnumbers.json"
sn_change_descriptions_file = data_sources_folder + "fragmentary_events_streetnumbers.json"

# Create the CSV files from the SPARQL queries
dfsq.select_streetnumbers_labels(graphdb_url, repository_name, facts_named_graph_name, sn_labels_file)
dfsq.select_streetnumbers_attr_geom_version_valid_times(graphdb_url, repository_name, facts_named_graph_name, sn_attr_version_valid_times_file)
dfsq.select_streetnumbers_attr_geom_version_values(graphdb_url, repository_name, facts_named_graph_name, sn_attr_version_values_file)
dfsq.select_streetnumbers_attr_geom_change_valid_times(graphdb_url, repository_name, facts_named_graph_name, sn_attr_change_valid_times_file)

# Read the CSV files
labels = pd.read_csv(sn_labels_file)
version_valid_times = pd.read_csv(sn_attr_version_valid_times_file)
version_values = pd.read_csv(sn_attr_version_values_file)
changes_valid_times = pd.read_csv(sn_attr_change_valid_times_file)

# Take a sample of 60% of the data
versions_sample = version_valid_times.sample(frac=0.6)
changes_sample = changes_valid_times.sample(frac=0.6)

# For each street number, take a random sample of 1 label
random_labels_for_versions = labels.groupby("sn").sample(n=1, random_state=42)
random_labels_for_changes = labels.groupby("sn").sample(n=1, random_state=42)

#####################################################################################

# Group all version values by their version, and create a list of version values for each version
grouped_version_values = version_values.groupby("attrVersion")["versionValue"].apply(list).to_dict()
version_values_dict = ea.get_random_geometry_for_street_number(grouped_version_values, epsg_code, max_distance=5)

# Generate new valid time contained in old valid time for each version value
ea.generate_random_dates_for_versions(versions_sample)

# Generate new changes coherent with generated ones 
ea.generate_random_dates_for_changes(changes_sample)

# Add a new column "versionValue" to the version_valid_times dataframe
versions_sample["versionValue"] = versions_sample["attrVersion"].map(version_values_dict)

# Merge the two dataframes on the "sn" column
out_versions = pd.merge(versions_sample, random_labels_for_versions, on="sn", how="inner")
out_changes = pd.merge(changes_sample, random_labels_for_changes, on="sn", how="inner")

####################################################################################

versions_desc = cfd.create_version_descriptions(out_versions)
json_versions_desc = {"states": versions_desc}

changes_desc = cfd.create_change_descriptions(out_changes)
json_changes_desc = {"events": changes_desc}

# Exporter dans un fichier JSON
with open(sn_version_descriptions_file, 'w', encoding='utf-8') as f:
    json.dump(json_versions_desc, f, ensure_ascii=False, indent=4)

# Exporter dans un fichier JSON
with open(sn_change_descriptions_file, 'w', encoding='utf-8') as f:
    json.dump(json_changes_desc, f, ensure_ascii=False, indent=4)