import pandas as pd
from rdflib import URIRef

import functions.data_from_sparql_queries as dfsq
import functions.evaluation_aux as ea

data_folder = "../data/eval_2/"
graphdb_url = URIRef("http://localhost:7200")

# Repository names
# repository_name = "addresses_from_factoids" is the repository we want to compare with (kind of ground truth)
# repository_states_name = "addresses_from_factoids_with_frag_states" in which we add only the fragmentary states
# repository_states_and_events_name = "addresses_from_factoids_with_frag_states_and_events" in which we add the fragmentary states and events
repository_name = "addresses_from_factoids"
repository_states_name = "addresses_from_factoids_with_frag_states"
repository_states_and_events_name = "addresses_from_factoids_with_frag_states_and_events"

# Named graph names
facts_named_graph_name = "facts"
frag_sn_states_named_graph_name = "fragmentary_states_streetnumbers"
frag_sn_events_named_graph_name = "fragmentary_events_streetnumbers"

# File paths
sn_attr_geom_sources_file = data_folder + "versions_and_sources_from_unmodified_graph.csv"
sn_attr_geom_states_and_sources_file = data_folder + "versions_and_sources_from_states_graph.csv" 
sn_attr_geom_states_events_sources_file = data_folder + "versions_and_sources_from_states_and_events_graph.csv"

sn_attr_geom_times_file = data_folder + "changes_and_times_from_unmodified_graph.csv"
sn_attr_geom_states_and_times_file = data_folder + "changes_and_times_from_states_graph.csv" 
sn_attr_geom_states_events_times_file = data_folder + "changes_and_times_from_states_and_events_graph.csv"

output_file = data_folder + "out.csv"

# Create the CSV files from the SPARQL queries
frag_named_graph_names = [frag_sn_states_named_graph_name, frag_sn_events_named_graph_name]
dfsq.select_streetnumbers_attr_geom_version_and_sources(graphdb_url, repository_name, facts_named_graph_name, sn_attr_geom_sources_file)
dfsq.select_streetnumbers_attr_geom_version_and_sources(graphdb_url, repository_states_name, facts_named_graph_name, sn_attr_geom_states_and_sources_file)
dfsq.select_streetnumbers_attr_geom_version_and_sources(graphdb_url, repository_states_and_events_name, facts_named_graph_name, sn_attr_geom_states_events_sources_file)

dfsq.select_streetnumbers_attr_geom_change_times(graphdb_url, repository_name, facts_named_graph_name, sn_attr_geom_times_file)
dfsq.select_streetnumbers_attr_geom_change_times(graphdb_url, repository_states_name, facts_named_graph_name, sn_attr_geom_states_and_times_file)
dfsq.select_streetnumbers_attr_geom_change_times(graphdb_url, repository_states_and_events_name, facts_named_graph_name, sn_attr_geom_states_events_times_file)

# Remplacer 'ton_fichier.csv' par le nom de ton fichier
df_versions_unmodified = pd.read_csv(sn_attr_geom_sources_file)
df_versions_states = pd.read_csv(sn_attr_geom_states_and_sources_file)
df_versions_states_events = pd.read_csv(sn_attr_geom_states_events_sources_file)

# Remplacer 'ton_fichier.csv' par le nom de ton fichier
df_changes_unmodified = pd.read_csv(sn_attr_geom_times_file)
df_changes_states = pd.read_csv(sn_attr_geom_states_and_times_file)
df_changes_states_events = pd.read_csv(sn_attr_geom_states_events_times_file)

#######################################################################################################################

# Know if evolutions of street numbers for which new fragmentary states were inserted does not change (geometry versions are still the result of the merging of the same sources)

# Detect street numbers that has been modified during the process
frag_source_label = "Factoïdes générés pour les numéros de rue"

unmodified_sn = ea.get_sources_for_versions(df_versions_unmodified, None)
modified_sn_for_states = ea.get_sources_for_versions(df_versions_states, frag_source_label)
modified_sn_for_states_events = ea.get_sources_for_versions(df_versions_states_events, frag_source_label)

version_quality_for_states = ea.get_graph_quality_from_attribute_versions(unmodified_sn, modified_sn_for_states, frag_source_label)
version_quality_for_states_events = ea.get_graph_quality_from_attribute_versions(unmodified_sn, modified_sn_for_states_events, frag_source_label)

print(version_quality_for_states[0])
print(version_quality_for_states[1])
print("-----------------------------")
print(version_quality_for_states_events[0])
print(version_quality_for_states_events[1])

######################################################################################################################

# Know if evolutions of street numbers for which new fragmentary states were inserted does not change (changes happen at the same time each time)

unmodified_sn = ea.get_times_for_changes(df_changes_unmodified)
modified_sn_for_states = ea.get_times_for_changes(df_changes_states)
modified_sn_for_states_events = ea.get_times_for_changes(df_changes_states_events)

change_quality_for_states = ea.get_graph_quality_from_attribute_changes(unmodified_sn, modified_sn_for_states)
change_quality_for_states_events = ea.get_graph_quality_from_attribute_changes(unmodified_sn, modified_sn_for_states_events)

print("###########################################")

print(change_quality_for_states[0])
print(change_quality_for_states[1])
print("-----------------------------")
print(change_quality_for_states_events[0])
print(change_quality_for_states_events[1])