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
sn_modified_attr_file = data_folder + "streetnumber_modified_attr_geom_versions.csv" 
sn_unmodified_attr_file = data_folder + "streetnumber_unmodified_attr_geom_versions.csv"
sn_unmodified_filtered_attr_file = data_folder + "streetnumber_filtered_unmodified_attr_geom_versions.csv"
output_file = data_folder + "out.csv"

# Create the CSV files from the SPARQL queries
frag_named_graph_names = [frag_sn_states_named_graph_name, frag_sn_events_named_graph_name]
dfsq.select_streetnumber_unmodified_attr_geom_versions(graphdb_url, repository_name, facts_named_graph_name, sn_unmodified_attr_file)
dfsq.select_streetnumber_modified_attr_geom_versions(graphdb_url, repository_states_name, frag_named_graph_names, sn_modified_attr_file)

# Remplacer 'ton_fichier.csv' par le nom de ton fichier
df_modified = pd.read_csv(sn_modified_attr_file)
df_unmodified = pd.read_csv(sn_unmodified_attr_file)

# # Appliquer la fonction pour créer la colonne "join"
# df_modified["join"] = df_modified.apply(ea.build_join_label, axis=1)
# df_unmodified["join"] = df_unmodified.apply(ea.build_join_label, axis=1)

# # On garde les street numbers qui sont dans les deux fichiers
# df_unmodified = df_unmodified[df_unmodified['label'].isin(df_modified['label'])]

# Merge the two dataframes on the "sn" column
out_df = pd.merge(df_modified, df_unmodified, on="attrVersion", how="outer", suffixes=('_modified', '_unmodified'))

df_unmodified.to_csv(sn_unmodified_filtered_attr_file, index=False)
out_df.to_csv(output_file, index=False)