# Import required libraries
import pandas as pd  # For handling tabular data with DataFrames
from rdflib import URIRef  # For working with RDF URI references
import functions.data_from_sparql_queries as dfsq  # Custom module to perform SPARQL queries
import functions.evaluation_aux as ea  # Custom module with evaluation-related helper functions
import functions.get_configs as gc  # Module for configuration handling (not used here)

########################################################################################################################

# Variables

# Define data and configuration file paths
proj_config_file = "../configs/project_config.ini"

# Load table settings from configuration file
graphs_table_settings = gc.get_graph_settings(proj_config_file)

# Define paths to the data and link folders
data_folder = "../data/eval_1/"  # Folder containing evaluation data
links_folder = "../data/links/"  # Folder containing link data

# Define SPARQL endpoint and graph names
graphdb_url_str = graphs_table_settings['graphdb_url']  # SPARQL endpoint URL in string format
graphdb_url = URIRef(graphdb_url_str)  # SPARQL endpoint base URL
repository_name = graphs_table_settings["repository_name"]  # Name of the repository in GraphDB
facts_named_graph_name = graphs_table_settings["facts_named_graph_name"]  # Named graph containing the RDF facts

# Define file paths for input/output CSVs
facts_graph_file = data_folder + "versions_and_sources_from_unmodified_graph.csv"  # CSV file for storing fact graph data
links_ground_truth_file = links_folder + "links_ground_truth.csv"  # Ground truth file for evaluated links
sn_without_link_ground_truth_file = links_folder + "sn_without_link_ground_truth.csv"  # Street numbers with no corresponding links in the GT

# Mapping between source identifiers and their human-readable labels + order
source_mapping = {
    "cadastre_paris_1807_adresses":{"order":1, "label":"Adresses du cadastre général de Paris de 1807"},
    "atlas_vasserot_1810_adresses":{"order":2, "label":"Cadastre de Paris par îlot : 1810-1836"},
    "atlas_jacoubet_1836_adresses":{"order":3, "label":"Atlas de la ville de Paris de Jacoubet de 1836"},
    "atlas_municipal_1888_adresses":{"order":4, "label":"Adresses du plan de l'atlas municipal de 1888"},
    "ban_adresses":{"order":5, "label":"Base Adresse Nationale"},
    "osm_adresses":{"order":6, "label":"OpenStreetMap"},
}

# Load ground truth for versions and link-free street numbers, annotated with source metadata
sn_gt_version_sources = ea.get_ground_truth_version_sources(
    links_ground_truth_file,
    sn_without_link_ground_truth_file,
    source_mapping
)

# Extract and export attributes, geometries, versions, and sources for street numbers from the RDF graph
dfsq.select_streetnumbers_attr_geom_version_and_sources(
    graphdb_url,
    repository_name,
    facts_named_graph_name,
    facts_graph_file
)

# Load the exported facts graph as a DataFrame
df_facts_graph = pd.read_csv(facts_graph_file)

# Get all unmodified street number versions and their sources
unmodified_sn = ea.get_sources_for_versions(df_facts_graph, None)

# Compute quality metrics comparing the reconstructed versions against the ground truth
version_quality_for_states = ea.get_graph_quality_from_attribute_versions(
    unmodified_sn,
    sn_gt_version_sources,
    None,
    union=True  # Use union-based matching (lenient comparison strategy)
)

# Print evaluation results
print("-----------------------------")
print(version_quality_for_states[0])  # Likely metrics like precision, recall, F1-score
print(version_quality_for_states[1])  # Possibly per-source or per-version breakdowns
print("-----------------------------")
