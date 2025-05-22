import functions.addr_matching as am
import functions.db_connection as dbc

data_folder = "../data/eval_1/"

schema_name = "charly_bernard_verite_these"
links_table_name = "auto_links_adresses"
id_table_from_col = "id_from"
id_table_to_col = "id_to"
table_name_from_col = "table_from"
table_name_to_col = "table_to"
geom_col = "geometry"
id_col = "id"
validated_col = "validated"
to_keep_col = "to_keep"
similar_geom_col = "are_similar_geom"
creation_date_col = "creation_date"
method_col = "method"

default_epsg_code = 2154
max_distance = 10 # Distance in meters
simp_label_col = "simplified_label"
norm_label_col = "normalised_label"

cadastre_paris_1807_adresses_settings = {
    "name": "cadastre_paris_1807_adresses",
    "th_attr_col": "NOM_SAISI",
    "sn_attr_col": "NUMERO TXT",
}

atlas_vasserot_1810_adresses_settings = {
    "name": "atlas_vasserot_1810_adresses",
    "th_attr_col": "nom_entier",
    "sn_attr_col": "num_voies",
}

atlas_jacoubet_1836_adresses_settings = {
    "name": "atlas_jacoubet_1836_adresses",
    "th_attr_col": "nom_entier",
    "sn_attr_col": "num_voies",
}

atlas_municipal_1888_adresses_settings = {
    "name": "atlas_municipal_1888_adresses",
    "th_attr_col": "normalised",
    "sn_attr_col": "numbers_va",
}

ban_adresses_settings = {
    "name": "ban_adresses",
    "th_attr_col": "nom_voie",
    "sn_attr_col": "numero",
    "add_sn_attr_col": "rep"
}

osm_adresses_settings = {
    "name": "osm_adresses",
    "th_attr_col": "osm_adresses_streetName",
    "sn_attr_col": "houseNumberLabel",
}

tables_settings = [
    cadastre_paris_1807_adresses_settings,
    atlas_vasserot_1810_adresses_settings,
    atlas_jacoubet_1836_adresses_settings,
    atlas_municipal_1888_adresses_settings,
    ban_adresses_settings,
    osm_adresses_settings
]

# Connexion à la base de données PostgreSQL
config_file = "../configs/db_config.ini"
conn = dbc.connect_bdd_from_config_file(config_file)

exceptions = [
    # ["Marché Beauveau", "Place du Marché Beauveau"],
    # ["Rue d'Aral", "rue Daval"],
    # ["Rue d'Aval", "rue Daval"],
    # ["Rue de Basfroid", "rue Basfroi"],
    # ["Rue de Bercy Faubourg Saint Antoine", "Rue de Bercy"],
    # ["Rue de Bercy Saint Antoine", "Rue de Bercy"],
    # ["Rue de l'Égout au Marais", "Rue de l'Égout"],
    # ["Rue de Sarente", "Rue de Jarente"],
    # ["Rue des Barrées", "Rue des Barres"],
    # ["Rue Girard Beauguet", "Rue Gérard Beauquet"],
    # ["Rue Girard Bauquet", "Rue Gérard Beauquet"],
    # ["Rue Gérard Bauquet", "Rue Gérard Beauquet"],
    # ["Rue Girard-Bocquet", "Rue Gérard Beauquet"],
    # ["Rue des Lions", "Rue des Lions Saint Paul"],
    # ["Rue Lenoir Saint Antoine", "Rue Lenoir"],
    # ["Rue Royale", "Rue de Birague"],
    # ["Rue Royale Saint Antoine", "Rue de Birague"],
    # ["Rue Saint Louis au Marais", "Rue Saint Louis"],
    # ["Rue Saint Nicolas Faubourg Saint Antoine", "Rue Saint Nicolas"],
    # ["Rue Sainte Marguerite Saint Antoine", "Rue Sainte Marguerite"],
    # ["Cul-de-sac Guéménée", "Impasse Guéménée"],
    # ["Cul-de-sac de la Forge Royale", "Impasse de la Forge"],
    # ["Rue Gonnet", "Rue Marguerite Gonnet"],
    # ["Place Royale", "Place des Vosges"],
    # ["Boulevard Saint-Antoine", "Boulevard Beaumarchais"],
]

# Path to the CSV file for manual links
csv_file_path = data_folder + "manual_links.csv"

############################################ Links creation ############################################ 

# Create simplified labels for tables
# We take into account exceptions :
#  - if there is the sublist ["Rue de Basfroid", "rue Basfroi"] in exceptions, it means "Rue de Basfroid" will be replaced by "rue Basfroi" to detect street name similarities
am.add_name_columns_for_multiple_tables(conn, tables_settings, schema_name, simp_label_col, norm_label_col, exceptions)

# am.extract_manual_links(conn, schema_name, links_table_name,
#                       id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
#                       geom_col, validated_col, to_keep_col, method_col, creation_date_col, csv_file_path)

# # Create links between tables
am.create_links_table(
    conn, schema_name, links_table_name,
    id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
    geom_col, validated_col, to_keep_col, similar_geom_col, method_col, creation_date_col, default_epsg_code, overwrite=True)
am.create_links_table_from_multiple_tables(
    conn, tables_settings, schema_name, links_table_name,
    id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
    geom_col, validated_col, to_keep_col, similar_geom_col, method_col, creation_date_col, simp_label_col, norm_label_col,
    default_epsg_code, max_distance)

# # Insert manual links from CSV
# am.insert_manual_links_from_csv(conn, schema_name, links_table_name, csv_file_path,
#                                  id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
#                                  geom_col, validated_col, method_col, creation_date_col, default_epsg_code)
# print(f"Manual links from {csv_file_path} inserted")

# Get links to keep
am.get_links_to_keep(conn, schema_name, links_table_name, tables_settings, id_table_from_col, table_name_from_col, table_name_to_col, to_keep_col)

# Create views for final links
am.create_view_for_final_links(conn, schema_name, links_table_name, to_keep_col)

############################################################################################################### 


gt_links_file_path = data_folder + "links_ground_truth.csv"
gt_sn_without_link_file_path = data_folder + "sn_without_link_ground_truth.csv"

am.extract_ground_truth_links(
    conn, tables_settings, schema_name, links_table_name,
    id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
    geom_col, to_keep_col, similar_geom_col, simp_label_col, gt_links_file_path)

am.extract_streetnumbers_without_link(
    conn, tables_settings, schema_name, links_table_name,
    id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
    geom_col, to_keep_col, similar_geom_col, simp_label_col, gt_sn_without_link_file_path)

conn.close()