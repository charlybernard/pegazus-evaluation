import functions.create_addresses_table as cat
import functions.add_labels_for_addresses_table as alfat
import functions.create_links_table as clt
import functions.extract_addr_links as eal
import functions.get_configs as gc
from functions.db_utils import PostgresManager

########################################################################################################################

# Variables

data_folder = "../data/sources/"
links_folder = "../data/links/"
config_file = "../configs/db_config.ini"

links_ground_truth = links_folder + "links_ground_truth.csv"
sn_without_link_ground_truth = links_folder + "sn_without_link_ground_truth.csv"
pm = PostgresManager(config_file)

addr_table_settings = gc.get_addresses_table_settings(config_file)
links_table_settings = gc.get_links_table_settings(config_file)

source_names = [
    "cadastre_paris_1807_adresses",
    "atlas_vasserot_1810_adresses",
    "atlas_jacoubet_1836_adresses",
    "atlas_municipal_1888_adresses",
    "ban_adresses",
    "osm_adresses"
]
max_distance = 5  # Distance in meters

table_name = f"{addr_table_settings['schema_name']}.{addr_table_settings['table_name']}"

sources_settings = [
    {
        'source_name': 'cadastre_paris_1807_adresses',
        'file': data_folder + 'cadastre_paris_1807_adresses.geojson',
        'number_prop': 'NUMERO TXT',
        'street_name_prop': 'NOM_SAISI',
        'epsg_code': 2154
    },
    {
        'source_name': 'atlas_vasserot_1810_adresses',
        'file': data_folder + 'atlas_vasserot_1810_adresses.geojson',
        'number_prop': 'num_voies',
        'street_name_prop': 'nom_entier',
        'epsg_code': 4326
    },
    {
        'source_name': 'atlas_jacoubet_1836_adresses',
        'file': data_folder + 'atlas_jacoubet_1836_adresses.geojson',
        'number_prop': 'num_voies',
        'street_name_prop': 'nom_entier',
        'epsg_code': 2154
    },
    {
        'source_name': 'atlas_municipal_1888_adresses',
        'file': data_folder + 'atlas_municipal_1888_adresses.geojson',
        'number_prop': 'numbers_va',
        'street_name_prop': 'normalised',
        'epsg_code': 2154
    }
]

ban_settings = {
    'source_name': 'ban_adresses',
    'file': data_folder + 'ban_adresses.csv',
    'number_prop': 'numero',
    'repetition_prop': 'rep',
    'street_name_prop': 'nom_voie',
    'lat_prop': 'lat',
    'lon_prop': 'lon',
    'epsg_code': 4326
}

osm_settings = {
    'source_name': 'osm_adresses',
    'file': data_folder + 'osm_adresses.csv',
    'hn_file': data_folder + 'osm_hn_adresses.csv',
    'join_prop': 'houseNumberId',
    'number_prop': 'houseNumberLabel',
    'street_name_prop': 'streetName',
    'geom_prop': 'houseNumberGeomWKT',
    'epsg_code': 4326
}

########################################################################################################################

# Launch process

pm.create_postgis_extension()
cat.create_streetnumbers_table(
    pm,
    addr_table_settings['schema_name'],
    addr_table_settings['table_name'],
    addr_table_settings['id_col'],
    addr_table_settings['number_col'],
    addr_table_settings['street_name_col'],
    addr_table_settings['source_col'],
    addr_table_settings['geom_col'],
    addr_table_settings['geom_type'],
    addr_table_settings['epsg_code']
    )
 
print(f"Created table {table_name}.")

for source in sources_settings:
    cat.insert_geojson_features_in_streetnumber_table(
        pm, source['file'], table_name, source['source_name'],
        addr_table_settings['source_col'], addr_table_settings['number_col'], addr_table_settings['street_name_col'], addr_table_settings['geom_col'],
        source['number_prop'], source['street_name_prop'],
        from_epsg=source['epsg_code'], to_epsg=addr_table_settings['epsg_code']
    )

cat.insert_ban_features_in_streetnumber_table(
    pm, ban_settings['file'], table_name, ban_settings['source_name'],
    addr_table_settings['source_col'],
    addr_table_settings['number_col'], addr_table_settings['street_name_col'], addr_table_settings['geom_col'],
    ban_settings['number_prop'], ban_settings['repetition_prop'],
    ban_settings['street_name_prop'], ban_settings['lat_prop'], ban_settings['lon_prop'],
    from_epsg=ban_settings['epsg_code'], to_epsg=addr_table_settings['epsg_code']
)

cat.insert_osm_features_in_streetnumber_table(
    pm, osm_settings['file'], osm_settings['hn_file'], osm_settings['join_prop'],
    table_name, osm_settings['source_name'], addr_table_settings['source_col'],
    addr_table_settings['number_col'], addr_table_settings['street_name_col'], addr_table_settings['geom_col'],
    osm_settings['number_prop'], osm_settings['street_name_prop'], osm_settings['geom_prop'],
    from_epsg=osm_settings['epsg_code'], to_epsg=addr_table_settings['epsg_code']
)

print(f"Inserted features from sources in table {table_name}.")

alfat.add_label_columns_for_table(
    pm,
    addr_table_settings['schema_name'], addr_table_settings['table_name'],
    addr_table_settings['id_col'],
    addr_table_settings['number_col'], addr_table_settings['street_name_col'],
    addr_table_settings['simplified_label_col'], addr_table_settings['normalized_label_col'],
    exceptions=None)

print(f"Added label columns in table {table_name}.")

clt.create_links_table(
    pm,
    links_table_settings['schema_name'], links_table_settings['table_name'],
    links_table_settings['id_col'], links_table_settings['id_from_col'], links_table_settings['id_to_col'],
    links_table_settings['source_from_col'], links_table_settings['source_to_col'],
    links_table_settings['similar_geom_col'], links_table_settings['successive_geom_col'],
    links_table_settings['geom_col'], links_table_settings['geom_type'], links_table_settings['epsg_code'],
)

print(f"Created links table {links_table_settings['schema_name']}.{links_table_settings['table_name']}.")

# Create links between similar addresses
clt.create_links_between_similar_addresses(
    pm,
    links_table_settings['schema_name'], links_table_settings['table_name'],
    addr_table_settings['schema_name'], addr_table_settings['table_name'],
    links_table_settings['id_from_col'], links_table_settings['id_to_col'],
    links_table_settings['source_from_col'], links_table_settings['source_to_col'],
    links_table_settings['geom_col'], links_table_settings['similar_geom_col'], links_table_settings['successive_geom_col'],
    addr_table_settings['id_col'], addr_table_settings['source_col'], addr_table_settings['geom_col'], addr_table_settings['simplified_label_col'],
    source_names,
    links_epsg_code=links_table_settings['epsg_code'], addr_epsg_code=addr_table_settings['epsg_code'], max_distance=max_distance)

print(f"Created links between similar addresses in table {links_table_settings['schema_name']}.{links_table_settings['table_name']}.")

clt.get_successive_geom_links(
    pm,
    links_table_settings['schema_name'], links_table_settings['table_name'],
    source_names,
    links_table_settings['id_from_col'], links_table_settings['source_from_col'],
    links_table_settings['source_to_col'], links_table_settings['successive_geom_col']
    )
print(f"Updated successive geometry links in table {links_table_settings['schema_name']}.{links_table_settings['table_name']}.")

eal.extract_ground_truth_links(
    pm,
    links_table_settings['schema_name'], links_table_settings['table_name'],
    addr_table_settings['schema_name'], addr_table_settings['table_name'],
    links_table_settings['source_from_col'], links_table_settings['source_to_col'],
    links_table_settings['id_from_col'],
    links_table_settings['similar_geom_col'], links_table_settings['successive_geom_col'],
    addr_table_settings['id_col'], addr_table_settings['simplified_label_col'],
    links_ground_truth
)

print(f"Extracted ground truth links to {links_ground_truth}.")

eal.extract_streetnumbers_without_link(
    pm,
    links_table_settings['schema_name'], links_table_settings['table_name'],
    addr_table_settings['schema_name'], addr_table_settings['table_name'],
    links_table_settings['id_from_col'], links_table_settings['id_to_col'],
    addr_table_settings['id_col'], addr_table_settings['source_col'], addr_table_settings['simplified_label_col'],
    sn_without_link_ground_truth
)

print(f"Extracted street numbers without link to {sn_without_link_ground_truth}.")

pm.close()