import json
import configparser
import psycopg2
from shapely.geometry import shape
from shapely import wkt
# import functions.db_connection as db_conn
import db_connection as db_conn

def launch_update_query(conn, query, error_message=None):
    if error_message is None:
        error_message = f"❌ Error executing query: {e}"
    try:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()
        print("✅ Query executed successfully.")
    except Exception as e:
        conn.rollback()
        print(error_message)

def create_postgis_extension(conn):
    query = "CREATE EXTENSION IF NOT EXISTS postgis;"
    launch_update_query(conn, query)
    
def create_schema(conn, schema_name):
    query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    error_message = f"❌ Error while creating {schema_name} (maybe permission denied for database)"
    launch_update_query(conn, query, error_message)

def create_housenumbers_table(
    conn, schema_name, table_name,
    id_col, number_col, street_name_col, source_col, geom_col,
    geom_type="Point", epsg_code=4326
):
    
    name = table_name
    if schema_name is not None:
        name = f"{schema_name}.{table_name}"
        create_schema(conn, schema_name)

    query = f"""
    DROP TABLE IF EXISTS {name};
    CREATE TABLE IF NOT EXISTS {name} (
        {id_col} SERIAL PRIMARY KEY,
        {number_col} TEXT,
        {street_name_col} TEXT,
        {source_col} TEXT,
        {geom_col} GEOMETRY({geom_type}, {epsg_code})
    );
    """
    
    launch_update_query(conn, query)

def get_addresses_table_settings(config_file):
     # Lire le fichier de configuration
    config = configparser.ConfigParser()
    config.read(config_file)

    # Extraire les paramètres de connexion
    table_params = config['addresses']

    addr_table_settings = {
        'schema_name': table_params.get('schema_name', 'localhost'),
        'table_name': table_params.get('table_name', 'addresses'),
        'id_col': table_params.get('id_col', 'id'),
        'number_col': table_params.get('number_col', 'number'),
        'street_name_col': table_params.get('street_name_col', 'street_name'),
        'source_col': table_params.get('source_col', 'source'),
        'geom_col': table_params.get('geom_col', 'geometry'),
        'geom_type': table_params.get('geom_type', 'Point'),
        'epsg_code': table_params.get('epsg_code', 'addresses')
    }

    return addr_table_settings

def insert_geojson_features_in_database(conn, cur, geojson_file):

    # Charger le GeoJSON
    with open(geojson_file) as f:
        data = json.load(f)

    # Parcourir les entités
    for feature in data['features']:
        props = feature['properties']
        geom = shape(feature['geometry'])
        wkt_geom = geom.wkt
        # Exemple simple : insérer la géométrie et un attribut
        cur.execute("INSERT INTO ma_table (nom_colonne, geom) VALUES (%s, ST_GeomFromText(%s, 4326))",
                    (props.get("nom_colonne"), wkt_geom))


if __name__ == "__main__":
    config_file = "../../configs/db_config.ini"

    # Connexion à PostgreSQL
    conn = db_conn.connect_bdd_from_config_file(config_file)

    create_postgis_extension(conn)

    addr_table_settings = get_addresses_table_settings(config_file)
    create_housenumbers_table(conn, **addr_table_settings)

    conn.close()
