import psycopg2
import configparser

def connect_bdd_from_config_file(config_file:str):
    # Lire le fichier de configuration
    config = configparser.ConfigParser()
    config.read(config_file)

    # Extraire les paramètres de connexion
    db_params = config['postgresql']

    # Connexion à la base de données
    conn = psycopg2.connect(
        host=db_params['host'],
        port=db_params['port'],
        dbname=db_params['database'],
        user=db_params['user'],
        password=db_params['password']
    )

    return conn