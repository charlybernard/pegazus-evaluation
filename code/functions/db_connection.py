import psycopg2
import configparser

def connect_bdd_from_config_file(config_file: str):
    # Lire le fichier de configuration
    config = configparser.ConfigParser()
    config.read(config_file)

    # Extraire les paramètres de connexion
    db_params = config['postgresql']

    # Construire le dict des paramètres pour psycopg2
    conn_params = {
        'host': db_params.get('host', 'localhost'),
        'port': int(db_params.get('port', 5432)),
        'dbname': db_params.get('database', ''),
        'user': db_params.get('user', '')
    }

    # Ajouter le mot de passe seulement s'il est défini et non vide
    password = db_params.get('password', None)
    
    if password:
        conn_params['password'] = password

    # Connexion à la base de données
    conn = psycopg2.connect(**conn_params)

    return conn