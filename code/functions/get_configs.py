import configparser

def get_links_table_settings(config_file):
    """
    Reads address table settings from a configuration INI file.

    Args:
        config_file (str): Path to the config file.

    Returns:
        dict: Dictionary of address table settings including schema, table name, column names, and geometry options.
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    table_params = config['links']

    links_table_settings = {
        'schema_name': table_params.get('schema_name', 'public'),
        'table_name': table_params.get('table_name', 'links'),
        'id_col': table_params.get('id_col', 'id'),
        'id_from_col': table_params.get('id_from_col', 'from_id'),
        'id_to_col': table_params.get('id_to_col', 'to_id'),
        'source_from_col': table_params.get('source_from_col', 'from_source'),
        'source_to_col': table_params.get('source_to_col', 'to_source'),
        'similar_geom_col': table_params.get('similar_geom_col', 'similar_geom'),
        'successive_geom_col': table_params.get('successive_geom_col', 'successive_geom_col'),
        'geom_col': table_params.get('geom_col', 'geometry'),
        'geom_type': table_params.get('geom_type', 'LineString'),
        'epsg_code': table_params.get('epsg_code', '4326')
    }

    return links_table_settings


def get_addresses_table_settings(config_file):
    """
    Reads address table settings from a configuration INI file.

    Args:
        config_file (str): Path to the config file.

    Returns:
        dict: Dictionary of address table settings including schema, table name, column names, and geometry options.
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    table_params = config['addresses']

    addr_table_settings = {
        'schema_name': table_params.get('schema_name', 'public'),
        'table_name': table_params.get('table_name', 'addresses'),
        'id_col': table_params.get('id_col', 'id'),
        'number_col': table_params.get('number_col', 'number'),
        'street_name_col': table_params.get('street_name_col', 'street_name'),
        'normalized_label_col': table_params.get('normalized_label_col', 'normalized_label'),
        'simplified_label_col': table_params.get('simplified_label_col', 'simplified_label'),
        'source_col': table_params.get('source_col', 'source'),
        'geom_col': table_params.get('geom_col', 'geometry'),
        'geom_type': table_params.get('geom_type', 'Point'),
        'epsg_code': table_params.get('epsg_code', '4326')
    }

    return addr_table_settings