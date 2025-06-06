import json
def load_city_config(config_file):
    """Loads city configuration from a JSON file."""
    with open(config_file, "r") as f:
        return json.load(f)