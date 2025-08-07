import os
import yaml

class ConfigManager:
    def __init__(self):
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
    
    def get(self, key):
        return self.config.get(key)