import yaml
from pathlib import Path

def load_config(config_path="config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

if __name__ == "__main__":
    cfg = load_config()
    print("Config loaded successfully for mode:", cfg.get("mode"))