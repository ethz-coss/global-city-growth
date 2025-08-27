import yaml
import os
import dagster as dg

def load_constants():
    # Constants are defined in the dbt_project.yml file because otherwise it is rather difficult to load them in the dbt project
    with open(f"src/warehouse/dbt_project.yml", "r") as f:
        constants = yaml.safe_load(f)
        constants = constants["vars"]["constants"]
    return constants

constants = load_constants()


if __name__ == "__main__":
    print(constants)