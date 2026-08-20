import importlib

from dagster import Definitions


def test_definitions_loadable():
    # Import your definitions module
    module = importlib.import_module("bag3d.floors_estimation.code_location")
    defs = module.defs

    # This will raise an error if definitions are not loadable
    Definitions.validate_loadable(defs)
