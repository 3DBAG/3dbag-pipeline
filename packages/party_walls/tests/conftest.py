import sys
import types

pytest_plugins = ["bag3d.common.testing.conftest_plugin"]


walls_module = types.ModuleType("building_surfaces.walls")
walls_module.shared_walls = lambda *args, **kwargs: {}
walls_module.write_cityjsonfeature = lambda *args, **kwargs: None
sys.modules.setdefault("building_surfaces", types.ModuleType("building_surfaces"))
sys.modules["building_surfaces.walls"] = walls_module
