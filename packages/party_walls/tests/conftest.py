import sys
import types


walls_module = types.ModuleType("building_surfaces.walls")
setattr(walls_module, "shared_walls", lambda *args, **kwargs: {})
setattr(walls_module, "write_cityjsonfeature", lambda *args, **kwargs: None)

sys.modules.setdefault("building_surfaces", types.ModuleType("building_surfaces"))
sys.modules["building_surfaces.walls"] = walls_module
