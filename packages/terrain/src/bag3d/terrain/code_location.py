"""Conventional code-location entry point, usable without dg project discovery."""

from bag3d.terrain.components.terrain_component import TerrainComponent

defs = TerrainComponent().build_defs()
