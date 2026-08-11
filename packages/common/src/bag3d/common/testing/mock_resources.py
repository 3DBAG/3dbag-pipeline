import dagster as dg

MOCK_RESOURCES = {
    "file_store": dg.ResourceDefinition.mock_resource(),
    "pointcloud_store": dg.ResourceDefinition.mock_resource(),
    "integration_data_store": dg.ResourceDefinition.mock_resource(),
    "gdal": dg.ResourceDefinition.mock_resource(),
    "computation_db": dg.ResourceDefinition.mock_resource(),
    "publication_db": dg.ResourceDefinition.mock_resource(),
    "publication_server": dg.ResourceDefinition.mock_resource(),
    "pdal": dg.ResourceDefinition.mock_resource(),
    "lastools": dg.ResourceDefinition.mock_resource(),
    "tyler": dg.ResourceDefinition.mock_resource(),
    "validation": dg.ResourceDefinition.mock_resource(),
    "roofer": dg.ResourceDefinition.mock_resource(),
    "version": dg.ResourceDefinition.mock_resource(),
    "specs": dg.ResourceDefinition.mock_resource(),
    "model_store": dg.ResourceDefinition.mock_resource(),
    "nl_transform": dg.ResourceDefinition.mock_resource(),
    "reconstruction_index": dg.ResourceDefinition.mock_resource(),
    "party_walls_index": dg.ResourceDefinition.mock_resource(),
}
