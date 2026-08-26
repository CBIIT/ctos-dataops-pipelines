from opensearch_backup_universal_prefect import ins_opensearch_backup_prefect
from opensearch_restore_universal_prefect import ins_opensearch_restore_prefect


def assert_common_ins_parameter_schema(flow):
    schema = flow.parameters.model_dump()

    assert list(schema["properties"]) == [
        "environment",
        "snapshot_name",
        "s3_bucket",
        "opensearch_repo",
        "indices",
    ]
    assert schema["properties"]["environment"]["enum"] == ["dev", "qa"]
    assert schema["properties"]["environment"]["type"] == "string"
    assert schema["properties"]["snapshot_name"]["type"] == "string"
    assert schema["properties"]["s3_bucket"]["type"] == "string"
    assert schema["properties"]["opensearch_repo"]["type"] == "string"
    assert "indices" not in schema["required"]
    return schema


def test_ins_opensearch_backup_parameter_schema():
    schema = assert_common_ins_parameter_schema(ins_opensearch_backup_prefect)

    assert ins_opensearch_backup_prefect.name == "OpenSearch backup"
    assert schema["properties"]["indices"]["type"] == "array"
    assert schema["properties"]["indices"]["items"] == {"type": "string"}
    assert schema["properties"]["indices"]["default"] == []


def test_ins_opensearch_restore_parameter_schema():
    schema = assert_common_ins_parameter_schema(ins_opensearch_restore_prefect)

    assert ins_opensearch_restore_prefect.name == "OpenSearch restore"
    assert schema["properties"]["indices"]["type"] == "string"
    assert schema["properties"]["indices"]["default"] == ""
