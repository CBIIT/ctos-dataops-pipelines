from opensearch_backup_universal_prefect import ins_opensearch_backup_prefect
from opensearch_restore_universal_prefect import ins_opensearch_restore_prefect


def assert_ins_parameter_schema(flow):
    schema = flow.parameters.model_dump()

    assert list(schema["properties"]) == [
        "environment",
        "snapshot_name",
        "s3_bucket",
        "opensearch_repo",
        "indices",
    ]
    assert schema["properties"]["environment"]["enum"] == ["dev", "qa"]
    assert schema["properties"]["indices"]["type"] == "string"
    assert schema["properties"]["indices"]["default"] == ""
    assert "indices" not in schema["required"]


def test_ins_opensearch_backup_parameter_schema():
    assert_ins_parameter_schema(ins_opensearch_backup_prefect)


def test_ins_opensearch_restore_parameter_schema():
    assert_ins_parameter_schema(ins_opensearch_restore_prefect)
