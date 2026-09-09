from opensearch_backup_universal_prefect import ins_opensearch_backup_prefect
from opensearch_restore_universal_prefect import (
    ins_promote_dropdown_config,
    ins_opensearch_promote_prefect,
    ins_opensearch_restore_prefect,
    opensearch_restore_prefect,
)


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
    assert schema["properties"]["indices"]["type"] == "array"
    assert schema["properties"]["indices"]["items"] == {"type": "string"}
    assert schema["properties"]["indices"]["default"] == []


def test_universal_opensearch_restore_parameter_schema():
    schema = opensearch_restore_prefect.parameters.model_dump()

    assert opensearch_restore_prefect.name == "OpenSearch restore"
    assert list(schema["properties"]) == [
        "snapshot_name",
        "secret_name_prefect_variable",
        "aws_role_prefect_variable",
        "opensearch_repo",
        "s3_bucket",
        "indices",
        "aws_operations_role",
    ]
    assert schema["properties"]["snapshot_name"]["type"] == "string"
    assert schema["properties"]["secret_name_prefect_variable"]["type"] == "string"
    assert schema["properties"]["aws_role_prefect_variable"]["type"] == "string"
    assert schema["properties"]["s3_bucket"]["type"] == "string"
    assert schema["properties"]["opensearch_repo"]["type"] == "string"
    assert schema["properties"]["indices"]["type"] == "array"
    assert schema["properties"]["indices"]["items"] == {"type": "string"}
    assert schema["properties"]["indices"]["default"] == []
    assert "indices" not in schema["required"]


def test_ins_opensearch_promote_parameter_schema():
    schema = ins_opensearch_promote_prefect.parameters.model_dump()

    assert ins_opensearch_promote_prefect.name == "OpenSearch promote"
    assert list(schema["properties"]) == [
        "environment",
        "snapshot_name",
        "s3_bucket",
        "opensearch_repo",
        "indices",
    ]
    assert schema["properties"]["environment"]["enum"] == ["stage", "prod"]
    assert schema["properties"]["indices"]["type"] == "array"
    assert schema["properties"]["indices"]["items"] == {"type": "string"}
    assert schema["properties"]["indices"]["default"] == []
    assert ins_promote_dropdown_config == {
        "stage": {"secret_name_prefect_variable": "ins_secret_name_stage"},
        "prod": {"secret_name_prefect_variable": "ins_secret_name_prod"},
    }
