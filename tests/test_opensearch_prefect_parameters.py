from unittest.mock import patch

import pytest

from opensearch_backup_universal_prefect import ins_opensearch_backup_prefect
from opensearch_restore_universal_prefect import (
    ES_HOST,
    PROMOTE_ES_HOST,
    get_promote_environment_config,
    ins_promote_dropdown_config,
    ins_opensearch_promote_prefect,
    ins_opensearch_restore_prefect,
    opensearch_restore_prefect,
)


def test_restore_and_promote_use_different_secret_host_keys():
    assert ES_HOST == "es_host"
    assert PROMOTE_ES_HOST == "opensearch_host"


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
        "base_path",
        "s3_bucket",
        "opensearch_repo",
        "indices",
    ]
    assert schema["properties"]["environment"]["enum"] == ["stage", "prod"]
    assert schema["properties"]["snapshot_name"]["type"] == "string"
    assert schema["properties"]["base_path"]["type"] == "string"
    assert "default" not in schema["properties"]["snapshot_name"]
    assert "default" not in schema["properties"]["base_path"]
    assert "snapshot_name" in schema["required"]
    assert "base_path" in schema["required"]
    assert schema["properties"]["indices"]["type"] == "array"
    assert schema["properties"]["indices"]["items"] == {"type": "string"}
    assert schema["properties"]["indices"]["default"] == []
    assert ins_promote_dropdown_config == {
        "stage": {
            "secret_name_prefect_variable": "ins_secret_name_stage",
            "opensearch_operations_role_arn": (
                "arn:aws:iam::697201234594:role/ins-prod-prefect-operations"
            ),
            "opensearch_snapshot_role_arn": (
                "arn:aws:iam::697201234594:role/"
                "power-user-ccdi-stage-ins-opensearch-snapshot"
            ),
        },
        "prod": {
            "secret_name_prefect_variable": "ins_secret_name_prod",
            "opensearch_operations_role_arn": None,
            "opensearch_snapshot_role_arn": None,
        },
    }


def test_stage_promote_roles_are_selected_from_dropdown_config():
    config = get_promote_environment_config("stage")

    assert config["opensearch_operations_role_arn"] == (
        "arn:aws:iam::697201234594:role/ins-prod-prefect-operations"
    )
    assert config["opensearch_snapshot_role_arn"] == (
        "arn:aws:iam::697201234594:role/"
        "power-user-ccdi-stage-ins-opensearch-snapshot"
    )


def test_promote_uses_separate_logical_snapshot_name_and_s3_base_path():
    with patch(
        "opensearch_restore_universal_prefect.run_opensearch_restore"
    ) as restore:
        ins_opensearch_promote_prefect.fn(
            environment="stage",
            snapshot_name="3.4.0.4",
            base_path="opensearch-backup-2026-06-26",
            s3_bucket="ccdi-stage-ins-opensearch-snapshot-bucket",
            opensearch_repo="ins",
            indices=[],
        )

    assert restore.call_args.args[0] == "3.4.0.4"
    assert restore.call_args.args[-2] == "opensearch-backup-2026-06-26"


@pytest.mark.parametrize(
    ("snapshot_name", "base_path", "message"),
    [
        ("", "opensearch-backup-2026-06-26", "snapshot_name is required"),
        ("3.4.0.4", "", "base_path is required"),
    ],
)
def test_promote_rejects_blank_snapshot_name_or_base_path(
    snapshot_name, base_path, message
):
    with pytest.raises(ValueError, match=message):
        ins_opensearch_promote_prefect.fn(
            environment="stage",
            snapshot_name=snapshot_name,
            base_path=base_path,
            s3_bucket="ccdi-stage-ins-opensearch-snapshot-bucket",
            opensearch_repo="ins",
            indices=[],
        )


def test_prod_promote_fails_until_roles_are_configured():
    with pytest.raises(ValueError, match="OpenSearch promote is not configured for 'prod'"):
        get_promote_environment_config("prod")
