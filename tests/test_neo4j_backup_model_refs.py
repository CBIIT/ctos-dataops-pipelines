import importlib
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from data_model_archiving import data_model_archiving


def load_backup_flow():
    sys.modules.pop("data_asset_generation_prefect", None)
    with patch("github_refs.requests.get") as github_request:
        module = importlib.import_module("data_asset_generation_prefect")
    github_request.assert_not_called()
    return module.data_asset_generation_prefect


def git(repo, *args):
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    )


@pytest.fixture
def model_repo(tmp_path):
    repo = tmp_path / "ins-model"
    repo.mkdir()
    git(repo, "init", "--initial-branch=main")
    git(repo, "config", "user.email", "test@example.com")
    git(repo, "config", "user.name", "Test")
    (repo / "model-desc").mkdir()
    (repo / "model-desc" / "ins-model.yml").write_text("version: main\n")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "Initial model")
    return repo


def test_backup_accepts_future_branches_without_redeployment():
    flow = load_backup_flow()
    schema = flow.parameters.model_dump()
    parameter = schema["properties"]["data_model_version"]
    assert parameter["type"] == "string"
    assert "enum" not in parameter
    assert "new branches need no deployment update" in parameter["description"]
    parameters = dict(environment="dev", data_model_version="future/9.0.0", s3_bucket="test")
    assert flow.validate_parameters(parameters)["data_model_version"] == "future/9.0.0"


def test_archive_fetches_new_branch_latest_commit_and_tags(model_repo):
    load_backup_flow()
    # Create the branch after the flow's parameter schema has been generated.
    git(model_repo, "checkout", "-b", "2.5.0")
    model = model_repo / "model-desc" / "ins-model.yml"
    model.write_text("version: 2.5.0\n")
    git(model_repo, "commit", "-am", "New branch")
    uploaded = []
    paths = []

    def capture_upload(destination, filename):
        assert destination == "s3://test/dump_files"
        paths.append(Path(filename))
        uploaded.append(Path(filename).read_text())

    with patch("data_model_archiving.upload_log_file", side_effect=capture_upload):
        data_model_archiving(model_repo.as_uri(), "2.5.0", "test", "dump_files")
        model.write_text("version: updated\n")
        git(model_repo, "commit", "-am", "Update branch")
        data_model_archiving(model_repo.as_uri(), "2.5.0", "test", "dump_files")
        git(model_repo, "tag", "release-2.5.0")
        data_model_archiving(model_repo.as_uri(), "release-2.5.0", "test", "dump_files")

    assert uploaded == ["version: 2.5.0\n", "version: updated\n", "version: updated\n"]
    assert len(set(paths)) == 3
    assert all(not path.exists() for path in paths)


def test_missing_branch_fails_without_upload(model_repo):
    with patch("data_model_archiving.upload_log_file") as upload:
        with pytest.raises(subprocess.CalledProcessError):
            data_model_archiving(model_repo.as_uri(), "missing-branch", "test", "dump_files")
    upload.assert_not_called()


def test_missing_schemas_fail_without_upload(model_repo):
    git(model_repo, "rm", "model-desc/ins-model.yml")
    git(model_repo, "commit", "-m", "Remove schemas")
    with patch("data_model_archiving.upload_log_file") as upload:
        with pytest.raises(ValueError, match="No model YAML schemas found"):
            data_model_archiving(model_repo.as_uri(), "main", "test", "dump_files")
    upload.assert_not_called()
