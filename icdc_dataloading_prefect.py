from prefect import flow
from typing import Literal
import os
import subprocess
import tempfile

import yaml
import prefect.variables as Variable

from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.secret_manager import get_secret

from icdc_dataloading import icdc_dataloading

SECRET = "secret"
NEO4J_URI = "neo4j_uri"
NEO4J_USER = "neo4j_user"
NEO4J_PASSWORD = "neo4j_password"

# ICDC data model files, cloned at runtime (same repo/branch as the OpenSearch loader).
MODEL_REPO = "https://github.com/CBIIT/icdc-model-tool.git"
MODEL_BRANCH = "master"
MODEL_FILES_REL = ["model-desc/icdc-model.yml", "model-desc/icdc-model-props.yml"]

# Prop file committed to this repo (shared with the OpenSearch loader).
PROP_FILE_REPO_PATH = "config/os_loader/props-icdc-pmvp.yml"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = "ICDC Data Loader"
    os.environ[APP_NAME] = "ICDC Data Loader"

config_file = "config/prefect_drop_down_config_icdc.yaml"
with open(config_file, "r") as file:
    dropdown_config = yaml.safe_load(file)
environment_choices = Literal[tuple(list(dropdown_config.keys()))]


def _git_clone(repo_url: str, branch: str, dest: str, log):
    log.info(f"Cloning {repo_url} (branch: {branch}) -> {dest}")
    subprocess.run(
        ["git", "clone", "--depth", "1", "--branch", branch, repo_url, dest],
        check=True,
    )


@flow(name="ICDC Data Loader", log_prints=True)
def icdc_dataloading_prefect(
    environment: environment_choices,  # type: ignore
    s3_bucket: str,
    s3_folder: str,
    mode: str = "upsert",
    cheat_mode: bool = False,
    dry_run: bool = False,
    wipe_db: bool = False,
):
    """
    Load TSV files from S3 into Neo4j for the selected ICDC environment.

    Note: Redis flush is a separate deployment (redis_flush_prefect.py) and is
    not triggered automatically by this flow.
    """
    log = get_logger("ICDC Data Loader")

    env_cfg = dropdown_config[environment]
    secret_name = Variable.get(env_cfg[SECRET])
    secret = get_secret(secret_name)

    with tempfile.TemporaryDirectory() as tmpdir:
        model_dir = os.path.join(tmpdir, "model")
        _git_clone(MODEL_REPO, MODEL_BRANCH, model_dir, log)
        schema_files = [os.path.join(model_dir, p) for p in MODEL_FILES_REL]

        argList = {
            "neo4j_uri": secret[NEO4J_URI],
            "neo4j_user": secret[NEO4J_USER],
            "neo4j_password": secret[NEO4J_PASSWORD],
            "schema_files": schema_files,
            "prop_file": PROP_FILE_REPO_PATH,
            "s3_bucket": s3_bucket,
            "s3_folder": s3_folder,
            "mode": mode,
            "cheat_mode": cheat_mode,
            "dry_run": dry_run,
            "wipe_db": wipe_db,
        }
        icdc_dataloading(argList)


if __name__ == "__main__":
    icdc_dataloading_prefect.serve(name="icdc_dataloading")
