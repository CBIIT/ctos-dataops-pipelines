from prefect import flow
from typing import Literal
import os
import subprocess
import tempfile

import boto3
import yaml
import prefect.variables as Variable

from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.secret_manager import get_secret

from opensearch_loader import opensearch_loader

SECRET = "secret"
ENV = "env"
ES_HOST = "es_host"
NEO4J_IP = "neo4j_ip"
NEO4J_USER = "neo4j_user"
NEO4J_PASSWORD = "neo4j_password"
REGION = "us-east-1"

# ---- External repos cloned at runtime (public GitHub) ----
# Indices YAML lives in the ICDC Spring Boot backend repo.
BACKEND_REPO = "https://github.com/CBIIT/bento-icdc-backend.git"
BACKEND_BRANCH = "main"  # not env-branched; only `main` and `develop` exist
BACKEND_INDICES_FILE_REL = "src/main/resources/yaml/es_indices_icdc.yml"

# About-page content: env-branched (develop/qa/stage/production).
ABOUT_PAGE_REPO = "https://github.com/CBIIT/bento-icdc-static-content.git"
ABOUT_PAGE_FILE_REL = "aboutPagesContent.yaml"
TIER_TO_STATIC_BRANCH = {
    "dev": "develop",
    "qa": "qa",
    "stage": "stage",
    "prod": "production",
}

# ICDC data model files.
MODEL_REPO = "https://github.com/CBIIT/icdc-model-tool.git"
MODEL_BRANCH = "master"
MODEL_FILES_REL = ["model-desc/icdc-model.yml", "model-desc/icdc-model-props.yml"]

# Prop file committed to this repo (copy from legacy loader repo)
PROP_FILE_REPO_PATH = "config/os_loader/props-icdc-pmvp.yml"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = "OpenSearch Loading"
    os.environ[APP_NAME] = "OpenSearch Loading"

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


def _assume_role(role_arn: str, session_name: str = "os-loading"):
    sts = boto3.client("sts")
    resp = sts.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
    c = resp["Credentials"]
    return boto3.Session(
        aws_access_key_id=c["AccessKeyId"],
        aws_secret_access_key=c["SecretAccessKey"],
        aws_session_token=c["SessionToken"],
    )


@flow(name="OpenSearch Loading", log_prints=True)
def opensearch_loader_prefect(
    environment: environment_choices,  # type: ignore
    indices_file: str = "",
    indices_filter: str = "",
):
    """
    Load OpenSearch indices from Neo4j for the selected environment.

    Parameters:
        environment    : dropdown env key (icdc_dev, icdc_qa, ...)
        indices_file   : optional local override path. If empty (default),
                         the indices YAML is fetched from the ICDC backend repo.
        indices_filter : optional comma-separated list of index names to load.
                         Empty (default) means load all.
    """
    log = get_logger("OpenSearch Loading")

    env_cfg = dropdown_config[environment]
    secret_name = Variable.get(env_cfg[SECRET])
    tier = env_cfg[ENV]  # dev / qa / stage / prod

    if tier not in TIER_TO_STATIC_BRANCH:
        raise ValueError(
            f"Unknown tier '{tier}' for environment '{environment}'. "
            f"Expected one of: {list(TIER_TO_STATIC_BRANCH.keys())}"
        )
    static_branch = TIER_TO_STATIC_BRANCH[tier]

    secret = get_secret(secret_name)
    role_arn = Variable.get("icdc_role_arn")
    aws_session = _assume_role(role_arn)

    filter_list = [i.strip() for i in indices_filter.split(",") if i.strip()]

    with tempfile.TemporaryDirectory() as tmpdir:
        about_dir = os.path.join(tmpdir, "about")
        model_dir = os.path.join(tmpdir, "model")
        backend_dir = os.path.join(tmpdir, "backend")

        _git_clone(ABOUT_PAGE_REPO, static_branch, about_dir, log)
        _git_clone(MODEL_REPO, MODEL_BRANCH, model_dir, log)

        if indices_file:
            log.info(f"Using indices_file override: {indices_file}")
            resolved_indices_file = indices_file
        else:
            _git_clone(BACKEND_REPO, BACKEND_BRANCH, backend_dir, log)
            resolved_indices_file = os.path.join(backend_dir, BACKEND_INDICES_FILE_REL)

        about_file_path = os.path.join(about_dir, ABOUT_PAGE_FILE_REL)
        model_files_paths = [os.path.join(model_dir, p) for p in MODEL_FILES_REL]

        argList = {
            "indices_file": resolved_indices_file,
            "es_host": secret[ES_HOST],
            "neo4j_uri": f"bolt://{secret[NEO4J_IP]}:7687",
            "neo4j_user": secret[NEO4J_USER],
            "neo4j_password": secret[NEO4J_PASSWORD],
            "about_file": about_file_path,
            "model_files": model_files_paths,
            "prop_file": PROP_FILE_REPO_PATH,
            "region": REGION,
            "aws_session": aws_session,
            "indices_filter": filter_list,
        }
        opensearch_loader(argList)


if __name__ == "__main__":
    opensearch_loader_prefect.serve(name="opensearch_loading")
