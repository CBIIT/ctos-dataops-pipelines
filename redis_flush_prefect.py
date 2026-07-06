from prefect import flow
from typing import Literal
import yaml
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.secret_manager import get_secret
import os
import prefect.variables as Variable
from redis_flush import redis_flush

SECRET = "secret"
PROJECT_NAME = "icdc"
REGION = "us-east-1"
ENVIRONMENT = "env"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = "Redis Flush"
    os.environ[APP_NAME] = "Redis Flush"

config_file = "config/prefect_drop_down_config_icdc.yaml"
with open(config_file, "r") as file:
    config = yaml.safe_load(file)
environment_choices = Literal[tuple(list(config.keys()))]


@flow(name="Redis Flush", log_prints=True)
def redis_flush_prefect(
    environment: environment_choices,  # type: ignore
):
    log = get_logger("Redis Flush")
    log.info(f"Flushing Redis for {environment}")

    secret_name = Variable.get(config[environment][SECRET])
    secret = get_secret(secret_name)
    log.info(f"Retrieved secret for environment: {environment}")

    redis_host = secret["redis_host"]
    redis_password = secret["redis_password"]
    log.info(f"Flushing Redis at host: {redis_host}")

    redis_flush(redis_host, redis_password)
    log.info(f"Successfully flushed Redis for environment: {environment}")
