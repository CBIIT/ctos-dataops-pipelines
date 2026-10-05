# ICDC DataOps Pipelines (Prefect)

Prefect-based data operations pipelines for the Integrated Canine Data Commons
(ICDC), migrated from Jenkins. This branch (`dataops_icdc_pipelines`) contains
only the ICDC-specific flows and configuration.

## What's here

| Flow | Entry point | Purpose |
|---|---|---|
| OpenSearch Backup | `opensearch_backup_prefect.py` | Snapshot OpenSearch indices to S3 |
| OpenSearch Restore | `opensearch_restore_prefect.py` | Restore OpenSearch indices from an S3 snapshot |
| OpenSearch Indices Loading | `opensearch_loader_prefect.py` | Load OpenSearch indices from Neo4j (+ about-page content, data model, external placeholders) |
| ICDC Neo4j Data Loading | `icdc_dataloading_prefect.py` | Load TSV submission files from S3 into Neo4j |
| Redis Flush | `redis_flush_prefect.py` | Flush the ICDC Redis cache for an environment |

Each flow has a separate deployment for each tier (`dev`, `qa`, `stage`,
`prod`), for 20 deployments total. Each deployment sets its environment
parameter (`icdc_dev`, `icdc_qa`, etc.); the environment-to-secret and IAM
variable mapping is in `config/prefect_drop_down_config_icdc.yaml`.

## Repo layout

- `opensearch_backup.py` / `opensearch_restore.py` / `opensearch_utils.py` — core OpenSearch snapshot/restore logic (SigV4 signing, cross-account role assumption)
- `opensearch_loader.py` / `os_loader_icdc_schema.py` / `os_loader_icdc_props.py` — core OpenSearch indices loader (Neo4j → OpenSearch), and the ICDC data model/props parser it shares with the Neo4j loader
- `icdc_data_loader.py` / `icdc_dataloading.py` — core Neo4j data loader (TSV → Neo4j), ported from the legacy `icdc-dataloader` repo
- `redis_flush.py` — core Redis flush logic
- `*_prefect.py` — Prefect flow wrappers for each of the above (secret/variable lookup, environment dropdown, runtime cloning of external repos)
- `bento/` — shared utilities (logging, S3, Secrets Manager helpers, ICDC schema base classes) — git submodule/vendored from the Bento framework
- `config/icdc_ctos_dataops_prefect.yaml` — Prefect deployment definitions (`prefect.yaml`-style)
- `config/prefect_drop_down_config_icdc.yaml` — maps each environment (`icdc_dev`, `icdc_qa`, ...) to its Secrets Manager secret variable, tier (`dev`/`qa`/`stage`/`prod`), and IAM role ARN variables
- `config/os_loader/props-icdc-pmvp.yml` — ICDC data model properties file (id fields, plurals, indexes) shared by the OpenSearch and Neo4j loaders

## Runtime dependencies fetched from other repos

Several flows clone other CBIIT repos at runtime rather than vendoring their
content, since it changes independently of this repo:

- **ICDC data model** — `icdc-model-tool` (branch `master`)
- **OpenSearch indices YAML** — `bento-icdc-backend` (branch `main`)
- **About-page content** — `bento-icdc-static-content` (branch mapped per tier: `dev→develop`, `qa→qa`, `stage→stage`, `prod→production`)

## Configuration

Each environment entry in `config/prefect_drop_down_config_icdc.yaml` has:

```yaml
icdc_dev:
  secret: "icdc_secret_name_dev"       # Prefect Variable holding the Secrets Manager secret name
  env: dev                             # tier, used for branch selection etc.
  role_arn: "icdc_role_arn_nonprod"    # Prefect Variable holding the SigV4 operations role ARN
  os_role_arn: "icdc_os_role_arn_nonprod"  # Prefect Variable holding the OpenSearch snapshot role ARN
```

The referenced secret must contain (depending on which flows you run):
`es_host`, `neo4j_uri`, `neo4j_user`, `neo4j_password`, `redis_host`, `redis_password`.

## Local testing

Each core module has a CLI entry point independent of Prefect, useful for
testing against a local Neo4j/OpenSearch instance:

```bash
# OpenSearch indices loading
python opensearch_loader.py <indices_yaml_path> <local_config.yml>

# Neo4j data loading
python icdc_dataloading.py <local_config.yml>
```

See the `Config:` keys expected by each module's `main()` for the local
config file shape (mirrors the Prefect flow's `argList`).

## Deploying

The deployment definitions are in `config/icdc_ctos_dataops_prefect.yaml`.
Deploy them to the intended Prefect Cloud workspace:

```bash
prefect deploy --prefect-file config/icdc_ctos_dataops_prefect.yaml --all
```

All deployments currently name the same work pool,
`crdc-curation-16gb-prefect-3.2.14-python3.9`. This is a configuration value,
not proof that Stage/Prod runs on a prod-side worker. Confirm the correct pool
name with the cloud team and update the Stage/Prod deployment definitions if
they require a distinct pool.

## Verifying Stage/Prod execution

1. In the intended Prefect Cloud workspace, open **Work Pools**. Confirm the
  pool named by the deployment exists, is the expected type, and is connected
  to the intended worker or ECS configuration. Also confirm the deployment
  uses that pool and the expected queue. The work pool is a Prefect resource,
  not an ECS resource.
2. Use the pool's base job template or worker configuration to identify the
  AWS ECS cluster, task definition, networking, and IAM roles it uses. In the
  prod AWS account and configured region, verify those referenced resources
  exist and are configured for the prod-side worker or flow task.
3. For a persistent ECS worker, check that its ECS service has a running task
  and that worker logs show it polling the intended pool. With an ECS push
  pool, Prefect launches ECS tasks for flow runs; verify the launched task
  instead of expecting a persistent worker service.
4. Check the flow task's IAM task role for access to the required Secrets
  Manager secrets and AWS resources. Check its subnets, routes, and security
  groups for connectivity to the tier's Neo4j, OpenSearch, and Redis endpoints.
  Use CloudWatch Logs to inspect worker or flow-task startup and execution.
5. Start with an approved, non-mutating check. Do not use a Prod restore, Neo4j
  data load, or Redis flush as a connectivity test; those flows can change or
  delete data.

## Requirements

See `requirements.txt`. Notably:
- `neo4j>=5,<6` — pinned because ICDC's Neo4j servers run 4.4, which the v6 driver dropped support for
- `opensearch-py` — used instead of the `elasticsearch` client, which (v8+) rejects non-Elasticsearch clusters
