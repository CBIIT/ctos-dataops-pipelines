# CTOS data operations pipelines

## INS Prefect configuration and Neo4j backup/restore

This guide explains how `config/ins-prefect.yaml` customizes the repository's Prefect deployments for INS and how to run the INS Neo4j backup and restore flows from Prefect Cloud.

## INS Prefect YAML

The INS deployment configuration is stored in `config/ins-prefect.yaml`. It differs from the repository's other Prefect configurations in the following ways:

- The pull step clones `https://github.com/CBIIT/ctos-dataops-pipelines.git` at the `ins-pipelines` branch.
- Deployments run in the `ccdi-dcc-8gb-prefect-3.4.19-python3.13` work pool and its `default` queue.
- Deployment names are prefixed with `ins-`.
- Environment-specific Neo4j secrets are selected through `config/prefect_drop_down_config.yaml`.
- S3 bucket names are resolved from Prefect variables rather than being hardcoded in the deployment.
- The file also defines INS deployments for the universal OpenSearch snapshot flows. Those flows are outside the scope of the operating instructions below.

The Neo4j deployments are:

| Deployment | Entrypoint | Purpose |
|---|---|---|
| `ins-neo4j-backup` | `data_asset_generation_prefect.py:data_asset_generation_prefect` | Generate a database summary, archive the data model, create a Neo4j dump, and upload the assets to S3. |
| `ins-neo4j-restore` | `data_asset_loading_prefect.py:data_asset_loading_prefect` | Download a dump, replace the Neo4j database, create a new summary, and compare it with the backup summary. |

`flow_name: null` is intentional. Prefect reads each flow's name from its Python `@flow` decorator, while the YAML `name` identifies the deployment.

## Required Prefect variables and AWS secrets

Create these variables in the same Prefect Cloud workspace in which the deployments run:

| Prefect variable | Value |
|---|---|
| `ins_dataops_backup_bucket` | Plain S3 bucket name, for example `ccdi-nonprod-ins-neo4j-datadump-bucket`. Do not use an ARN or an `s3://` URI. |
| `ins_secret_name_dev` | Name or ARN of the AWS Secrets Manager secret containing the development Neo4j endpoint and database credentials. |
| `ins_neo4j_ssh_secret_name` | Name or ARN of the AWS Secrets Manager secret containing the operating-system SSH username and private key. |

`config/prefect_drop_down_config.yaml` maps the flow's `environment` parameter to Prefect variable names:

```yaml
dev:
  neo4j_summary_secret: ins_secret_name_dev
  neo4j_ssh_secret: ins_neo4j_ssh_secret_name
```

The database secret referenced by `ins_secret_name_dev` must contain:

```json
{
  "neo4j_ip": "10.0.0.10",
  "neo4j_user": "neo4j",
  "neo4j_password": "REDACTED"
}
```

The SSH secret referenced by `ins_neo4j_ssh_secret_name` must contain:

```json
{
  "neo4j_prefect_user": "ccdi-docker",
  "neo4j_key": "-----BEGIN OPENSSH PRIVATE KEY-----\n...\n-----END OPENSSH PRIVATE KEY-----\n"
}
```

The `neo4j_key` value is the private key itself, not a filename or public key. In the raw JSON, line breaks are represented by `\n`. After JSON parsing, the value must contain actual newline characters; it must not contain literal backslash-plus-`n` characters. The current SSH loader expects an unencrypted RSA private key.

The database user and SSH user are deliberately separate:

- `neo4j_user` authenticates to the Neo4j Bolt endpoint for summary queries.
- `neo4j_prefect_user` authenticates to the host over SSH to stop Neo4j and run `neo4j-admin`.

## AWS and network prerequisites

The IAM task role used by the Prefect ECS work pool must be able to:

- call `secretsmanager:GetSecretValue` for both secrets;
- call `s3:ListBucket` on the backup bucket;
- call `s3:GetObject` and `s3:PutObject` for objects under the selected S3 prefix;
- call `s3:PutObjectAcl` if required for the `bucket-owner-full-control` ACL used by this repository;
- use the applicable KMS key if the secrets or bucket use a customer-managed key.

The ECS task must have network access to:

- SSH on the Neo4j host;
- Neo4j Bolt on port `7687`;
- AWS Secrets Manager and S3.

The SSH user must have passwordless `sudo` access for the commands used by the flows, including stopping and starting Neo4j and running `neo4j-admin`. Its public key must be installed in the user's `authorized_keys` file on the Neo4j host.

## Deploy or update the deployments

Authenticate the local Prefect CLI to the intended Prefect Cloud workspace:

```bash
prefect cloud login
prefect variable get ins_dataops_backup_bucket
```

From the repository root, create or update the deployments:

```bash
prefect deploy \
  --prefect-file config/ins-prefect.yaml \
  --name ins-neo4j-backup

prefect deploy \
  --prefect-file config/ins-prefect.yaml \
  --name ins-neo4j-restore
```

Running the same command again with the same deployment and flow names updates the existing deployment; it does not require deleting it first.

Because each flow run clones `ins-pipelines`, Python-only changes take effect after they are committed and pushed to that branch. Redeploy when deployment configuration in `config/ins-prefect.yaml` changes.

## Run a Neo4j backup in Prefect Cloud

1. In Prefect Cloud, open **Deployments** and select `ins-neo4j-backup`.
2. Select **Run**, then configure a custom run.
3. Set the parameters described below.
4. Start the run and follow its logs until the parent flow and all subflows complete.
5. Verify the expected files in the configured S3 bucket and folder.

Backup parameters:

| Parameter | Description |
|---|---|
| `environment` | Environment key from `config/prefect_drop_down_config.yaml`, normally `dev`. |
| `data_model_version` | Git branch, tag, or commit to check out from the data-model repository. |
| `data_model_repo_url` | Clone URL for the data-model repository. |
| `s3_folder` | S3 key prefix that groups all assets from this backup, such as `dump_files`. If blank, the flow generates a timestamped `neo4j-assets-*` folder. |
| `neo4j_summary_file_name` | Backup-time inventory JSON name, such as `DevDump_2026-08-13-13-50.json`. |
| `neo4j_dump_file_name` | Dump filename, such as `DevDump_2026-08-13-13-50.dump`. |
| `s3_bucket` | Resolved from `ins_dataops_backup_bucket`; normally leave the configured value unchanged. |

The backup executes in this order:

1. Query Neo4j and save counts for all nodes and relationships in the summary JSON.
2. Archive the selected data-model files.
3. Connect to the Neo4j host over SSH.
4. Stop Neo4j and run `neo4j-admin dump`.
5. Restart Neo4j, copy the dump to the Prefect task, and upload it to S3.

Use the same S3 folder for the dump and its summary. Record their exact names; the restore flow needs both.

## Run a Neo4j restore in Prefect Cloud

> **Warning:** This is a destructive operation. It stops Neo4j and replaces the current `neo4j` database with the selected dump. Confirm the target environment, S3 folder, and dump name before starting the run.

1. In Prefect Cloud, open **Deployments** and select `ins-neo4j-restore`.
2. Select **Run**, then configure a custom run.
3. Enter the exact S3 folder and filenames produced by the backup.
4. Give the new restore summary a distinct filename.
5. Start the run and monitor it through validation.

Restore parameters:

| Parameter | Description |
|---|---|
| `environment` | Target environment, normally `dev`. |
| `s3_folder` | Folder containing the selected dump and backup summary. |
| `dump_file_name` | Existing dump object's basename. Do not include `s3://`, the bucket, or the folder. |
| `validation_summary_file_name` | Existing backup-time summary JSON stored next to the dump. |
| `restore_summary_file_name` | Name for a new post-restore summary, such as `DevDump_2026-08-13-14-43_restore.json`. |
| `s3_bucket` | Resolved from `ins_dataops_backup_bucket`; normally leave the configured value unchanged. |

For example:

```text
s3_folder:                     dump_files
dump_file_name:                DevDump_2026-08-13-13-50.dump
validation_summary_file_name:  DevDump_2026-08-13-13-50.json
restore_summary_file_name:     DevDump_2026-08-13-14-43_restore.json
```

The restore executes in this order:

1. Download the dump from S3 to the Prefect task.
2. Upload it to the Neo4j host over SSH.
3. Stop Neo4j, run `neo4j-admin load --force`, fix data ownership, and restart Neo4j.
4. Connect to Neo4j over Bolt and generate `restore_summary_file_name`.
5. Upload the restore summary to the same S3 folder.
6. Download `validation_summary_file_name` and compare the two summary objects.

The summary contains total node and relationship counts plus counts grouped by node label and relationship type. A successful validated restore logs:

```text
Data asset loading successfully
Finished in state Completed()
```

Immediately after `systemctl start neo4j`, the first Bolt connection can receive `Connection refused` while Neo4j starts. The summary code retries up to three times with a 20-second delay. If a later attempt logs `Connect to the neo4j database successfully` and the flow completes its summary comparison, the temporary first failure is harmless.

## Troubleshooting

### Prefect deployment returns `401 Unauthorized`

The local CLI is not authenticated to the configured Prefect Cloud workspace. Run `prefect cloud login`, then confirm access with:

```bash
prefect variable get ins_dataops_backup_bucket
```

### SSH key is rejected

Confirm that Secrets Manager returns a multiline private key rather than literal `\n` characters. The key must be an unencrypted RSA private key, and its public key must be authorized on the Neo4j host.

### S3 returns `AccessDenied`

Inspect the principal ARN in the error. For Neo4j backup and restore, S3 access belongs to the IAM task role used by the Prefect ECS work pool—not to the Neo4j SSH user and not to the OpenSearch snapshot role.

### Restore reports Neo4j authentication failure

Confirm that the database secret contains `neo4j_user` and `neo4j_password`. The SSH secret's `neo4j_prefect_user` is only for the host connection and must not be used as the Neo4j database username.
