# CTOS data operations pipelines

## Prefect Flows

Before proceeding, if you're the one who will deploy or run these Prefect flows,
then submit a request to be added to the FNL Prefect developers mailing list. Being
on the mailing list will let you log in to Prefect Cloud and deploy/run flows.

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

### Configuration

Two configuration files tailor `ctos-dataops-pipelines` Prefect flows for INS's needs:

- [`config/ins-prefect.yaml`](./config/ins-prefect.yaml)
  - Name the project in `name`, up top.
  - Set `data_model_repo_url` to the GitHub repository whose branches and tags should populate the Neo4j backup's `data_model_version` dropdown.
  - Specify the INS branch `ins-pipelines` of `ctos-dataops-pipelines` in the `pull` section.
  - Specify `name`, `parameters`, and `work_pool` for the following deployments:
    - `ins-neo4j-backup`
    - `ins-neo4j-restore`
    - `ins-opensearch-backup`
    - `ins-opensearch-restore`
- [`config/prefect_drop_down_config.yaml`](./config/prefect_drop_down_config.yaml)
  - Specify parameters for the `dev` and `qa` environments.
  - The values of these parameters are the names of Prefect variables.
  - We don't plan to use Neo4j backup/restore in the QA environment, but the YAML needs an entry in addition to `dev`.

### Prefect Cloud Workspace Variables

Define the following Workspace Variables (Settings -> Variables) in Prefect Cloud:

- `ins_secret_name_dev`
  - The value of this variable should be the key of the key-value pair in AWS Secrets Manager for the INS Dev
    environment secrets.
  - Eg: suppose AWS Secrets Manager is set up like so:

    ```json
    {
      ..., // Other projects' secrets
      "super_secret_ins_stuff": {
        "neo4j_host": "123.456.7.890",
        "neo4j_user": "my_username",
        ... // Other INS secrets
      },
      ... // Other projects' secrets
    }
    ```

    Then `ins_secret_name_dev` should be set to `"super_secret_ins_stuff"`.
- `ins_dataops_backup_bucket`
  - The value of this variable should be the short name (i.e. not ARN) of the S3 bucket in which to store Neo4j dumps.
- `ins_neo4j_ssh_secret_name`
  - The value of this variable should be the key of the key-value pair in AWS Secrets Manager for the INS Dev
    environment Neo4j SSH secrets.
  - It could be the same value as for `ins_secret_name_dev`, depending on where you chose to store the Neo4j SSH secrets.
  - Eg: suppose AWS Secrets Manager is set up like so:

    ```json
    {
      ..., // Other projects' secrets
      "super_secret_ins_ssh_stuff": {
        "neo4j_prefect_user": "my_username", // Name of user who can SSH into the Neo4j instance
        "neo4j_key": "...", // SSH key for my_username in the Neo4j instance
        ... // Other INS secrets
      },
      ... // Other projects' secrets
    }
    ```

    Then `ins_neo4j_ssh_secret_name` should be set to `"super_secret_ins_ssh_stuff"`.
  - The SSH key needs to be formatted precisely. Paste the SSH key into the secret by using the Plaintext editor.
    - Eg:

      ```json
      {
        "neo4j_prefect_user": "my_username", // Name of user who can SSH into the Neo4j instance
        "neo4j_key": "-----BEGIN OPENSSH PRIVATE KEY-----\nABC123\n...\nXYZ890\n-----END OPENSSH PRIVATE KEY-----\n"
      }
      ```

    - Pay **close** attention to the newline characters!

### Deployment

To deploy a flow to Prefect Cloud, install Prefect in your local environment (eg: `pip install prefect`)
and run the command

```bash
prefect deploy --prefect-file config/ins-prefect.yaml
```

Select the flow you want to deploy, and choose "No" for all the options that follow.

### Execution

To run a Prefect flow:

1. Log in to Prefect Cloud, and make sure that you're in the `ccdi-workspace` workspace.
2. Search "Deployments" for the flow you're trying to run. Eg:
    - `ins-opensearch-loader`
    - `ins-metadata-loading-dev`

    and click on the deployment.
3. Click on "Run", and choose "Quick run" or "Custom run" - whichever floats your boat.
    - Custom runs are nice, because you can name the run something meaningful.
4. Select or fill in each parameter for the run.
    - For `ins-neo4j-backup`, make sure to choose a value for `data_model_version`.
      The remaining default parameters should be good enough.
      Even the summary and dump filenames are prefilled with the time!
    - For `ins-neo4j-restore`, copy the summary and dump filenames from the `ins-neo4j-backup` run whose
      dump you want to restore. Then paste those filenames into the following parameters, but don't
      overwrite the file extensions of course:
        - `dump_file_name`
        - `validation_summary_file_name`
          - I recommend adding the text `"validation_summary"` to distinguish this file from the restore summary JSON.
        - `restore_summary_file_name`
          - I recommend adding the text `"restore_summary"` to distinguish this file from the validation summary JSON.
    - For `ins-opensearch-backup`, choose `dev` or `qa`, enter a unique
      `snapshot_name`, and optionally add index names to the `indices` array.
      Leave the array empty to snapshot all non-hidden indices.
    - For `ins-opensearch-restore`, choose the target `dev` or `qa` environment,
      enter the exact existing `snapshot_name`, and optionally enter
      comma-separated index names in `indices`. Leave `indices` blank to restore
      all non-hidden indices from the snapshot.

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
