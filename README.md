# CTOS data operations pipelines

## Prefect Flows

Before proceeding, if you're the one who will deploy or run these Prefect flows,
then submit a request to be added to the FNL Prefect developers mailing list. Being
on the mailing list will let you log in to Prefect Cloud and deploy/run flows.

## AWS and network prerequisites

The OpenSearch backup flow uses three IAM roles with separate responsibilities:

1. **Prefect ECS task role**

   This is the task role attached to the ECS task started by the Prefect work
   pool—not the ECS task execution role. For INS, runs have used a principal
   such as:

   ```text
   arn:aws:iam::986019062625:role/power-user-prefect-ecs-task-ccdi-ccdi
   ```

   The Python flow initially runs as this role. It reads the AWS secret that
   contains `es_host` and calls `sts:AssumeRole` on the OpenSearch operations
   role. Therefore, it needs `secretsmanager:GetSecretValue` and permission to
   assume the configured operations role. It does **not** write OpenSearch
   snapshot files directly to S3.

2. **OpenSearch operations role**

   The INS Prefect configuration currently identifies this role as:

   ```text
   arn:aws:iam::082604052123:role/power-user-prefect-operations
   ```

   The flow assumes this role and uses its temporary credentials to sign HTTP
   requests to the OpenSearch domain. This role registers the snapshot
   repository, creates snapshots, checks repositories, and starts restores. It
   needs the applicable `es:ESHttpGet`, `es:ESHttpPut`, `es:ESHttpPost`, and
   `es:ESHttpDelete` permissions. It also needs `iam:PassRole` for the exact
   snapshot role supplied during repository registration. Its trust policy must
   allow the Prefect ECS task role to assume it.

3. **OpenSearch snapshot role**

   The Prefect variable `ins_opensearch_snapshot_role` should contain this
   role's full IAM role ARN—not a policy ARN. The flow passes the ARN to
   OpenSearch as the repository's `role_arn`. AWS OpenSearch Service then
   assumes this role to read and write snapshot data in S3. Its trust policy
   must allow the service principal `es.amazonaws.com` to assume it. It needs
   `s3:ListBucket` on the snapshot bucket and the necessary `s3:GetObject`,
   `s3:PutObject`, and `s3:DeleteObject` permissions for snapshot objects. If
   the bucket uses a customer-managed KMS key, it also needs the corresponding
   KMS permissions. A cross-account bucket or KMS key policy must allow this
   role as well.

The role chain is:

```text
Prefect ECS task role
    -- sts:AssumeRole --> OpenSearch operations role
    -- signed OpenSearch request + iam:PassRole --> OpenSearch Service
    -- assumes snapshot role --> S3 snapshot bucket
```

For Neo4j backup and restore, the Prefect ECS task role must additionally be
able to:

- call `s3:ListBucket` on the backup bucket;
- call `s3:GetObject` and `s3:PutObject` for objects under the selected S3 prefix;
- call `s3:PutObjectAcl` if required for the `bucket-owner-full-control` ACL used by this repository;
- use the applicable KMS key if the secrets or bucket use a customer-managed key.

The ECS task must have network access to:

- SSH on the Neo4j host;
- Neo4j Bolt on port `7687`;
- HTTPS on port `443` to the selected OpenSearch VPC endpoint;
- AWS Secrets Manager and S3.

If the Prefect ECS tasks and an OpenSearch domain are in different VPCs, the
VPC peering or Transit Gateway routes, network ACLs, and security groups must
allow traffic in both directions. The OpenSearch security group should allow
inbound TCP `443` from the Prefect ECS task security group.

The SSH user must have passwordless `sudo` access for the commands used by the flows, including stopping and starting Neo4j and running `neo4j-admin`. Its public key must be installed in the user's `authorized_keys` file on the Neo4j host.

### Configuration

Three configuration files tailor `ctos-dataops-pipelines` Prefect flows for INS's needs:

- [`config/ins-prefect.yaml`](./config/ins-prefect.yaml)
  - Name the project in `name`, up top.
  - Set `data_model_repo_url` to the GitHub repository whose branches and tags should populate the Neo4j backup's `data_model_version` dropdown.
  - `opensearch_snapshot_role_prefect_variable` is the **name of a Prefect
    variable**, currently `ins_opensearch_snapshot_role`. The value stored in
    that Prefect variable must be the snapshot role's full IAM role ARN.
  - `opensearch_operations_role` is the full IAM ARN of the role that the
    Prefect ECS task assumes before making requests to the OpenSearch domain.
    This is not the snapshot role.
  - Specify the INS branch `ins-pipelines` of `ctos-dataops-pipelines` in the `pull` section.
  - Specify `name`, `parameters`, and `work_pool` for the following deployments:
    - `ins-neo4j-backup`
    - `ins-neo4j-restore`
    - `ins-opensearch-backup`
    - `ins-opensearch-restore`
    - `ins-opensearch-promote`
  - Expressions such as
    `{{ prefect.variables.ins_opensearch_backup_bucket }}` are resolved from
    Prefect variables defined in the Prefect Cloud workspace during deployment. The resolved values
    become prefilled run parameters and can still be overridden for an
    individual run.
  - `environment: dev` is only the initial selection. The generated run form
    allows the operator to select `dev` or `qa`.
  - OpenSearch `snapshot_name` is intentionally blank because backup needs a
    new, unique name and restore needs the exact name of an existing snapshot.
  - OpenSearch `indices` defaults to an empty array. An empty array means all
    non-hidden indices; a populated array limits the backup or restore to the
    listed index names.
- [`config/prefect_drop_down_config.yaml`](./config/prefect_drop_down_config.yaml)
  - Specify parameters for the `dev` and `qa` environments.
  - The values of these parameters are the names of Prefect variables.
  - We don't plan to use Neo4j backup/restore in the QA environment, but the YAML needs an entry in addition to `dev`.
- [`config/ins_promote_drop_down_config.yaml`](./config/ins_promote_drop_down_config.yaml)
  - Defines the `stage` and `prod` choices shown by the
    `ins-opensearch-promote` environment dropdown.
  - Maps each choice to the Prefect variable containing that environment's AWS
    Secrets Manager secret ARN.

The `ins-opensearch-promote` deployment uses a thin Prefect wrapper around the
existing universal OpenSearch restore implementation. The wrapper reads the
dropdown choices and their Prefect-variable names from
`config/ins_promote_drop_down_config.yaml`; it does not duplicate the restore
logic.

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
        "neo4j_ip": "123.456.7.890",
        "neo4j_user": "my_username",
        ... // Other INS secrets
      },
      ... // Other projects' secrets
    }
    ```

    Then `ins_secret_name_dev` should be set to `"super_secret_ins_stuff"`.
- `ins_secret_name_qa`
  - Same purpose as `ins_secret_name_dev`, but its value identifies the AWS
    Secrets Manager secret for the INS QA environment.
- `ins_secret_name_stage`
  - Set this Prefect variable to
    `arn:aws:secretsmanager:us-east-1:697201234594:secret:ccdi-ins-stage-credentials-cdk-Vg9jAQ`.
  - The promote flow resolves this variable when `stage` is selected.
- `ins_secret_name_prod`
  - Set this Prefect variable to
    `arn:aws:secretsmanager:us-east-1:697201234594:secret:ccdi-ins-prod-credentials-cdk-0VnG6b`.
  - The promote flow resolves this variable when `prod` is selected.
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
- `ins_opensearch_backup_bucket`
  - The short S3 bucket name - not an ARN - used by the OpenSearch snapshot
    repository.
- `ins_opensearch_repo`
  - The logical repository name registered in OpenSearch, such as `ins`.
- `ins_opensearch_snapshot_role`
  - The full IAM **role ARN** that OpenSearch Service assumes for S3 snapshot
    access, for example `arn:aws:iam::<account-id>:role/<snapshot-role>`.
  - Do not store a policy ARN such as `arn:aws:iam::<account-id>:policy/...`.

### Deployment

To deploy a flow to Prefect Cloud, install Prefect in your local environment (eg: `pip install prefect`)
and run the command

```bash
prefect deploy --prefect-file config/ins-prefect.yaml
```

Select the flow you want to deploy, and choose "No" for all the options that follow.

Deploy the shared stage/production promotion flow from the same Prefect YAML:

```bash
prefect deploy \
  --prefect-file config/ins-prefect.yaml \
  --name ins-opensearch-promote
```

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
      enter the exact existing `snapshot_name`, and optionally add index names
      to the `indices` array. Leave the array empty to restore all non-hidden
      indices from the snapshot.
    - For `ins-opensearch-promote`, choose `stage` or `prod`, enter the exact
      existing `snapshot_name`, and optionally add index names to the `indices`
      array. Promotion runs on the production Prefect work pool.

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
