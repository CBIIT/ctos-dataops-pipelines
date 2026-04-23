#!/bin/bash

# Configuration - Update these values
ROLE_NAME="power-user-opensearch-snapshot"
S3_BUCKET_NAME="integrated-dashboard-opensearch-snapshots"
REGION="us-east-1"

echo "============================================================"
echo "Step 1: Creating S3 bucket for OpenSearch snapshots..."
echo "============================================================"

# 1. Create S3 bucket
echo "Creating S3 bucket: $S3_BUCKET_NAME"
if aws s3 ls "s3://${S3_BUCKET_NAME}" 2>/dev/null; then
  echo "✓ Bucket already exists: $S3_BUCKET_NAME"
else
  if [ "$REGION" = "us-east-1" ]; then
    aws s3api create-bucket \
      --bucket "$S3_BUCKET_NAME" \
      --region "$REGION"
  else
    aws s3api create-bucket \
      --bucket "$S3_BUCKET_NAME" \
      --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION"
  fi
  echo "✓ Bucket created: $S3_BUCKET_NAME"
fi

# 2. Enable versioning
echo "Enabling versioning..."
aws s3api put-bucket-versioning \
  --bucket "$S3_BUCKET_NAME" \
  --versioning-configuration Status=Enabled

# 3. Enable server-side encryption
echo "Enabling server-side encryption..."
aws s3api put-bucket-encryption \
  --bucket "$S3_BUCKET_NAME" \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      },
      "BucketKeyEnabled": true
    }]
  }'

# 4. Add bucket tags
echo "Adding tags..."
aws s3api put-bucket-tagging \
  --bucket "$S3_BUCKET_NAME" \
  --tagging 'TagSet=[
    {Key=Purpose,Value=OpenSearch-Snapshots},
    {Key=ManagedBy,Value=CTOS-DataOps}
  ]'

# 5. Block public access (security best practice)
echo "Blocking public access..."
aws s3api put-public-access-block \
  --bucket "$S3_BUCKET_NAME" \
  --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

echo ""
echo "============================================================"
echo "Step 2: Creating IAM role for OpenSearch..."
echo "============================================================"

# 6. Create trust policy for OpenSearch
cat > /tmp/opensearch-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "es.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# 7. Create the IAM role
echo "Creating IAM role: $ROLE_NAME"
aws iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document file:///tmp/opensearch-trust-policy.json \
  --description "Role for OpenSearch to access S3 snapshots"

# 8. Create S3 permissions policy
cat > /tmp/opensearch-s3-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:ListBucketMultipartUploads",
        "s3:ListBucketVersions"
      ],
      "Resource": "arn:aws:s3:::${S3_BUCKET_NAME}"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": "arn:aws:s3:::${S3_BUCKET_NAME}/*"
    }
  ]
}
EOF

# 9. Create and attach the policy
POLICY_NAME="${ROLE_NAME}-s3-policy"
echo "Creating IAM policy: $POLICY_NAME"
POLICY_ARN=$(aws iam create-policy \
  --policy-name "$POLICY_NAME" \
  --policy-document file:///tmp/opensearch-s3-policy.json \
  --query 'Policy.Arn' \
  --output text)

echo "Attaching policy to role..."
aws iam attach-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-arn "$POLICY_ARN"

# 10. Get the role ARN
ROLE_ARN=$(aws iam get-role \
  --role-name "$ROLE_NAME" \
  --query 'Role.Arn' \
  --output text)

echo ""
echo "==================================================================="
echo "✓ Setup completed successfully!"
echo "==================================================================="
echo "S3 Bucket: $S3_BUCKET_NAME"
echo "S3 Bucket Region: $REGION"
echo "S3 Bucket ARN: arn:aws:s3:::${S3_BUCKET_NAME}"
echo ""
echo "IAM Role Name: $ROLE_NAME"
echo "IAM Role ARN: $ROLE_ARN"
echo ""
echo "Next steps:"
echo "1. Create Prefect variable for bucket: s3_bucket = $S3_BUCKET_NAME"
echo "2. Create Prefect variable for role: aws_role_prefect_variable = $ROLE_NAME"
echo "3. Use these variables in your OpenSearch backup/restore pipelines"
echo "==================================================================="

# Clean up temporary files
rm /tmp/opensearch-trust-policy.json
rm /tmp/opensearch-s3-policy.json
