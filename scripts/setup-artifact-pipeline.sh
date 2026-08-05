#!/usr/bin/env bash
# Build artifact pipeline: AWS side. Idempotent, safe to re-run.
#
# Creates: artifact bucket, GitHub OIDC provider, a CI publish role, and read
# access for the demo box. No long-lived credentials anywhere.
#
# Run this BEFORE turning publishing on, then in each publishing repo:
#
#     gh variable set ARTIFACT_PUBLISH --body true -R openexch/admin
#     gh variable set ARTIFACT_PUBLISH --body true -R openexch/assets
#     gh variable set ARTIFACT_PUBLISH --body true -R openexch/match
#
# and on the box, in the admin's systemd drop-in:
#
#     ARTIFACT_BUCKET=openexch-build-artifacts-675715936685
#     ARTIFACT_REGION=eu-central-1
#     DEPLOY_CHANNEL=main          # or "release" once the box is production
#
# In the repo rather than in someone's shell history because it is the only
# record of WHY these permissions are shaped the way they are, and because the
# next person to need it will be looking for it here.
set -euo pipefail

ACCOUNT=675715936685
REGION=eu-central-1
BUCKET=openexch-build-artifacts-${ACCOUNT}
CI_ROLE=openexch-ci-artifact-publisher
BOX_ROLE=openexch-ledger-archive-writer
REPOS=(admin-gateway assets match)

say() { printf '\n=== %s\n' "$*"; }

# ---------------------------------------------------------------- bucket
say "bucket ${BUCKET}"
if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "already exists"
else
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION"
  echo "created"
fi

aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws s3api put-bucket-versioning --bucket "$BUCKET" \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption --bucket "$BUCKET" \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "aws:kms"},
      "BucketKeyEnabled": true
    }]
  }'

# 180 days, NOT the 90 the ledger archive uses. An artifact has to outlive every
# bundle that names its buildSha, or that bundle stops being replayable: the
# debug cluster must boot the exact binary that wrote the snapshot. Bundles
# expire at 90 days, so this is that number plus room to notice.
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" \
  --lifecycle-configuration '{
    "Rules": [
      {"ID": "expire-artifacts", "Status": "Enabled", "Filter": {"Prefix": ""},
       "Expiration": {"Days": 180}},
      {"ID": "expire-noncurrent", "Status": "Enabled", "Filter": {"Prefix": ""},
       "NoncurrentVersionExpiration": {"NoncurrentDays": 7}},
      {"ID": "abort-multipart", "Status": "Enabled", "Filter": {"Prefix": ""},
       "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 3}}
    ]
  }'

aws s3api put-bucket-policy --bucket "$BUCKET" --policy "{
  \"Version\": \"2012-10-17\",
  \"Statement\": [{
    \"Sid\": \"DenyInsecureTransport\",
    \"Effect\": \"Deny\",
    \"Principal\": \"*\",
    \"Action\": \"s3:*\",
    \"Resource\": [\"arn:aws:s3:::${BUCKET}\", \"arn:aws:s3:::${BUCKET}/*\"],
    \"Condition\": {\"Bool\": {\"aws:SecureTransport\": \"false\"}}
  }]
}"
echo "bucket configured"

# ------------------------------------------------------------ OIDC provider
say "GitHub OIDC provider"
OIDC_ARN="arn:aws:iam::${ACCOUNT}:oidc-provider/token.actions.githubusercontent.com"
if aws iam get-open-id-connect-provider --open-id-connect-provider-arn "$OIDC_ARN" >/dev/null 2>&1; then
  echo "already exists"
else
  aws iam create-open-id-connect-provider \
    --url https://token.actions.githubusercontent.com \
    --client-id-list sts.amazonaws.com \
    --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
  echo "created"
fi

# --------------------------------------------------------------- CI role
# The subject list is explicit rather than repo:openexch/*. A wildcard would let
# any repo in the org, including one created later for something unrelated,
# write the binaries that get deployed to the box.
#
# Tags are here because the release channel publishes from them. Leaving them out
# is a hole that stays invisible until the first release: the workflow's tag branch
# runs, fails at the credential step, and the releases/ prefix it was supposed to
# write is simply absent. That is what happened on v0.5.0-beta. Narrowed to v* so a
# scratch tag cannot publish.
say "CI role ${CI_ROLE}"
SUBS=$(printf '"repo:openexch/%s:ref:refs/heads/main","repo:openexch/%s:ref:refs/tags/v*",' \
  $(for r in "${REPOS[@]}"; do printf '%s %s ' "$r" "$r"; done) | sed 's/,$//')
TRUST="{
  \"Version\": \"2012-10-17\",
  \"Statement\": [{
    \"Effect\": \"Allow\",
    \"Principal\": {\"Federated\": \"${OIDC_ARN}\"},
    \"Action\": \"sts:AssumeRoleWithWebIdentity\",
    \"Condition\": {
      \"StringEquals\": {\"token.actions.githubusercontent.com:aud\": \"sts.amazonaws.com\"},
      \"StringLike\": {\"token.actions.githubusercontent.com:sub\": [${SUBS}]}
    }
  }]
}"

if aws iam get-role --role-name "$CI_ROLE" >/dev/null 2>&1; then
  aws iam update-assume-role-policy --role-name "$CI_ROLE" --policy-document "$TRUST"
  echo "trust policy updated"
else
  aws iam create-role --role-name "$CI_ROLE" \
    --description "GitHub Actions publishes build artifacts keyed by commit sha" \
    --assume-role-policy-document "$TRUST"
  echo "created"
fi

# Write-only, and no DeleteObject: an artifact is immutable once published,
# because a bundle's buildSha is a promise that this exact binary still exists.
# Removal is the lifecycle rule's job, same split as the ledger archive.
aws iam put-role-policy --role-name "$CI_ROLE" --policy-name artifact-publish \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {\"Effect\": \"Allow\", \"Action\": [\"s3:PutObject\", \"s3:GetObject\"],
       \"Resource\": \"arn:aws:s3:::${BUCKET}/*\"},
      {\"Effect\": \"Allow\", \"Action\": [\"s3:ListBucket\"],
       \"Resource\": \"arn:aws:s3:::${BUCKET}\"}
    ]
  }"
echo "publish policy attached"

# ------------------------------------------------------------- box read
say "box read access on ${BOX_ROLE}"
aws iam put-role-policy --role-name "$BOX_ROLE" --policy-name artifact-read \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {\"Effect\": \"Allow\", \"Action\": [\"s3:GetObject\"],
       \"Resource\": \"arn:aws:s3:::${BUCKET}/*\"},
      {\"Effect\": \"Allow\", \"Action\": [\"s3:ListBucket\"],
       \"Resource\": \"arn:aws:s3:::${BUCKET}\"}
    ]
  }"
echo "read policy attached"

say "done"
echo "bucket:   s3://${BUCKET}"
echo "ci role:  arn:aws:iam::${ACCOUNT}:role/${CI_ROLE}"
echo "box role: ${BOX_ROLE} (+artifact-read)"
