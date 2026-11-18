#!/bin/sh
# Bootstrap for the Amazon CloudWatch Logs SDK fixture (Moto with IAM auth enabled).
# POSIX sh + curl (curlimages/curl). Idempotent: a re-run on the same stack reuses the stored keys,
# tolerates existing users/groups/streams, does not duplicate seed events and rewrites /bootstrap/env.
#
# Creates:
#   - IAM user grow-sdk-admin    (policy Allow * on *)                 -> AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
#   - IAM user grow-sdk-readonly (logs read/query/tail actions only)   -> GROW_TEST_READONLY_*
#   - log group grow-bootstrap/seed (retention 7 days, tags grow:fixture=seed, team=sdk)
#     with stream seed-stream holding 3 seed events (marker grow-seed-event, ingested at bootstrap time)
# Writes KEY=value lines to $BOOTSTRAP_DIR/env (0644). Keys are Moto-generated fakes, valid only inside
# this disposable emulator; they are never printed.
set -eu

URL=${CWL_URL:-http://aws-cloudwatch-logs:5000}
REGION=${CWL_REGION:-us-east-1}
DIR=${BOOTSTRAP_DIR:-/bootstrap}
GROUP=grow-bootstrap/seed
STREAM=seed-stream
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$DIR"
umask 077

log() { echo "bootstrap: $*" >&2; }
die() { log "ERROR: $*"; exit 1; }

# curl with a few retries on connection-level failures (Moto can reset the first connection).
xcurl() {
  n=0
  while :; do
    if curl "$@"; then return 0; fi
    rc=$?
    n=$((n + 1))
    case $rc in 7|52|56) [ $n -lt 10 ] || return $rc; sleep 1 ;; *) return $rc ;; esac
  done
}

# iam <access-key> <secret-key> <form args...>  -> prints HTTP status, body in $TMP/body
iam() {
  _ak=$1 _sk=$2; shift 2
  xcurl -sS -o "$TMP/body" -w '%{http_code}' \
    --aws-sigv4 "aws:amz:us-east-1:iam" --user "$_ak:$_sk" \
    --data-urlencode "Version=2010-05-08" "$@" "$URL/"
}

# cwl <Operation> <json>  -> prints HTTP status, body in $TMP/body (signed with the admin key)
cwl() {
  xcurl -sS -o "$TMP/body" -w '%{http_code}' \
    --aws-sigv4 "aws:amz:$REGION:logs" --user "$ADMIN_AK:$ADMIN_SK" \
    -H "X-Amz-Target: Logs_20140328.$1" -H 'Content-Type: application/x-amz-json-1.1' \
    --data-binary "$2" "$URL/"
}

xml_val() { grep -o "<$1>[^<]*</$1>" "$TMP/body" | head -n 1 | sed 's/<[^>]*>//g'; }
json_str() { tr -d '\n' <"$TMP/body" | grep -o "\"$1\": *\"[^\"]*\"" | head -n 1 | sed 's/.*: *"\(.*\)"/\1/'; }
exists_or_die() { # <label> <code>
  case $2 in
    200) return 0 ;;
    400) grep -q ResourceAlreadyExistsException "$TMP/body" && return 1 || die "$1: HTTP 400 $(head -c 200 "$TMP/body")" ;;
    *) die "$1: HTTP $2" ;;
  esac
}

ADMIN_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}'
RO_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["logs:Describe*","logs:Get*","logs:FilterLogEvents","logs:StartQuery","logs:StopQuery","logs:StartLiveTail","logs:ListTagsForResource","logs:ListTagsLogGroup"],"Resource":"*"}]}'

# --- 1. admin user (uses the 3 unauthenticated actions; must be the first AWS calls) ---------
if [ -s "$DIR/admin.env" ]; then
  . "$DIR/admin.env"
  log "reusing admin key"
else
  code=$(iam bootstrap bootstrap --data-urlencode Action=CreateUser --data-urlencode UserName=grow-sdk-admin)
  case $code in
    200) ;;
    409) die "grow-sdk-admin exists but its key is lost; recreate the stack (docker compose down -v)" ;;
    *) die "CreateUser grow-sdk-admin: HTTP $code" ;;
  esac
  code=$(iam bootstrap bootstrap --data-urlencode Action=PutUserPolicy --data-urlencode UserName=grow-sdk-admin \
    --data-urlencode PolicyName=grow-sdk-admin --data-urlencode "PolicyDocument=$ADMIN_POLICY")
  [ "$code" = 200 ] || die "PutUserPolicy grow-sdk-admin: HTTP $code"
  code=$(iam bootstrap bootstrap --data-urlencode Action=CreateAccessKey --data-urlencode UserName=grow-sdk-admin)
  [ "$code" = 200 ] || die "CreateAccessKey grow-sdk-admin: HTTP $code"
  ADMIN_AK=$(xml_val AccessKeyId) ADMIN_SK=$(xml_val SecretAccessKey)
  [ -n "$ADMIN_AK" ] && [ -n "$ADMIN_SK" ] || die "CreateAccessKey: could not parse key"
  printf 'ADMIN_AK=%s\nADMIN_SK=%s\n' "$ADMIN_AK" "$ADMIN_SK" >"$DIR/admin.env"
  log "created admin user + key"
fi

# --- 2. admin key must work (signed DescribeLogGroups) ---------------------------------------
code=$(cwl DescribeLogGroups '{"limit":1}')
[ "$code" = 200 ] || die "signed DescribeLogGroups with admin key: HTTP $code (is IAM auth on?)"

# --- 3. read-only user -----------------------------------------------------------------------
code=$(iam "$ADMIN_AK" "$ADMIN_SK" --data-urlencode Action=CreateUser --data-urlencode UserName=grow-sdk-readonly)
case $code in 200|409) ;; *) die "CreateUser grow-sdk-readonly: HTTP $code" ;; esac
code=$(iam "$ADMIN_AK" "$ADMIN_SK" --data-urlencode Action=PutUserPolicy --data-urlencode UserName=grow-sdk-readonly \
  --data-urlencode PolicyName=grow-sdk-readonly --data-urlencode "PolicyDocument=$RO_POLICY")
[ "$code" = 200 ] || die "PutUserPolicy grow-sdk-readonly: HTTP $code"
if [ -s "$DIR/readonly.env" ]; then
  . "$DIR/readonly.env"
else
  code=$(iam "$ADMIN_AK" "$ADMIN_SK" --data-urlencode Action=CreateAccessKey --data-urlencode UserName=grow-sdk-readonly)
  [ "$code" = 200 ] || die "CreateAccessKey grow-sdk-readonly: HTTP $code"
  RO_AK=$(xml_val AccessKeyId) RO_SK=$(xml_val SecretAccessKey)
  [ -n "$RO_AK" ] && [ -n "$RO_SK" ] || die "CreateAccessKey readonly: could not parse key"
  printf 'RO_AK=%s\nRO_SK=%s\n' "$RO_AK" "$RO_SK" >"$DIR/readonly.env"
fi

# --- 4. seed log group + stream + events (read-only fixtures; tests create their own groups) --
if exists_or_die "CreateLogGroup $GROUP" "$(cwl CreateLogGroup "{\"logGroupName\":\"$GROUP\",\"tags\":{\"grow:fixture\":\"seed\",\"team\":\"sdk\"}}")"; then
  log "created log group $GROUP"
fi
code=$(cwl PutRetentionPolicy "{\"logGroupName\":\"$GROUP\",\"retentionInDays\":7}")
[ "$code" = 200 ] || die "PutRetentionPolicy $GROUP: HTTP $code"
if exists_or_die "CreateLogStream $STREAM" "$(cwl CreateLogStream "{\"logGroupName\":\"$GROUP\",\"logStreamName\":\"$STREAM\"}")"; then
  # Events must be within the last 14 days (Moto + AWS reject older ones) and in chronological order.
  NOW_MS=$(( $(date +%s) * 1000 ))
  EV=""
  for i in 1 2 3; do
    ts=$((NOW_MS - (4 - i) * 1000))
    EV="$EV{\"timestamp\":$ts,\"message\":\"{\\\"level\\\":\\\"info\\\",\\\"msg\\\":\\\"grow-seed-event $i\\\",\\\"seq\\\":$i}\"},"
  done
  code=$(cwl PutLogEvents "{\"logGroupName\":\"$GROUP\",\"logStreamName\":\"$STREAM\",\"logEvents\":[${EV%,}]}")
  [ "$code" = 200 ] || die "PutLogEvents seed: HTTP $code $(head -c 200 "$TMP/body")"
  grep -q rejectedLogEventsInfo "$TMP/body" && die "PutLogEvents seed: events rejected"
  log "seeded 3 events into $GROUP/$STREAM"
fi
code=$(cwl DescribeLogGroups "{\"logGroupNamePrefix\":\"$GROUP\"}")
[ "$code" = 200 ] || die "DescribeLogGroups $GROUP: HTTP $code"
GROUP_ARN=$(json_str logGroupArn)
[ -n "$GROUP_ARN" ] || die "DescribeLogGroups: no logGroupArn"

# --- 5. env file (atomic) --------------------------------------------------------------------
cat >"$DIR/env.tmp" <<EOF
AWS_REGION=$REGION
AWS_DEFAULT_REGION=$REGION
AWS_ACCESS_KEY_ID=$ADMIN_AK
AWS_SECRET_ACCESS_KEY=$ADMIN_SK
AWS_ENDPOINT_URL_CLOUDWATCH_LOGS=$URL
AWS_EC2_METADATA_DISABLED=true
GROW_AWS_CLOUDWATCH_LOGS_URL=$URL
GROW_AWS_CLOUDWATCH_LOGS_REGION=$REGION
CLOUDWATCH_LOG_GROUP=$GROUP
CLOUDWATCH_LOG_STREAM=$STREAM
GROW_TEST_ACCOUNT_ID=123456789012
GROW_TEST_READONLY_ACCESS_KEY_ID=$RO_AK
GROW_TEST_READONLY_SECRET_ACCESS_KEY=$RO_SK
GROW_TEST_SEED_LOG_GROUP=$GROUP
GROW_TEST_SEED_LOG_GROUP_ARN=$GROUP_ARN
GROW_TEST_SEED_LOG_STREAM=$STREAM
GROW_TEST_SEED_MARKER=grow-seed-event
GROW_TEST_SEED_EVENT_COUNT=3
GROW_TEST_EMULATOR=moto
EOF
chmod 0644 "$DIR/env.tmp"
mv -f "$DIR/env.tmp" "$DIR/env"
log "wrote $DIR/env ($(wc -l <"$DIR/env") lines)"
