#!/usr/bin/env bash
# The Cloud Run Button runs this on a press that finds no service yet, after it pushes IMAGE_URL and before it deploys,
# with GOOGLE_CLOUD_PROJECT, GOOGLE_CLOUD_REGION, K_SERVICE and the HS_SETUP_PLIVO_AUTH_ID its form asks for. A failure
# stops the button. A run skips what an earlier run made and never replaces a secret's first version, which HS reads.
set +x -euo pipefail
: "${HS_SETUP_PLIVO_AUTH_ID:?the Auth ID of the Plivo account that registers first}"
p=$GOOGLE_CLOUD_PROJECT r=$GOOGLE_CLOUD_REGION s=$K_SERVICE
sa=$s@$p.iam.gserviceaccount.com conn=$p:$r:$s
export CLOUDSDK_CORE_PROJECT=$p CLOUDSDK_CORE_DISABLE_PROMPTS=1
has() { gcloud "$@" >/dev/null 2>&1; }
secret() {
  has secrets versions describe 1 --secret="$1" && return
  has secrets describe "$1" || gcloud secrets create "$1"
  printf %s "$2" | gcloud secrets versions add "$1" --data-file=-
}
hex() { python3 -c "import secrets; print(secrets.token_hex($1))"; }
op=$(hex 32)
key=$(hex 32)
dbpw=$(hex 24)

gcloud services enable cloudresourcemanager.googleapis.com compute.googleapis.com iam.googleapis.com \
  redis.googleapis.com secretmanager.googleapis.com sqladmin.googleapis.com
has iam service-accounts describe "$sa" || gcloud iam service-accounts create "$s"

has redis instances describe "$s" --region="$r" || gcloud redis instances create "$s" --region="$r" --size=1 \
  --tier=basic --redis-version=redis_7_2 --redis-config=maxmemory-policy=noeviction --enable-auth
redis=$(gcloud redis instances describe "$s" --region="$r" --format='value(state,host)')
read -r ready host <<<"$redis"
auth=$(gcloud redis instances get-auth-string "$s" --region="$r" --format='value(authString)')
[ "$ready" = READY ] && [ -n "$host" ] && [ -n "$auth" ] ||
  { echo "precreate.sh: Redis $s is not ready: press the button again later" >&2; exit 1; }
secret "$s-redis-url" "redis://:$auth@$host:6379/0"

for role in cloudsql.client secretmanager.secretAccessor; do
  gcloud projects add-iam-policy-binding "$p" --member="serviceAccount:$sa" --role="roles/$role" --condition=None \
    --format=none
done
secret "$s-operator-key" "$op"
secret "$s-secret-key" "$key"

has sql instances describe "$s" || gcloud sql instances create "$s" --database-version=POSTGRES_16 \
  --edition=enterprise --tier=db-f1-micro --region="$r" --storage-size=10 --database-flags=max_connections=50
has sql databases describe hs --instance="$s" || gcloud sql databases create hs --instance="$s"
secret "$s-database-url" "postgresql://hs:$dbpw@/hs?host=/cloudsql/$conn"
url=$(gcloud secrets versions access 1 --secret="$s-database-url")
pw=${url#postgresql://hs:}
pw=${pw%%@*}
# A flag file keeps the password out of the process list; mktemp makes it readable by this user alone.
flags=$(mktemp)
trap 'rm -f "$flags"' EXIT
printf -- '--password: %s\n' "$pw" >"$flags"
has sql users describe hs --instance="$s" || gcloud sql users create hs --instance="$s" --flags-file="$flags"

number=$(gcloud projects describe "$p" --format='value(projectNumber)')
secrets=HS_OPERATOR_KEY=$s-operator-key:1,HS_SECRET_KEY=$s-secret-key:1
secrets+=,DATABASE_URL=$s-database-url:1,REDIS_URL=$s-redis-url:1
env=PUBLIC_BASE_URL=https://$s-$number.$r.run.app,HS_SETUP_PLIVO_AUTH_ID=$HS_SETUP_PLIVO_AUTH_ID
# While a deploy overlaps two revisions, their 2 x (12 + 4 + 2) connections and a migration's 1 fit max_connections=50.
env+=,METRICS_PORT=0,DB_POOL_SIZE=12,DB_MAX_OVERFLOW=4
gcloud run deploy "$s" --image="$IMAGE_URL" --region="$r" --service-account="$sa" --port=6004 --cpu=1 --memory=1Gi \
  --no-cpu-throttling --min-instances=1 --max-instances=1 --concurrency=1000 --timeout=3600 --network=default \
  --set-cloudsql-instances="$conn" --allow-unauthenticated --set-secrets="$secrets" --set-env-vars="$env" \
  --command=/app/migrate-and-serve
