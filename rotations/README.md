# Composer-Airflow Rotation Tooling

## About

Rotate Airflow to a new composer environment and decommission the old environment.

1. Save DAG inventory locally and take snapshot of old Composer Environment
2. Pause all dags instantly in old Composer environment (ensuring no overlap in dag runs)
3. Load snapshot into new Composer environment

## Use Case

Prefer in-place update of composer env as far as possible. This is supported for composer env minor version update, using the google beta-provider. Apart from major version updates, there could be more scenarios where in-place update is not possible and env rotation is unavoidable. Examples:
- Changing any network configuration like tags, vpc network, ip ranges, etc.
- Encryption key

Updating following are safe as these doesn’t result in env replacement:
- Labels
- software config like
  - image version
  - pypi packages,
  - env variables, etc
- Workload config like scheduler, trigerrer, etc


## Usage

The script takes three arguments
1. (p) project_id where composer is deployed
2. (c) old composer environment name
3. (l) old composer environment location
1. (C) new composer environment name
2. (L) new composer environment location
3. (g) gcs location for snapshot

Sample Usage:

```bash
./rotate-composer.sh \
  -p "cy-artifacts" \
  -c "composer-2-small" \
  -l "us-central1" \
  -C "composer-2-small-new" \
  -L "us-central1" \
  -g "gs://cy-sandbox/composer-snapshots/" \
  -d "gs://us-central1-composer-2-smal-56609903-bucket/dags"
```