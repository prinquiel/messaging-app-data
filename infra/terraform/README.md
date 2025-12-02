# DigitalOcean infrastructure

This Terraform project provisions the production droplets that host each
messaging-app service inside DigitalOcean. The configuration now groups the app
into three droplets to simplify operations while keeping clear separation of
concerns (core app, frontend, analytics/observability).

## Prerequisites

- Terraform `>= 1.6`.
- A DigitalOcean personal access token with **write** scope.
- At least one SSH key already uploaded to DigitalOcean (record the ID or
  fingerprint).
- Optional but recommended: a remote backend (for example an encrypted DO
  Space or another state storage service). At the moment the state file stays
  local to the runner.

## Services

| Key               | Droplet name           | Purpose                                             | Default size   |
|-------------------|------------------------|-----------------------------------------------------|----------------|
| `core-prod`       | `msg-core-prod`        | FastAPI backend + Temporal server                   | `s-4vcpu-8gb`  |
| `frontend-prod`   | `msg-frontend-prod`    | Vite/React frontend                                 | `s-2vcpu-4gb`  |
| `analytics-prod`  | `msg-analytics-prod`   | Metabase, Spark/ETL jobs, Grafana observability     | `s-8vcpu-16gb` |

Use `service_overrides` in `terraform.tfvars` to adjust sizing, region, or tags
without editing the defaults.

## Usage

```bash
cp terraform.tfvars.example terraform.tfvars
# Fill in the token, SSH key IDs, and any overrides
terraform init
terraform plan
terraform apply
```

When running inside CI (see the Terraform workflow), export the DigitalOcean
token as `DIGITALOCEAN_ACCESS_TOKEN` or provide it via a secure secret and read
it into `TF_VAR_do_token`.

## Notes

- Droplets share the same SSH keys list. Add new keys to DigitalOcean and then
  append their IDs/fingerprints to `ssh_key_ids`.
- Consider attaching managed databases, load balancers, or firewalls as the next
  iteration; this module only creates compute resources.
- Add a backend block to `versions.tf` (for example, Terraform Cloud or S3) if
  you need remote state locking.

