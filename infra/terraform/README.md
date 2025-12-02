# DigitalOcean infrastructure

This Terraform project provisions the production droplets that host each
messaging-app service inside DigitalOcean. The configuration intentionally
creates one droplet per critical workload to keep blast radius low and to
support the monitoring requirement for a dedicated staging Grafana instance.

## Prerequisites

- Terraform `>= 1.6`.
- A DigitalOcean personal access token with **write** scope.
- At least one SSH key already uploaded to DigitalOcean (record the ID or
  fingerprint).
- Optional but recommended: a remote backend (for example an encrypted DO
  Space or another state storage service). At the moment the state file stays
  local to the runner.

## Services

| Key              | Droplet name           | Purpose                    | Default size   |
|------------------|------------------------|----------------------------|----------------|
| `backend-prod`   | `msg-backend-prod`     | FastAPI backend            | `s-2vcpu-4gb`  |
| `frontend-prod`  | `msg-frontend-prod`    | Vite/React frontend        | `s-1vcpu-2gb`  |
| `metabase-prod`  | `msg-metabase-prod`    | Metabase analytics         | `s-2vcpu-4gb`  |
| `spark-prod`     | `msg-spark-prod`       | Spark / ETL workers        | `s-4vcpu-8gb`  |
| `temporal-prod`  | `msg-temporal-prod`    | Temporal server            | `s-2vcpu-4gb`  |
| `grafana-prod`   | `msg-grafana-prod`     | Production Grafana         | `s-1vcpu-2gb`  |
| `grafana-staging`| `msg-grafana-staging`  | Isolated staging Grafana   | `s-1vcpu-1gb`  |

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

