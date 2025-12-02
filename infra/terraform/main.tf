locals {
  default_tags = ["messaging-app"]

  default_services = {
    core-prod = {
      name        = "msg-core-prod"
      environment = "prod"
      region      = null
      size        = "s-4vcpu-8gb"
      image       = null
      backups     = true
      ipv6        = true
      monitoring  = true
      tags        = ["backend", "temporal", "api"]
    }

    frontend-prod = {
      name        = "msg-frontend-prod"
      environment = "prod"
      region      = null
      size        = "s-2vcpu-4gb"
      image       = null
      backups     = false
      ipv6        = true
      monitoring  = true
      tags        = ["frontend", "web"]
    }

    analytics-prod = {
      name        = "msg-analytics-prod"
      environment = "prod"
      region      = null
      size        = "s-8vcpu-16gb"
      image       = null
      backups     = true
      ipv6        = true
      monitoring  = true
      tags        = ["analytics", "metabase", "spark", "grafana"]
    }
  }

  services = {
    for key, defaults in local.default_services :
    key => merge(defaults, try(var.service_overrides[key], {}))
  }
}

resource "digitalocean_droplet" "service" {
  for_each = local.services

  name   = coalesce(each.value.name, "msg-${replace(each.key, "_", "-")}")
  region = coalesce(each.value.region, var.region)
  size   = coalesce(each.value.size, var.default_size)
  image  = coalesce(each.value.image, var.default_image)

  backups    = coalesce(each.value.backups, false)
  ipv6       = coalesce(each.value.ipv6, true)
  monitoring = coalesce(each.value.monitoring, true)

  tags = distinct(concat(
    local.default_tags,
    each.value.environment != null ? [each.value.environment] : [],
    try(each.value.tags, [])
  ))

  ssh_keys = var.ssh_key_ids
}

