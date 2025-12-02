locals {
  default_tags = ["messaging-app"]

  default_services = {
    backend-prod = {
      name        = "msg-backend-prod"
      environment = "prod"
      region      = null
      size        = "s-2vcpu-4gb"
      image       = null
      backups     = true
      ipv6        = true
      monitoring  = true
      tags        = ["backend", "api"]
    }

    frontend-prod = {
      name        = "msg-frontend-prod"
      environment = "prod"
      region      = null
      size        = "s-1vcpu-2gb"
      image       = null
      backups     = false
      ipv6        = true
      monitoring  = true
      tags        = ["frontend", "web"]
    }

    metabase-prod = {
      name        = "msg-metabase-prod"
      environment = "prod"
      region      = null
      size        = "s-2vcpu-4gb"
      image       = null
      backups     = true
      ipv6        = true
      monitoring  = true
      tags        = ["analytics", "metabase"]
    }

    spark-prod = {
      name        = "msg-spark-prod"
      environment = "prod"
      region      = null
      size        = "s-4vcpu-8gb"
      image       = null
      backups     = false
      ipv6        = true
      monitoring  = true
      tags        = ["analytics", "spark"]
    }

    temporal-prod = {
      name        = "msg-temporal-prod"
      environment = "prod"
      region      = null
      size        = "s-2vcpu-4gb"
      image       = null
      backups     = true
      ipv6        = true
      monitoring  = true
      tags        = ["temporal", "workers"]
    }

    grafana-prod = {
      name        = "msg-grafana-prod"
      environment = "prod"
      region      = null
      size        = "s-1vcpu-2gb"
      image       = null
      backups     = true
      ipv6        = true
      monitoring  = true
      tags        = ["grafana", "monitoring"]
    }

    grafana-staging = {
      name        = "msg-grafana-staging"
      environment = "staging"
      region      = null
      size        = "s-1vcpu-1gb"
      image       = null
      backups     = false
      ipv6        = true
      monitoring  = true
      tags        = ["grafana", "monitoring", "staging"]
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

