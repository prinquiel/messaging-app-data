output "droplet_public_ipv4" {
  description = "IPv4 address per service droplet."
  value = {
    for key, droplet in digitalocean_droplet.service :
    key => droplet.ipv4_address
  }
}

output "droplet_public_ipv6" {
  description = "IPv6 address per service droplet (if enabled)."
  value = {
    for key, droplet in digitalocean_droplet.service :
    key => droplet.ipv6_address
  }
}

output "droplet_metadata" {
  description = "Useful identifiers for each droplet."
  value = {
    for key, droplet in digitalocean_droplet.service :
    key => {
      id  = droplet.id
      urn = droplet.urn
      region = droplet.region
      name = droplet.name
      tags = droplet.tags
    }
  }
}

