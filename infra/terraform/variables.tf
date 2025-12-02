variable "do_token" {
  description = "DigitalOcean personal access token with write scope."
  type        = string
  sensitive   = true
}

variable "region" {
  description = "Default region where droplets will be created."
  type        = string
  default     = "nyc3"
}

variable "default_image" {
  description = "Fallback droplet image slug."
  type        = string
  default     = "ubuntu-22-04-x64"
}

variable "default_size" {
  description = "Fallback droplet size slug."
  type        = string
  default     = "s-2vcpu-4gb"
}

variable "ssh_key_ids" {
  description = "List of SSH key IDs or fingerprints allowed on every droplet."
  type        = list(string)
  default     = []

  validation {
    condition     = length(var.ssh_key_ids) > 0
    error_message = "Provide at least one SSH key ID or fingerprint so droplets can be accessed."
  }
}

variable "service_overrides" {
  description = <<EOT
Optional per-service overrides. Keys must match one of the default services
(core-prod, frontend-prod, analytics-prod). Only provide the attributes that
need to change.
EOT

  type = map(object({
    name        = optional(string)
    environment = optional(string)
    region      = optional(string)
    size        = optional(string)
    image       = optional(string)
    backups     = optional(bool)
    ipv6        = optional(bool)
    monitoring  = optional(bool)
    tags        = optional(list(string))
  }))

  default = {}
}

