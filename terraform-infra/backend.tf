terraform {
  backend "s3" {
    bucket       = "coretelecom-terraform-state"
    key          = "dev/coretelecoms/terraform.tfstate"
    use_lockfile = true
    region       = "eu-north-1"
    profile      = "personal"
  }
}
