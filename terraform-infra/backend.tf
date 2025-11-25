terraform {
  backend "s3" {
    bucket       = "coretelecoms-terraform-state"
    key          = "dev/coretelecoms/terraform.tfstate"
    use_lockfile = true
    region       = "eu-north-1"
  }
}