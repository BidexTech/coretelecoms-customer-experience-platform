terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  # alias = "personal_acoount"
  region = "eu-north-1"
  profile = "personal"
}