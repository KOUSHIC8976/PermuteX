# infrastructure/terraform/providers.tf

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }


  backend "s3" {
    bucket         = "permutex-tf-state-lock-12345" # Must be globally unique
    key            = "permutex/prod/terraform.tfstate"
    region         = "us-east-2"
    dynamodb_table = "permutex-tf-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "PermuteX"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}