terraform {
  required_version = ">= 1.5.0"

  backend "s3" {
    bucket         = "aws-s3-dados-data-lake"
    key            = "terraform/state/handson-bigdata.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "aws-dynamodb-terraform-lock"
  }
}

provider "aws" {
  region = "us-east-1"
}
