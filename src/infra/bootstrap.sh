#!/bin/bash
set -e

sudo yum update -y
sudo yum install -y unzip curl awscli

TERRAFORM_VERSION="1.9.5"
curl -Lo terraform.zip "https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip"
unzip terraform.zip
sudo mv terraform /usr/local/bin/terraform
terraform -version

mkdir -p /opt/terraform/handson-bigdata
cd /opt/terraform/handson-bigdata


aws s3 sync s3://aws-s3-dados-data-lake/artifacts/infra/ .

terraform init -input=false

terraform apply -auto-approve
