variable "aws_region" {
  description = "Região AWS"
  type        = string
  default     = "us-east-1"
}

variable "data_lake_bucket_name" {
  description = "Nome do bucket do data lake já existente"
  type        = string
  default     = "aws-s3-dados-data-lake"
}
