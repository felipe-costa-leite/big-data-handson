variable "data_lake_bucket_name" {
  description = "Nome do bucket do data lake já existente"
  type        = string
  default     = "aws-s3-dados-data-lake"
}

variable "region" {
  type        = string
  default     = "us-east-1"
  description = "Região AWS do handson"
}

variable "emr_subnet_id" {
  type        = string
  description = "Subnet onde o cluster EMR será criado"
  default     = "subnet-0ccc01e45099d8120"
}

variable "emr_log_bucket" {
  type        = string
  default     = "aws-s3-dados-data-lake"
  description = "Bucket onde ficam os logs do EMR"
}

variable "emr_log_prefix" {
  type        = string
  default     = "logs/emr/"
  description = "Prefixo de logs do EMR dentro do bucket"
}

variable "emr_release_label" {
  type        = string
  default     = "emr-6.15.0"
  description = "Versão do EMR"
}

variable "emr_master_instance_type" {
  type        = string
  default     = "m5.large"
  description = "Instance type do nó master (single-node cluster)"
}

variable "emr_pipeline_script_s3" {
  type        = string
  default     = "s3://aws-s3-dados-data-lake/artifacts/code/bronze/main.py"
  description = "Caminho do script Spark no S3"
}
