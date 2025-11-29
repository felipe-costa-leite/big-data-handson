data "aws_s3_bucket" "aws-s3-dados-data-lake" {
  bucket = var.data_lake_bucket_name
}
