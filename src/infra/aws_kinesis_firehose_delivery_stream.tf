resource "aws_kinesis_firehose_delivery_stream" "aws-kinesis-firehose-visitas-handson-bigdata" {
  name        = "aws-kinesis-firehose-visitas-handson-bigdata"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn = aws_iam_role.aws-iam-role-firehouse-handson-bigdata.arn
    bucket_arn = data.aws_s3_bucket.aws-s3-dados-data-lake.arn
    custom_time_zone = "America/Sao_Paulo"


    prefix              = "landing/streaming/visitas/"
    error_output_prefix = "landing/streaming/visitas_errors/!{firehose:error-output-type}/"

    buffering_interval = 60
    buffering_size = 5

    compression_format = "GZIP"
  }

  tags = {
    Name    = "aws-kinesis-firehose-visitas-handson-bigdata"
    Project = "handson-bigdata"
    Type    = "streaming"
  }
}