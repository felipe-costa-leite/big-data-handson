resource "aws_glue_job" "aws-glue-job-ingestion-pedidos-handson-bigdata" {
  name     = "aws-glue-job-ingestion-pedidos-handson-bigdata"
  role_arn = aws_iam_role.aws-iam-role-glue-handson-bigdata.arn

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://aws-s3-dados-data-lake/artifacts/code/ingestion/pedidos/main.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-glue-datacatalog"          = "true"
    "--TempDir"                          = "s3://${var.data_lake_bucket_name}/tmp/glue/ingestion-pedidos/"
  }

  tags = {
    Name    = "aws-glue-job-ingestion-pedidos-handson-bigdata"
    Project = "handson-bigdata"
    Type    = "ingestion"
  }
}

resource "aws_glue_job" "aws-glue-job-dataquality-handson-bigdata" {
  name     = "aws-glue-job-dataquality-handson-bigdata"
  role_arn = aws_iam_role.aws-iam-role-glue-handson-bigdata.arn

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://aws-s3-dados-data-lake/artifacts/code/dataquality/main.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-glue-datacatalog"          = "true"
    "--TempDir"                          = "s3://${var.data_lake_bucket_name}/tmp/glue/dataquality/"
  }

  tags = {
    Name    = "aws-glue-job-dataquality-handson-bigdata"
    Project = "handson-bigdata"
    Type    = "dataquality"
  }
}

resource "aws_glue_job" "aws-glue-job-store-handson-bigdata" {
  name     = "aws-glue-job-store-handson-bigdata"
  role_arn = aws_iam_role.aws-iam-role-glue-handson-bigdata.arn

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://aws-s3-dados-data-lake/artifacts/code/store/main.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-glue-datacatalog"          = "true"
    "--TempDir"                          = "s3://${var.data_lake_bucket_name}/tmp/glue/store/"
  }

  tags = {
    Name    = "aws-glue-job-store-handson-bigdata"
    Project = "handson-bigdata"
    Type    = "store"
  }
}
