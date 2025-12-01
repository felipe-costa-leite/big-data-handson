resource "aws_sfn_state_machine" "aws-sfn-emr-handson-bigdata" {
  name     = "aws-step-functions-handson-bigdata"
  role_arn = aws_iam_role.aws-iam-role-step-functions-handson-bigdata.arn

  definition = templatefile(
    "${path.module}/files/step_functions_handson_bigdata.json",
    {
      emr_cluster_name        = "emr-handson-bigdata"
      emr_release_label       = var.emr_release_label
      emr_log_bucket          = var.emr_log_bucket
      emr_log_prefix          = var.emr_log_prefix
      emr_subnet_id           = var.emr_subnet_id
      emr_master_instance_type = var.emr_master_instance_type
      emr_service_role_name   = aws_iam_role.aws-iam-role-emr-service-profile.name
      emr_jobflow_role_name   = aws_iam_role.aws-iam-role-emr-instance-profile.name
      bronze_script_path      = "s3://aws-s3-dados-data-lake/artifacts/code/bronze/main.py"
      silver_script_path      = "s3://aws-s3-dados-data-lake/artifacts/code/silver/main.py"
      gold_script_path        = "s3://aws-s3-dados-data-lake/artifacts/code/gold/main.py"
    }
  )
}
