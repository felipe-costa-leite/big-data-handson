resource "aws_sfn_state_machine" "aws-sfn-emr-handson-bigdata" {
  name     = "aws-step-functions-handson-bigdata"
  role_arn = aws_iam_role.aws-iam-role-step-functions-handson-bigdata.arn

  definition = templatefile(
    "${path.module}/files/stepfunctions/create_cluster.json",
    {
      emr_release_label        = var.emr_release_label
      emr_log_bucket           = var.emr_log_bucket
      emr_log_prefix           = var.emr_log_prefix
      emr_subnet_id            = var.emr_subnet_id
      emr_master_instance_type = var.emr_master_instance_type
      emr_pipeline_script_s3   = var.emr_pipeline_script_s3
      emr_service_role_name    = aws_iam_role.aws-iam-role-emr-service-profile.name
      emr_jobflow_role_name    = aws_iam_role.aws-iam-role-emr-instance-profile.name
      region    = var.region
    }
  )
}
