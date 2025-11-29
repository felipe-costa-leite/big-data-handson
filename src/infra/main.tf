# main.tf
#
# Estrutura dos arquivos deste módulo:
# - providers.tf: bloco terraform + provider "aws"
# - variables.tf: variáveis de entrada
# - data.tf: data sources (aws_s3_bucket, etc.)
# - aws_iam_role.tf: roles IAM (firehose, glue)
# - aws_iam_policy.tf: policies IAM (firehose, glue)
# - aws_iam_role_policy_attachment.tf: vínculos role-policy
# - aws_kinesis_firehose_delivery_stream.tf: definição do Firehose
# - aws_glue_job.tf: jobs do AWS Glue (ingestion, dq, store)
# - tf.auto.tfvars: valores das variáveis para este ambiente


output "firehose_name" {
  value = aws_kinesis_firehose_delivery_stream.aws-kinesis-firehose-visitas-handson-bigdata.name
}

output "glue_job_ingestion_name" {
  value = aws_glue_job.aws-glue-job-ingestion-pedidos-handson-bigdata.name
}
