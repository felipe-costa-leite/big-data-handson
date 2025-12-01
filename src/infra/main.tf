# main.tf
#
# Estrutura dos arquivos deste módulo:
# - providers.tf: bloco terraform + provider "aws"
# - variables.tf: variáveis de entrada
# - data.tf: data sources (aws_s3_bucket, etc.)
# - aws_iam_role.tf: roles IAM (firehose)
# - aws_iam_policy.tf: policies IAM (firehose, glue)
# - aws_iam_role_policy_attachment.tf: vínculos role-policy
# - aws_kinesis_firehose_delivery_stream.tf: definição do Firehose
# - tf.auto.tfvars: valores das variáveis para este ambiente


output "firehose_name" {
  value = aws_kinesis_firehose_delivery_stream.aws-kinesis-firehose-eventos-handson-bigdata.name
}
