resource "aws_iam_role" "aws-iam-role-firehouse-handson-bigdata" {
  name = "aws-iam-role-firehouse-handson-bigdata"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "firehose.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}


resource "aws_iam_role" "aws-iam-role-glue-handson-bigdata" {
  name = "aws-iam-role-glue-handson-bigdata"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}