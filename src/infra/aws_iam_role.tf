resource "aws_iam_role" "aws-iam-role-firehouse-handson-bigdata" {
  name = "aws-iam-role-firehouse-handson-bigdata"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "firehose.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
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

resource "aws_iam_role" "aws-iam-role-emr-instance-profile" {
  name = "aws-iam-role-emr-instance-profile"
  assume_role_policy = jsonencode({
    "Statement" : [
      {
        "Action" : "sts:AssumeRole",
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "ec2.amazonaws.com"
        }
      }
    ],
    "Version" : "2012-10-17"
  })
  description = "Emr Instance Profile"
  path        = "/"
}

resource "aws_iam_role" "aws-iam-role-emr-service-profile" {
  name = "aws-iam-role-emr-instance-profile"
  assume_role_policy = jsonencode({
    "Statement" : [
      {
        "Action" : "sts:AssumeRole",
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "elasticmapreduce.amazonaws.com"
        }
      }
    ],
    "Version" : "2012-10-17"
  })
  description = "EMR Service"
  path        = "/"
}

resource "aws_iam_role" "aws-iam-role-step-functions-handson-bigdata" {
  name = "aws-iam-role-step-functions-handson-bigdata"
  assume_role_policy = jsonencode({
    "Statement" : [
      {
        "Action" : "sts:AssumeRole",
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "states.amazonaws.com"
        }
      }
    ],
    "Version" : "2012-10-17"
  })
}


