resource "aws_iam_policy" "aws-iam-policy-firehouse-handson-bigdata" {
  name = "aws-iam-policy-firehouse-handson-bigdata"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject"
        ]
        Resource = [
          data.aws_s3_bucket.aws-s3-dados-data-lake.arn,
          "${data.aws_s3_bucket.aws-s3-dados-data-lake.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_policy" "aws-iam-policy-emr-instance-profile" {
  name = "aws-iam-policy-emr-instance-profile"


  policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Resource" : "*",
          "Action" : ["*"]
        }
      ]
    }

  )
}

resource "aws_iam_policy" "aws-iam-policy-emr-service-profile" {
  name = "aws-iam-policy-emr-service-profile"


  policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Resource" : "*",
          "Action" : ["*"]
        }
      ]
    }

  )
}

resource "aws_iam_policy" "aws-iam-policy-step-functions-handson-bigdata" {
  name = "aws-iam-policy-step-functions-handson-bigdata"


  policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Resource" : "*",
          "Action" : ["*"]
        }
      ]
    }

  )
}
