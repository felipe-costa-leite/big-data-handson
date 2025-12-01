resource "aws_iam_role_policy_attachment" "aws-iam-rpa-firehouse-handson-bigdata" {
  role       = aws_iam_role.aws-iam-role-firehouse-handson-bigdata.name
  policy_arn = aws_iam_policy.aws-iam-policy-firehouse-handson-bigdata.arn
}

resource "aws_iam_role_policy_attachment" "aws-iam-role-emr-instance-profile-attachment" {
  role       = aws_iam_role.aws-iam-role-emr-instance-profile.name
  policy_arn = aws_iam_policy.aws-iam-policy-emr-instance-profile.arn
}

resource "aws_iam_role_policy_attachment" "aws-iam-role-emr-service-profile-attachment" {
  role       = aws_iam_role.aws-iam-role-emr-service-profile.name
  policy_arn = aws_iam_policy.aws-iam-policy-emr-service-profile.arn
}

resource "aws_iam_role_policy_attachment" "aws-iam-role-step-functions-handson-bigdata-attachment" {
  role       = aws_iam_role.aws-iam-role-step-functions-handson-bigdata.name
  policy_arn = aws_iam_policy.aws-iam-policy-step-functions-handson-bigdata.arn
}
