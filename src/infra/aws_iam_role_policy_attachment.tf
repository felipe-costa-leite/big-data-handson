resource "aws_iam_role_policy_attachment" "aws-iam-rpa-firehouse-handson-bigdata" {
  role       = aws_iam_role.aws-iam-role-firehouse-handson-bigdata.name
  policy_arn = aws_iam_policy.aws-iam-policy-firehouse-handson-bigdata.arn
}

resource "aws_iam_role_policy_attachment" "aws-iam-rpa-glue-handson-bigdata" {
  role       = aws_iam_role.aws-iam-role-glue-handson-bigdata.name
  policy_arn = aws_iam_policy.aws-iam-policy-glue-handson-bigdata.arn
}
