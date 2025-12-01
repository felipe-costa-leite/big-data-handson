resource "aws_iam_instance_profile" "aws-iam-role-emr-instance-profile-instance-profile" {
  name = "aws-iam-role-emr-instance-profile"
  role = "${aws_iam_role.aws-iam-role-emr-instance-profile.name}"
}
