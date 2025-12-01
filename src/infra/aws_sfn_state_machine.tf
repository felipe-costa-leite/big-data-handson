resource "aws_sfn_state_machine" "aws-sfn-emr-handson-bigdata" {
  name     = "aws-step-functions-handson-bigdata"
  role_arn = aws_iam_role.aws-iam-role-step-functions-handson-bigdata.arn

  definition = jsonencode(
    {
    Comment = "Pipeline EMR: cria cluster, roda step Spark e termina cluster"
    StartAt = "CreateEmrCluster"
    States = {
      CreateEmrCluster = {
        Type     = "Task"
        Resource = "arn:aws:states:::elasticmapreduce:createCluster.sync"
        Parameters = {
          Name        = "emr-handson-bigdata"
          ReleaseLabel = var.emr_release_label

          Applications = [
            { Name = "Hadoop" },
            { Name = "Ganglia" },
            { Name = "Hive" },
            { Name = "Spark" }
          ]

          LogUri            = "s3://${var.emr_log_bucket}/${var.emr_log_prefix}"
          VisibleToAllUsers = true

          ServiceRole = aws_iam_role.aws-iam-role-emr-service-profile.name
          JobFlowRole = aws_iam_role.aws-iam-role-emr-instance-profile.name

          Instances = {
            Ec2SubnetId               = var.emr_subnet_id
            KeepJobFlowAliveWhenNoSteps = true
            TerminationProtected      = false

            InstanceGroups = [
              {
                Name          = "Master nodes"
                InstanceRole  = "MASTER"
                InstanceType  = var.emr_master_instance_type
                InstanceCount = 1
              }
            ]
          }
        }
        ResultPath = "$.CreateClusterResult"
        Next       = "AddSparkStepPipeline"
      }

      AddSparkStepPipeline = {
        Type     = "Task"
        Resource = "arn:aws:states:::elasticmapreduce:addStep.sync"
        Parameters = {
          ClusterId.$ = "$.CreateClusterResult.ClusterId"
          Step = {
            Name           = "spark-pipeline-bronze-silver-gold"
            ActionOnFailure = "CANCEL_AND_WAIT"
            HadoopJarStep = {
              Jar  = "command-runner.jar"
              Args = [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "--packages", "io.delta:delta-core_2.12:2.4.0",

                var.emr_pipeline_script_s3
              ]
            }
          }
        }
        ResultPath = "$.SparkStepResult"
        Next       = "TerminateCluster"
      }

      TerminateCluster = {
        Type     = "Task"
        Resource = "arn:aws:states:::elasticmapreduce:terminateCluster"
        Parameters = {
          ClusterId.$ = "$.CreateClusterResult.ClusterId"
        }
        End = true
      }
    }
  })
}
