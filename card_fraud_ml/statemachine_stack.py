import random
from aws_cdk import (
    Duration,
    Size,
    aws_ec2 as ec2,
    aws_iam as _iam,
    aws_lambda as lambda_,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Stack,
    RemovalPolicy,
    CfnOutput,
)
from constructs import Construct
from datetime import datetime

class CardFraudStateMachineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, stack_params, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        params = stack_params
   
        dataprep_function = lambda_.Function.from_function_name(self, "sagemaker-dep-fn",
                                function_name="CardFraudDataPrep")
              
        sagemaker_execution_role = _iam.Role(self, "sagemaker-execution-role",
            assumed_by=_iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name("AWSStepFunctionsFullAccess"),
                _iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess")
            ],
            inline_policies={
                "s3_access": _iam.PolicyDocument(statements=[
                    _iam.PolicyStatement(
                        effect=_iam.Effect.ALLOW,
                        actions=["s3:*"],
                        resources=["arn:aws:s3:::*/*"]
                    )
                ])},
            role_name="sagemaker-execution-role"
         )
        
        dataprep_task = tasks.LambdaInvoke(self, "InvokedataprepLambda",
            lambda_function=dataprep_function,
            result_path="$.DataPrepResults",
            result_selector={
                "HttpStatusCode.$": "$.SdkHttpMetadata.HttpStatusCode",
                "StatusCode.$": "$.StatusCode"
            },
            state_name="Data Load",
            task_timeout=sfn.Timeout.duration(Duration.minutes(10))
        )

        training_job_task = tasks.SageMakerCreateTrainingJob(self, "CreateTrainingJob",
            algorithm_specification=tasks.AlgorithmSpecification(
                training_image=tasks.DockerImage.from_json_expression(sfn.JsonPath.string_at("$.Model.imageName")),
                training_input_mode=tasks.InputMode.FILE
            ),
            input_data_config=[tasks.Channel(
                channel_name="train",
                data_source=tasks.DataSource(
                    s3_data_source=tasks.S3DataSource(
                        s3_data_type=tasks.S3DataType.S3_PREFIX,
                        s3_location=tasks.S3Location.from_json_expression("$.smTrainPath")
                    )
                )
            ),
            tasks.Channel(
                channel_name="test",
                data_source=tasks.DataSource(
                    s3_data_source=tasks.S3DataSource(
                        s3_data_type=tasks.S3DataType.S3_PREFIX,
                        s3_location=tasks.S3Location.from_json_expression("$.smTestPath")
                    )
                )
            )
            ],
            output_data_config=tasks.OutputDataConfig(
                s3_output_location=tasks.S3Location.from_json_expression("$.smModelOutput")
            ),
            training_job_name=f"cardfraud-{datetime.date(datetime.now())}-{random.randint(1000, 9999)}",
            hyperparameters={
                "feature_dim": "30",
                "predictor_type": "binary_classifier",
                "loss": "hinge_loss",
                "binary_classifier_model_selection_criteria": "precision_at_target_recall",
                "target_recall": "0.9",
                "positive_example_weight_mult": "balanced",
                "epochs": "40",
            },
            role=sagemaker_execution_role,           
            resource_config=tasks.ResourceConfig(
                instance_count=1,
                instance_type=ec2.InstanceType(sfn.JsonPath.string_at("$.smInstanceType")),
                volume_size=Size.gibibytes(20)
            ),
            stopping_condition=tasks.StoppingCondition(
                max_runtime=Duration.hours(2)
            ),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path= "$.TrainJobResults",
            result_selector={ 
                "ModelName.$": "$.TrainingJobName",
                "ModelArtifacts.$": "$.ModelArtifacts.S3ModelArtifacts"
             },
            task_timeout=sfn.Timeout.duration(Duration.minutes(60)),
            state_name="Train Model"

            
        )

        create_model_task = tasks.SageMakerCreateModel(self, "CreateModel",
            model_name=sfn.JsonPath.string_at("$.TrainJobResults.ModelName"),
            primary_container=tasks.ContainerDefinition(
                image=tasks.DockerImage.from_json_expression(sfn.JsonPath.string_at("$.Model.imageName")),
                model_s3_location=tasks.S3Location.from_json_expression("$.TrainJobResults.ModelArtifacts")
            ),
            role=sagemaker_execution_role,
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            result_path="$.CreateModelResults",
            result_selector={
                "HttpStatusCode.$": "$.SdkHttpMetadata.HttpStatusCode",
                "ModelArn.$": "$.ModelArn"
            },
            task_timeout=sfn.Timeout.duration(Duration.minutes(10)),
            state_name="Save Model"

        )

        endpoint_config_task = tasks.SageMakerCreateEndpointConfig(self, "CreateEndpointConfig",
            endpoint_config_name=sfn.JsonPath.string_at("$.TrainJobResults.ModelName"),
            production_variants=[tasks.ProductionVariant(
                initial_instance_count=1,
                instance_type=ec2.InstanceType(sfn.JsonPath.string_at("$.smInstanceType")),
                model_name=sfn.JsonPath.string_at("$.TrainJobResults.ModelName"),
                variant_name="AllTraffic",

            )],
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            result_path="$.CreateEndpointConfigResults",
            result_selector={
                "HttpStatusCode.$": "$.SdkHttpMetadata.HttpStatusCode",
                "EndpointConfigArn.$": "$.EndpointConfigArn"
            },
            task_timeout=sfn.Timeout.duration(Duration.minutes(10)),
            state_name="Create Endpoint Config"
        )

        create_endpoint_task = tasks.SageMakerCreateEndpoint(
            self, "CreateEndpoint",
            endpoint_config_name=sfn.JsonPath.string_at("$.TrainJobResults.ModelName"),
            endpoint_name=params["sagemaker_endpoint_name"],
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            result_path="$.CreateEndpointResults",
            result_selector={
                "HttpStatusCode.$": "$.SdkHttpMetadata.HttpStatusCode",
                "EndpointArn.$": "$.EndpointArn"
            },
            task_timeout=sfn.Timeout.duration(Duration.minutes(10)),
            state_name="Create Endpoint"
        )
        
        state_machine_chain = dataprep_task.next(training_job_task) \
                             .next(create_model_task) \
                             .next(endpoint_config_task) \
                             .next(create_endpoint_task)

        state_machine_role = _iam.Role(self, "CardFraudStateMachineRole",
            assumed_by=_iam.ServicePrincipal("states.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name("AWSStepFunctionsFullAccess"),
                _iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess"),
                _iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ],
            role_name="CardFraudStateMachineRole",
         )
                     
        state_machine = sfn.StateMachine(self, "card-fraud-state-machine",
            definition_body=sfn.DefinitionBody.from_chainable(state_machine_chain),
            state_machine_name=params["state_machine_name"],
            role=state_machine_role,
            timeout=Duration.minutes(60),
            removal_policy=RemovalPolicy.DESTROY    
        )

        CfnOutput(self, "StateMachineArn", 
                  value=state_machine.state_machine_arn,
                  description="The arn of the Statemachine",
                  export_name="StateMachineArn")