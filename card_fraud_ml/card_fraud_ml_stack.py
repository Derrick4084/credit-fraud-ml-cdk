import random
from aws_cdk import (
    Duration,
    Size,
    Aws,
    aws_ec2 as ec2,
    aws_iam as _iam,
    aws_logs as logs,
    aws_s3 as _s3, aws_s3_notifications,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_, aws_lambda_event_sources,
    aws_kinesis as _kinesis,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Stack,
    RemovalPolicy,
)
from constructs import Construct
from datetime import datetime

class CardFraudMlStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        random_number = random.randint(1000, 2000)   
        # create s3 bucket
        raw_data_bucket = _s3.Bucket(self, "raw-data-bucket",
            bucket_name=f"raw-data-{random_number}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        kinesis_stream = _kinesis.Stream(self, "credit-card-fraud-stream",
            stream_name="credit-card-fraud-stream",
            retention_period=Duration.hours(24),
            stream_mode=_kinesis.StreamMode.ON_DEMAND,
            removal_policy=RemovalPolicy.DESTROY           
        )

        results_dynamo_db = dynamodb.Table(self, "card-fraud-table",
            table_name="creditcard-fraud",
            partition_key=dynamodb.Attribute(name="id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )

        bucket_dynamo_db = dynamodb.Table(self, "bucket-table",
            table_name="bucket-info",
            partition_key=dynamodb.Attribute(name="bucketname", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )
       
        lambda_role = _iam.Role(self, "LambdaRole",
            assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name="LambdaRole",
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"),
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSageMakerFullAccess")
            ],
            inline_policies={
                "s3_access": _iam.PolicyDocument(statements=[
                    _iam.PolicyStatement(
                        effect=_iam.Effect.ALLOW,
                        actions=["s3:*"],
                        resources=["arn:aws:s3:::*/*"]
                    )
                ]),
                "logs_access": _iam.PolicyDocument(statements=[
                    _iam.PolicyStatement(
                        effect=_iam.Effect.ALLOW,
                        actions=["logs:PutLogEvents",
                                 "logs:CreateLogGroup",
                                 "logs:CreateLogStream"],
                        resources=["arn:aws:logs:*:*:*"]
                    )
                ])
            })
        
        sagemaker_lambda_layer = lambda_.LayerVersion(self, "sagemaker-layer]",
            removal_policy=RemovalPolicy.DESTROY,
            code=lambda_.Code.from_asset("lambda_layers/sagemaker.zip"),
            layer_version_name="sagemaker-layer",
        )

        dataprep_lambda_log_group = logs.LogGroup(
            self, "lambda-logs",
            retention=logs.RetentionDays.ONE_DAY,
            removal_policy = RemovalPolicy.DESTROY,
            log_group_name=f"/aws/lambda/dataprep-{random_number}"
        )

        dataprep_lambda_log_group.add_to_resource_policy(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                principals=[_iam.ServicePrincipal("lambda.amazonaws.com")],
                actions=["logs:CreateLogStream", 
                         "logs:PutLogEvents",
                         ],
                resources=[f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/lambda/dataprep-{random_number}:*"],
            )
        )

        dataprep_lambda = lambda_.Function(self, "dataprep-lambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="prep-data.lambda_handler",
            code=lambda_.Code.from_asset("./card_fraud_ml/lambda/dataprep"),
            role=lambda_role,
            function_name=f"data-retrieval-{random_number}",
            timeout=Duration.seconds(60),
            layers=[sagemaker_lambda_layer],
            log_group=dataprep_lambda_log_group,
            memory_size=2048,
        )
              
        startstate_lambda_role = _iam.Role(self, "StartStatePutEventsRole",
            assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name="StartStatePutEventsRole",
            path="/service-role/",
            managed_policies=[_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                              _iam.ManagedPolicy.from_aws_managed_policy_name("AWSStepFunctionsFullAccess"),
                              _iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess"),
                            ],
            inline_policies={
                "s3_access": _iam.PolicyDocument(statements=[
                    _iam.PolicyStatement(
                        effect=_iam.Effect.ALLOW,
                        actions=["s3:*"],
                        resources=["arn:aws:s3:::*/*"]
                    )
                ]),
                "dynamodb_access": _iam.PolicyDocument(statements=[
                    _iam.PolicyStatement(
                        effect=_iam.Effect.ALLOW,
                        actions=["dynamodb:*"],
                        resources=["*"]
                    )
                ]),}
        )
               
        startstate_lambda_log_group = logs.LogGroup(
            self, "startstate-logs",
            retention=logs.RetentionDays.ONE_DAY,
            removal_policy = RemovalPolicy.DESTROY,
            log_group_name=f"/aws/lambda/startstate-{random_number}"
        )

        startstate_lambda_log_group.add_to_resource_policy(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                principals=[_iam.ServicePrincipal("lambda.amazonaws.com")],
                actions=["logs:CreateLogStream", 
                         "logs:PutLogEvents",
                         ],
                resources=[f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/lambda/startstate-{random_number}:*"],
            )
        )

        startstate_lambda = lambda_.Function(self, "startstate-lambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="start-state.lambda_handler",
            code=lambda_.Code.from_asset("./card_fraud_ml/lambda/statestart"),
            role=startstate_lambda_role,
            function_name=f"start-state-{random_number}",
            timeout=Duration.seconds(60),
            layers=[sagemaker_lambda_layer],
            log_group=startstate_lambda_log_group,
            memory_size=256,
            environment={
                "STATE_MACHINE_ARN": f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:credit-card-fraud-state-machine",
                "DYNAMODB_TABLENAME": bucket_dynamo_db.table_name,
                "BUCKET_NAME": raw_data_bucket.bucket_name
            }
        )
        
        dataprep_notification = aws_s3_notifications.LambdaDestination(startstate_lambda)

        raw_data_bucket.add_event_notification(_s3.EventType.OBJECT_CREATED_COMPLETE_MULTIPART_UPLOAD, dataprep_notification,
            _s3.NotificationKeyFilter(suffix=".csv")
        )
        raw_data_bucket.add_event_notification(_s3.EventType.OBJECT_CREATED_PUT, dataprep_notification,
            _s3.NotificationKeyFilter(suffix=".csv")
        )

        sagemaker_endpoint_name = "credit-card-fraud-endpoint"
           
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
            lambda_function=dataprep_lambda,
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
            endpoint_name=sagemaker_endpoint_name,
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

        state_machine_role = _iam.Role(self, "state-machine-role",
            assumed_by=_iam.ServicePrincipal("states.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name("AWSStepFunctionsFullAccess"),
                _iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess"),
                _iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ],
            role_name=f"state-machine-role-{random_number}",
         )
        
        state_machine = sfn.StateMachine(self, "credit-card-fraud-state-machine",
            definition_body=sfn.DefinitionBody.from_chainable(state_machine_chain),
            state_machine_name="credit-card-fraud-state-machine",
            role=state_machine_role,
            timeout=Duration.minutes(60),
            removal_policy=RemovalPolicy.DESTROY    
        )

        consumer_lambda_role = _iam.Role(self, "ConsumerLambdaRole",
            assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name="ConsumerLambdaRole",
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole")
            ],
            inline_policies={
                "sagemaker_access": _iam.PolicyDocument(statements=[
                    _iam.PolicyStatement(
                        effect=_iam.Effect.ALLOW,
                        actions=["sagemaker:InvokeEndpoint"],
                        resources=["*"]
                    )
                ]),
                "dynamodb_access": _iam.PolicyDocument(statements=[
                    _iam.PolicyStatement(
                        effect=_iam.Effect.ALLOW,
                        actions=["dynamodb:*"],
                        resources=["*"]
                    )
                ]),
                "logs_access": _iam.PolicyDocument(statements=[
                    _iam.PolicyStatement(
                        effect=_iam.Effect.ALLOW,
                        actions=["logs:PutLogEvents",
                                 "logs:CreateLogGroup",
                                 "logs:CreateLogStream"],
                        resources=["arn:aws:logs:*:*:*"]
                    )
                ])
            })
        
        consumer_lambda_log_group = logs.LogGroup(
            self, "consumer-lambda-logs",
            retention=logs.RetentionDays.ONE_DAY,
            removal_policy = RemovalPolicy.DESTROY,
            log_group_name=f"/aws/lambda/consumer-{random_number}"
        )

        consumer_lambda_log_group.add_to_resource_policy(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                principals=[_iam.ServicePrincipal("lambda.amazonaws.com")],
                actions=["logs:CreateLogStream", 
                         "logs:PutLogEvents",
                         ],
                resources=[f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/lambda/consumer-{random_number}:*"],
            )
        )

        consumer_lambda_event_source = aws_lambda_event_sources.KinesisEventSource(
            kinesis_stream,
            starting_position=lambda_.StartingPosition.LATEST,
            batch_size=100,
            parallelization_factor=1,
            max_batching_window=Duration.seconds(60),
            retry_attempts=3
        )

        consumer_lambda = lambda_.Function(self, "consumer-lambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="cardfraud-consumer.lambda_handler",
            code=lambda_.Code.from_asset("./card_fraud_ml/lambda/consumer"),
            role=consumer_lambda_role,
            function_name=f"cardfraud-consumer-{random_number}",
            timeout=Duration.seconds(60),
            log_group=consumer_lambda_log_group,
            memory_size=512,
            environment={
                "SM_ENDPOINT_NAME": sagemaker_endpoint_name,
                "SM_RESULTS_TABLE_NAME": results_dynamo_db.table_name,
                "BUCKET_TABLE_NAME": bucket_dynamo_db.table_name,
                "RAW_BUCKET_NAME": raw_data_bucket.bucket_name                         
            }
        )
        consumer_lambda.add_event_source(consumer_lambda_event_source)

        generator_lambda_role = _iam.Role(self, "GeneratorLambdaRole",
            assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name="GeneratorLambdaRole",
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole")
            ],
            inline_policies={
                "kinesis_access": _iam.PolicyDocument(statements=[
                    _iam.PolicyStatement(
                        effect=_iam.Effect.ALLOW,
                        actions=["kinesis:PutRecord"],
                        resources=[kinesis_stream.stream_arn]
                    )
                ]),
                "dynamodb_access": _iam.PolicyDocument(statements=[
                    _iam.PolicyStatement(
                        effect=_iam.Effect.ALLOW,
                        actions=["dynamodb:GetItem"],
                        resources=[bucket_dynamo_db.table_arn]
                    )
                ]),
                "s3_access": _iam.PolicyDocument(statements=[
                    _iam.PolicyStatement(
                        effect=_iam.Effect.ALLOW,
                        actions=["s3:*"],
                        resources=["*"]
                    )
                ])
            })
        
        generator_lambda_log_group = logs.LogGroup(
            self, "generator-lambda-logs",
            retention=logs.RetentionDays.ONE_DAY,
            removal_policy = RemovalPolicy.DESTROY,
            log_group_name=f"/aws/lambda/generator-{random_number}"
        )

        generator_lambda_log_group.add_to_resource_policy(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                principals=[_iam.ServicePrincipal("lambda.amazonaws.com")],
                actions=["logs:CreateLogStream", 
                         "logs:PutLogEvents",
                         ],
                resources=[f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/lambda/generator-{random_number}:*"],
            )
        )
        
        generator_lambda = lambda_.Function(self, "generator-lambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="record-generator.lambda_handler",
            code=lambda_.Code.from_asset("./card_fraud_ml/lambda/generator"),
            role=generator_lambda_role,
            function_name=f"cardfraud-generator-{random_number}",
            timeout=Duration.minutes(3),
            log_group=generator_lambda_log_group,
            memory_size=1024,
            environment={
                "STREAM_NAME": kinesis_stream.stream_name,
                "RAW_BUCKET_NAME": raw_data_bucket.bucket_name,
                "BUCKET_TABLE_NAME": bucket_dynamo_db.table_name              
            }
        )
