import random
from aws_cdk import (
    Duration,
    Aws,
    aws_iam as _iam,
    aws_logs as logs,
    aws_s3 as _s3, aws_s3_notifications,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_, aws_lambda_event_sources,
    aws_kinesis as _kinesis,
    Stack,
    RemovalPolicy,
)
from constructs import Construct


class CardFraudLambdaStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, stack_params, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        params = stack_params


        raw_data_bucket = _s3.Bucket(self, "raw-data-bucket",
            bucket_name=params["raw_bucket_name"],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        kinesis_stream = _kinesis.Stream(self, "credit-card-fraud-stream",
            stream_name=params["kinesis_stream_name"],
            retention_period=Duration.hours(24),
            stream_mode=_kinesis.StreamMode.ON_DEMAND,
            removal_policy=RemovalPolicy.DESTROY           
        )

        results_dynamo_db = dynamodb.Table(self, "card-fraud-table",
            table_name=params["dynamodb_fraud_table"],
            partition_key=dynamodb.Attribute(name="id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )

        bucket_dynamo_db = dynamodb.Table(self, "bucket-table",
            table_name=params["dynamodb_bucket_info"],
            partition_key=dynamodb.Attribute(name="bucketname", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )
       
        
        sagemaker_lambda_layer = lambda_.LayerVersion(self, "sagemaker-layer]",
            removal_policy=RemovalPolicy.DESTROY,
            code=lambda_.Code.from_asset("lambda_layers/sagemaker.zip"),
            layer_version_name="sagemaker-layer",
        )
        numpy_lambda_layer = lambda_.LayerVersion(self, "numpy-layer]",
            removal_policy=RemovalPolicy.DESTROY,
            code=lambda_.Code.from_asset("lambda_layers/numpy.zip"),
            layer_version_name="numpy-layer",
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
            log_group_name=f"/aws/lambda/card-fraud-startstate"
        )

        startstate_lambda_log_group.add_to_resource_policy(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                principals=[_iam.ServicePrincipal("lambda.amazonaws.com")],
                actions=["logs:CreateLogStream", 
                         "logs:PutLogEvents",
                         ],
                resources=[f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/lambda/card-fraud-startstate:*"],
            )
        )

        startstate_lambda = lambda_.Function(self, "startstate-lambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="start-state.lambda_handler",
            code=lambda_.Code.from_asset("./card_fraud_ml/lambda/statestart"),
            role=startstate_lambda_role,
            function_name="CardFraudStartStateMachine",
            timeout=Duration.seconds(60),
            layers=[sagemaker_lambda_layer],
            log_group=startstate_lambda_log_group,
            memory_size=256,
            environment={
                "STATE_MACHINE_ARN": f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:{params['state_machine_name']}",
                "DYNAMODB_TABLENAME": params['dynamodb_bucket_info'],
                "BUCKET_NAME": params['raw_bucket_name']
            }
        )




        dataprep_role = _iam.Role(self, "DataPrepLambdaRole",
            assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name="DataPrepLambdaRole",
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"),
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSageMakerFullAccess"),
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AWSStepFunctionsFullAccess")
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
            }
        )          
        dataprep_lambda_log_group = logs.LogGroup(
            self, "lambda-logs",
            retention=logs.RetentionDays.ONE_DAY,
            removal_policy = RemovalPolicy.DESTROY,
            log_group_name=f"/aws/lambda/cardfraud-dataprep"
        )
        dataprep_lambda_log_group.add_to_resource_policy(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                principals=[_iam.ServicePrincipal("lambda.amazonaws.com")],
                actions=["logs:CreateLogStream", 
                         "logs:PutLogEvents",
                         ],
                resources=[f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/lambda/cardfraud-dataprep:*"],
            )
        )
        dataprep_lambda = lambda_.Function(self, "CardFraudDataPrepLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="prep-data.lambda_handler",
            code=lambda_.Code.from_asset("./card_fraud_ml/lambda/dataprep"),
            role=dataprep_role,
            function_name="CardFraudDataPrep",
            timeout=Duration.seconds(60),
            layers=[sagemaker_lambda_layer],
            log_group=dataprep_lambda_log_group,
            memory_size=2048,
            environment={
                "DYNAMODB_TABLENAME": params["dynamodb_bucket_info"],
                "STATE_MACHINE_ARN": f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:{params['state_machine_name']}"
            }
        )

        start_state_notification = aws_s3_notifications.LambdaDestination(startstate_lambda)   
        raw_data_bucket.add_event_notification(_s3.EventType.OBJECT_CREATED_COMPLETE_MULTIPART_UPLOAD, start_state_notification,
            _s3.NotificationKeyFilter(suffix=".csv")
        )
        raw_data_bucket.add_event_notification(_s3.EventType.OBJECT_CREATED_PUT, start_state_notification,
            _s3.NotificationKeyFilter(suffix=".csv")
        )



        consumer_lambda_event_source = aws_lambda_event_sources.KinesisEventSource(
            kinesis_stream,
            starting_position=lambda_.StartingPosition.LATEST,
            batch_size=100,
            parallelization_factor=1,
            max_batching_window=Duration.seconds(60),
            retry_attempts=3
        )
        consumer_lambda_role = _iam.Role(self, "CardFraudRecConsumerLambdaRole",
            assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name="CardFraudRecConsumerLambdaRole",
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
            }
        )       
        consumer_lambda_log_group = logs.LogGroup(
            self, "consumer-lambda-logs",
            retention=logs.RetentionDays.ONE_DAY,
            removal_policy = RemovalPolicy.DESTROY,
            log_group_name=f"/aws/lambda/cardfraud-rec-consumer"
        )
        consumer_lambda_log_group.add_to_resource_policy(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                principals=[_iam.ServicePrincipal("lambda.amazonaws.com")],
                actions=["logs:CreateLogStream", 
                         "logs:PutLogEvents",
                         ],
                resources=[f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/lambda/cardfraud-rec-consumer:*"],
            )
        )
        consumer_lambda = lambda_.Function(self, "CardFraudKinesisConsumer",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="cardfraud-consumer.lambda_handler",
            code=lambda_.Code.from_asset("./card_fraud_ml/lambda/consumer"),
            role=consumer_lambda_role,
            function_name="CardFraudKinesisConsumer",
            timeout=Duration.seconds(60),
            log_group=consumer_lambda_log_group,
            memory_size=512,
            environment={
                "SM_ENDPOINT_NAME": params["sagemaker_endpoint_name"],
                "SM_RESULTS_TABLE_NAME": params["dynamodb_fraud_table"],
                "BUCKET_TABLE_NAME": params["dynamodb_bucket_info"],
                "RAW_BUCKET_NAME": params["raw_bucket_name"]                         
            }
        )
        consumer_lambda.add_event_source(consumer_lambda_event_source)

        
        
        generator_lambda_role = _iam.Role(self, "CardFraudRecGenLambdaRole",
            assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name="CardFraudRecGenLambdaRole",
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
            }
        )       
        generator_lambda_log_group = logs.LogGroup(
            self, "generator-lambda-logs",
            retention=logs.RetentionDays.ONE_DAY,
            removal_policy = RemovalPolicy.DESTROY,
            log_group_name=f"/aws/lambda/card-fraud-rec-generator"
        )
        generator_lambda_log_group.add_to_resource_policy(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                principals=[_iam.ServicePrincipal("lambda.amazonaws.com")],
                actions=["logs:CreateLogStream", 
                         "logs:PutLogEvents",
                         ],
                resources=[f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/lambda/card-fraud-rec-generator:*"],
            )
        )       
        generator_lambda = lambda_.Function(self, "CardFraudRecordGenerator",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="record-generator.lambda_handler",
            code=lambda_.Code.from_asset("./card_fraud_ml/lambda/generator"),
            role=generator_lambda_role,
            function_name="CardFraudRecordGenerator",
            timeout=Duration.minutes(3),
            log_group=generator_lambda_log_group,
            memory_size=1024,
            environment={
                "STREAM_NAME": params["kinesis_stream_name"],
                "RAW_BUCKET_NAME": params["raw_bucket_name"],
                "BUCKET_TABLE_NAME": params["dynamodb_bucket_info"]              
            }
        )