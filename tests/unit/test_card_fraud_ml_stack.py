import aws_cdk as core
import aws_cdk.assertions as assertions

from card_fraud_ml.card_fraud_ml_stack import CardFraudMlStack

# example tests. To run these tests, uncomment this file along with the example
# resource in card_fraud_ml/card_fraud_ml_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = CardFraudMlStack(app, "card-fraud-ml")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
