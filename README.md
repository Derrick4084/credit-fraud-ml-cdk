
# Welcome to your CDK Python project!

This is a project that creates a machine learning model with the
linear-learning algorithm that detects fraudulent credit card transactions.


To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```



Drop csv file of transactions in the created bucket to start the model creation
process


Run the CardFraudRecordGenerator lambda to send transactions to kinesis and to the 
model endpoint
