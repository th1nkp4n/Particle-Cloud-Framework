from pcf.particle.aws.step_functions.state_machine import StateMachine
from pcf.core import State
import json
import time

state_machine_definition = {
    "Comment": "using an existing step function defintion to test pcf",
    "StartAt": "Start Task And Wait For Callback",
    "States": {
        "Start Task And Wait For Callback": {
        "Type": "Task",
        "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
        "Parameters": {
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/471176887411/StepFunctionsTemplate-CallbackQueue",
            "MessageBody": {
                "MessageTitle": "Task started by Step Functions. Waiting for callback with task token.",
                "TaskToken.$": "$$.Task.Token"
            }
        },
        "Next": "Notify Success",
        "Catch": [
        {
            "ErrorEquals": [ "States.ALL" ],
            "Next": "Notify Failure"
        }
        ]
        },
        "Notify Success": {
        "Type": "Task",
        "Resource": "arn:aws:states:::sns:publish",
        "Parameters": {
            "Message": "Callback received. Task started by Step Functions succeeded.",
            "TopicArn": "arn:aws:sns:us-east-1:471176887411:StepFunctionsSample-WaitForCallbacke1585b4e-1900-4c23-bbbf-959f3161fd6e-SNSTopic-1H9M0JROHKHX2"
        },
        "End": True
        },
        "Notify Failure": {
        "Type": "Task",
        "Resource": "arn:aws:states:::sns:publish",
        "Parameters": {
            "Message": "Task started by Step Functions failed.",
            "TopicArn": "arn:aws:sns:us-east-1:471176887411:StepFunctionsSample-WaitForCallbacke1585b4e-1900-4c23-bbbf-959f3161fd6e-SNSTopic-1H9M0JROHKHX2"
        },
        "End":True
        }
    }
}


state_machine_json = {
    "pcf_name" : "pcf-sm-test",
    "flavor" : "stepfunctions",
    "aws_resource" : {
        "name" : "pcf-sm-test",
        "definition" : json.dumps(state_machine_definition),
        "roleArn" : "arn:aws:iam::471176887411:role/StepFunctionsSample-WaitForCal-StatesExecutionRole-GONJVXVYABNM", 
        "tags" : [
            {
                "key" : "OwnerContact",
                "value": "kyla.qi@capitalone.com"
            }
        ]
    }
}

particle = StateMachine(state_machine_json)

particle.set_desired_state(State.running)
particle.apply()

print(particle.get_state())

time.sleep(60)

particle.set_desired_state(State.terminated)
particle.apply()

print(particle.get_state())

