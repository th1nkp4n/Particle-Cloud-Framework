from pcf.core.aws_resource import AWSResource
from pcf.core import State
from pcf.util import pcf_util
from pcf.core.pcf_exceptions import *
from botocore.errorfactory import ClientError
import logging
import json

logger = logging.getLogger(__name__)


class StateMachine(AWSResource):

    flavor='stepfunctions'

    state_lookup = {
        "active": State.running,
        "deleting": State.terminated,
        "missing": State.terminated,
    }

    #this is helpful if the particle doesn't have all three states or has more than three.
    equivalent_states = {
            State.running: 1,
            State.stopped: 0,
            State.terminated: 0,
        }

    START_PARAM_FILTER = {
        "name",
        "definition",
        "roleArn",
        "tags"
    }

    UPDATE_PARAM_FILTER = {
        "",
        "Definition",
        "RoleArn"
    }

    UNIQUE_KEYS = ["aws_resource.name"]

    def __init__(self, particle_definition):
        super(StateMachine, self).__init__(particle_definition, 'stepfunctions')
        self.machine_name = self.desired_state_definition["name"]

    def sync_state(self):
        try:
            self.state= self.get_status()
        except NoResourceException:
            self.state = StateMachine.state_lookup.get('missing')


    def _terminate(self):
        resp = self.client.delete_activity(activityArn=self._arn)

    def _start(self):
        """
        Starts the sfactivity particle that matches desired state definition
        Returns:
            response of boto3 create_activity
        """
        start_definition = pcf_util.param_filter(self.get_desired_state_definition(), StateMachine.START_PARAM_FILTER)
        print(start_definition)
        response = self.client.create_state_machine(**start_definition)
        self._arn = response.stateMachineArn
        return response

    def _stop(self):
        """
        SFactivity does not have a stopped state so it calls terminate.
        """
        return self.terminate()

    def _update(self):
        update_definition = pcf_util.param_filter(self.get_desired_state_definition(), StateMachine.UPDATE_PARAM_FILTER)
        return self.client.update_state_machine(**update_definition)
    
    def get_status(self):
        try:
            if self._arn is not None:
                current_description = self.client.describe_state_machine(stateMachineArn= self._arn)
                print(current_description)
                current_status = current_description.status
            else: 
                current_status = State.terminated
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.info("Function {} was not found. State is terminated".format(self.machine_name))
                return State.terminated
            else:
                raise e
        return current_status