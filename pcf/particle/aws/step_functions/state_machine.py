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
        "stateMachineArn",
        "definition",
        "roleArn"
    }

    TAG_PARAM_FILTER = {
        "tags"
    }

    UNIQUE_KEYS = ["aws_resource.name"]

    def __init__(self, particle_definition):
        super(StateMachine, self).__init__(particle_definition, 'stepfunctions')
        self.machine_name = self.desired_state_definition["name"]

    def sync_state(self):
        try:
            current_description = self.get_state_machine()
            if current_description is None:
                self.state= StateMachine.state_lookup.get('missing')
            else: 
                self.state = StateMachine.state_lookup.get(current_description["status"].lower()) 
                self.current_state_definition["name"] = current_description["name"]
                self.current_state_definition["definition"] = current_description["definition"]
                self.current_state_definition["roleArn"] = current_description["roleArn"]
                self.current_state_definition["tags"] = self.client.list_tags_for_resource(resourceArn = self._arn)["tags"]
        except NoResourceException:
            self.state = StateMachine.state_lookup.get('missing')


    def _terminate(self):
        resp = self.client.delete_state_machine(stateMachineArn=self._arn)

    def _start(self):
        """
        Starts the sfactivity particle that matches desired state definition
        Returns:
            response of boto3 create_activity
        """
        start_definition = pcf_util.param_filter(self.get_desired_state_definition(), StateMachine.START_PARAM_FILTER)
        #print(start_definition)
        response = self.client.create_state_machine(**start_definition)
        #print(response)
        self._arn = response["stateMachineArn"]
        print(self.arn)
        return response

    def _stop(self):
        """
        SFactivity does not have a stopped state so it calls terminate.
        """
        return self.terminate()

    def _update(self):
        update_definition = pcf_util.param_filter(self.get_desired_state_definition(), StateMachine.UPDATE_PARAM_FILTER)
        update_definition["stateMachineArn"] = self._arn
        update_response = self.client.update_state_machine(**update_definition)
        tag_response = self.client.tag_resource(resourceArn=self._arn, tags=self.desired_state_definition["tags"])
        return update_response
    
    def get_state_machine(self):
        try:
            if self._arn is not None:
                current_description = self.client.describe_state_machine(stateMachineArn= self._arn)
            else: 
                response = self.client.list_state_machines()
                current_description = None
                for machine in response["stateMachines"]:
                    if machine["name"] == self.machine_name:
                        self._arn = machine["stateMachineArn"]
                        current_description = self.client.describe_state_machine(stateMachineArn= machine["stateMachineArn"])
            return current_description
        except ClientError as e:
            if e.response['Error']['Code'] == 'StateMachineDoesNotExist':
                logger.info("State machine {} was not found. State is terminated".format(self.machine_name))
                return None
            else:
                raise e    
    