from pcf.core.aws_resource import AWSResource
from pcf.core import State
from pcf.util import pcf_util
import logging
import json

logger = logging.getLogger(__name__)


class Activity(AWSResource):

    flavor='sfactivity'

    #this is helpful if the particle doesn't have all three states or has more than three.
    equivalent_states = {
            State.running: 1,
            State.stopped: 0,
            State.terminated: 0,
        }

    START_PARAM_FILTER = {
        "Name",
        "TagSpecifications"
    }

    UNIQUE_KEYS = ["aws_resource.Name"]

    def __init__(self, particle_definition):
        super(Activity, self).__init__(particle_definition, 'sfactivity')
        self.activity_name = self.desired_state_definition["Name"]

    def sync_state(self):
        # try:
        #     self.current_state_definition = self.get_current_definition()
        # except NoResourceException:
        #     self.state = StepFunctionActivity.state_lookup.get('missing')
        pass

    def _terminate(self):
        resp = self.client.delete_activity(activityArn=self.arn)

    def _start(self):
        """
        Starts the sfactivity particle that matches desired state definition
        Returns:
            response of boto3 create_activity
        """
        start_definition = pcf_util.param_filter(self.get_desired_state_definition(), Activity.START_PARAM_FILTER)
        response = self.client.create_activity(**start_definition)
        self.arn = response.activityArn
        return response

    def _stop(self):
        """
        SFactivity does not have a stopped state so it calls terminate.
        """
        return self.terminate()

    def _update(self):
        update_definition = pcf_util.param_filter(self.get_desired_state_definition(), Activity.START_PARAM_FILTER)
        return self.client.put_rule(**update_definition)
    