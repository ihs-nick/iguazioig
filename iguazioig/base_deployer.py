import os
import yaml
import urllib3
import requests
from abc import ABC
from pathlib import Path
from typing import Dict, Union, List, Any
from v3io.dataplane import Client


class BaseDeployer:

    """
    Deployers handle the actual yaml parsing and deployment - they should be named through semantic versioning
    """
    def __init__(self):
        self.client: DeployerClient = None

    def setup_streams(self) -> Dict[str, Any]:
        raise NotImplementedError

    def setup_functions(self, stream_specs: Dict) -> None:
        raise NotImplementedError

    def __call__(self, dry_run: bool = False, *args, **kwargs) -> Union[None, List[str]]:
        """
        Invoke a deployment, optionally describe deployment plan

        Parameters
        ----------
        dry_run: bool, optional
            Boolean indicating if the deployment should return a list of commands describing the deployment plan

        Returns
        -------
        None or list of deployment steps if dry_run

        """
        self.client = IguazioClient() if not dry_run else DryRunClient()
        stream_specs = self.setup_streams()
        self.setup_functions(stream_specs)
        return self.client.dry_run if dry_run else None

    @staticmethod
    def _read_inference_graph(path: Union[str, Path]) -> Dict:
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    @staticmethod
    def _sluggify_name(name: str) -> str:
        return name.lower().strip().replace(" ", "-")

    def _make_stream_path(self, project_name, stream_name):
        return f'/{project_name}/streams/{self._sluggify_name(stream_name)}'


class DeployerClient(ABC):

    def create_stream(self, stream_name: str, stream_spec: Dict) -> None:
        pass

    def delete_stream(self, stream_name: str, stream_spec: Dict) -> None:
        pass

    def create_function(self,  name: str, project_name: str, mlrun_function) -> None:
        pass

    def delete_function(self,  name: str, project_name: str) -> None:
        pass


class IguazioClient(DeployerClient):

    def __init__(self):
        self.client = Client()

    def create_stream(self, stream_name: str, stream_spec: Dict) -> None:
        # TODO: ensure this doesn't overwrite the data, if it does then check for stream first
        try:
            self.client.stream.create(
                container=stream_spec['container'],
                stream_path=stream_spec['path'],
                shard_count=stream_spec['shards'],
                access_key=os.environ['V3IO_ACCESS_KEY'],
                retention_period_hours=stream_spec['retention']
            )
        except Exception as e:
            raise Exception(f'Stream creation for stream {stream_name} failed with error: {e}')

    def delete_stream(self, stream_name: str, stream_spec: Dict) -> None:

        try:
            self.client.stream.delete(
                container=stream_spec['container'],
                stream_path=stream_spec['path'],
                access_key=os.environ['V3IO_ACCESS_KEY'],
                raise_for_status=[200, 404, 400]  # don't worry if nothing to delete
            )
        except Exception as e:
            raise Exception(f'Stream deletion for stream {stream_name} failed with error: {e}')

    def create_function(self, name: str, project_name: str, mlrun_function) -> None:
        try:
            address = mlrun_function.deploy(project=project_name)
            print(f'Function {name} was deployed into project {project_name} at {address}')
        except Exception as e:
            raise Exception(f'Function {name} failed to deploy with error: {e}')

    def delete_function(self, name: str, project_name: str) -> None:
        # try:
        #     urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        #
        #     session = requests.Session()
        #     session.auth = (IGUAZIO_USERNAME, IGUAZIO_PASSWORD)
        #     auth = session.post(f'{DASHBOARD_URL}/api/sessions', verify=False)
        #
        #     session_id = auth.json()['data']['id']
        #     print(f'Session opened, your session ID is: {session_id}')
        pass  # TODO: find out if project name is also needed to deploy function


class DryRunClient(DeployerClient):

    def __init__(self):
        self.dry_run = []

    def add_command(self, command: str) -> None:
        print(command)
        self.dry_run.append(command)

    def create_stream(self, stream_name: str, stream_spec: Dict) -> None:

        cmd = (f'Create stream {stream_name} - '
               f'container: {stream_spec["container"]} '
               f'path: {stream_spec["path"]} '
               f'shards: {stream_spec["shards"]} '
               f'retention: {stream_spec["retention"]}')

        self.add_command(cmd)

    def delete_stream(self, stream_name: str, stream_spec: Dict) -> None:

        cmd = (f'Delete stream {stream_name} - '
               f'container: {stream_spec["container"]} '
               f'path: {stream_spec["path"]} '
               f'shards: {stream_spec["shards"]} '
               f'retention: {stream_spec["retention"]}')

        self.add_command(cmd)

    def create_function(self, name: str, project_name: str, mlrun_function) -> None:

        cmd = f'Create function {name} in project {project_name} - {mlrun_function.to_dict()}'
        self.add_command(cmd)

    def delete_function(self, name: str, project_name: str) -> None:
        cmd = f'Delete function {name} in project {project_name}'
        self.add_command(cmd)
