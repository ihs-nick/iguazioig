import os
import json
import warnings
from pathlib import Path
from typing import Dict, Union, List, Any

from mlrun import code_to_function, mount_v3io

from iguazioig.base_deployer import BaseDeployer


class Deployer(BaseDeployer):

    def __init__(self,
                 inference_graph: Union[str, Path],
                 recreate_data: bool = False,
                 include_functions: List[str] = None):

        """
        Deploys inference graph from yaml specification for api 0.2.0

        Parameters
        ----------
        inference_graph : str or pathlib.Path
            Path to a yaml file describing deployment
        recreate_data : bool, optional
            Boolean indicating whether to delete the project data directory before deployment (default is False)
        include_functions: list, optional
            List of functions in the inference graph to selectively deploy (default is None deploying all functions)

        """

        super().__init__()

        self.template = str(Path(__file__).parent.absolute() / 'templates' / 'processing_template_020.py')
        self.api_version = '0.2.0'
        self.inference_graph = self._read_inference_graph(inference_graph)

        ig_api = self.inference_graph['apiVersion']
        if ig_api != self.api_version:
            raise Exception(f'Incorrect API specified for this deployment in inference graph: {ig_api}')

        try:
            self.project_name = self._sluggify_name(self.inference_graph['project']['name'])
        except KeyError:
            raise Exception('Project name is missing from inference graph')

        self.recreate_data = recreate_data
        if self.recreate_data:
            warnings.warn('Recreating data will delete ALL functions before the data is recreated')

        self.include_functions = include_functions

        if self.recreate_data and include_functions is not None:
            warnings.warn(f'Only the following functions will be restored {include_functions}')

    @staticmethod
    def _format_pip_libraries(function: Dict) -> List[str]:
        """Adds user specified pip libraries to a string for function build commands"""
        pip_libraries = function['pip'] if 'pip' in function else []
        pip_libraries = [library for library in pip_libraries if 'v3io==' not in library]
        pip_libraries.append('v3io==0.5.0')
        pip_libraries = ' '.join(pip_libraries)
        return [f'pip install {pip_libraries}']

    def setup_streams(self) -> Dict[str, Any]:

        project = self.inference_graph['project']

        default_container = project['env_defaults'].get('stream_container', 'bigdata')
        default_shards = project['env_defaults'].get('stream_shards', 3)
        default_retention = project['env_defaults'].get('retention', 48)

        if self.recreate_data:
            for function in project['functions']:
                function_name = self._sluggify_name(function['name'])
                self.client.delete_function(function_name, self.project_name)

        stream_specs = project.get('v3io_streams', {})
        for stream_name, stream_spec in stream_specs.items():

            default_path = self._make_stream_path(self.project_name, stream_name)

            stream_spec.setdefault('path', default_path)
            stream_spec.setdefault('container', default_container)
            stream_spec.setdefault('shards', default_shards)
            stream_spec.setdefault('retention', default_retention)

            if self.recreate_data:
                self.client.delete_stream(stream_name, stream_spec)

            self.client.create_stream(stream_name, stream_spec)

        return stream_specs

    def setup_functions(self, stream_specs: Dict) -> None:

        project = self.inference_graph['project']
        functions = self.inference_graph['project']['functions']

        if self.include_functions is not None:
            functions = [function for function in functions if function in self.include_functions]

        for function in functions:
            function_name = self._sluggify_name(function['name'])
            function_tag = function.get('tag', 'latest')
            function_log_level = function.get('log_level', 'debug')
            fn = code_to_function(name=function_name,
                                  tag=function_tag,
                                  project=self.project_name,
                                  filename=self.template,
                                  kind='nuclio')

            fn.set_env('NUCLIO_FUNCTION_BUILDER_VERSION', self.api_version)
            fn.set_env('NUCLIO_FUNCTION_PROJECT', self.project_name)
            fn.set_env('NUCLIO_FUNCTION_NAME', function_name)
            fn.set_env('NUCLIO_FUNCTION_TAG', function_tag)
            fn.set_env('NUCLIO_FUNCTION_LOG_LEVEL', function_log_level)

            fn.spec.base_spec['spec']['loggerSinks'] = [{'level': function_log_level}]

            # Build Vars
            fn.spec.build.base_image = function['docker_image']
            fn.spec.build.commands = self._format_pip_libraries(function)

            fn.spec.min_replicas = function['min_replicas']
            fn.spec.max_replicas = function['max_replicas']

            if bool(function.get('gpu', False)):
                fn.spec.base_spec['spec']['resources'].update({'limits': {'nvidia.com/gpu': function['num_gpus']}})

            # Input Stream Triggers
            input_streams = function.get('input_streams', {})
            for stream_name, trigger_spec in input_streams.items():
                container = stream_specs[stream_name]['container']
                stream_path = stream_specs[stream_name]['path']

                # default max workers is the number of shards for that stream
                max_workers = trigger_spec.get('max_workers', int(stream_specs[stream_name]['shards']))

                v3io_key = trigger_spec.get('v3io_access_key', os.getenv('V3IO_ACCESS_KEY'))
                polling_interval = trigger_spec.get('polling_interval_ms', 500)
                seek_to = trigger_spec.get('seek_to', 'latest')
                read_batch_size = trigger_spec.get('read_batch_size', 100)

                trigger_spec = {
                    'kind': 'v3ioStream',
                    'url': f"http://v3io-webapi:8081/{container}/{stream_path}@{function_name}",
                    "password": v3io_key,
                    "maxWorkers": max_workers,
                    'attributes': {
                        "pollingIntervalMs": polling_interval,
                        "seekTo": seek_to,
                        "readBatchSize": read_batch_size,
                    }
                }

                fn.add_trigger(stream_name, trigger_spec)

            # Volumes
            for name, volume_spec in project.get('v3io_volumes', {}).items():
                fn.apply(mount_v3io(name=name, remote=volume_spec['remote'], mount_path=volume_spec['mount_path']))

            # Environment Vars
            for env_var, value in function.get('env_custom', {}).items():
                fn.set_env(env_var, value)

            # Template Config
            function_fields = [
                'module_paths',
                'class_module',
                'class_name',
                'methods',
                'outputs'
            ]

            step_config = dict()
            for field in function_fields:
                step_config[field] = function[field]

            step_config['class_init'] = function.get('class_init', {})
            step_config['function_name'] = function_name
            step_config['streams'] = input_streams
            step_config['partition_key_name'] = project.get('partition_key_name', 'PartitionKey')

            fn.set_env("STEP_CONFIG", json.dumps(step_config))

            self.client.create_function(function_name, self.project_name, fn)
