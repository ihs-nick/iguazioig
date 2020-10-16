import os
import sys
import ast
import json
import base64
import requests
import importlib
import urllib3
import nuclio
from typing import Dict, Union

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _encode_message(message: Dict) -> str:
    return base64.b64encode(json.dumps(message).encode('utf-8')).decode('utf-8')


def output_stream(context: nuclio.Context,
                  message: Dict,
                  stream_container: str,
                  stream_path: str,
                  partition_key_name: str) -> None:
    try:
        payload = {
            'Records': [{
                'Data': _encode_message(message),
                'PartitionKey': message[partition_key_name]
            }]
        }
    except KeyError as e:
        context.logger.error(f'Failed to post to form payload and post to stream due to missing key {e}')
        return

    url = f'http://v3io-webapi:8081/{stream_container}/{stream_path}/'

    try:
        requests.post(url, headers=context.user_data.headers, json=payload, verify=False)
    except Exception as e:
        context.logger.error(f'Failed to post to stream at {url} for message {message} with error {e}')


def process(context: nuclio.Context, message: Dict) -> Union[None, Dict]:
    user_data = context.user_data
    methods = [getattr(user_data, attribute) for attribute in dir(user_data) if attribute.startswith('methods_')]
    for method in methods:
        try:
            message = method(context, message)
        except Exception as e:
            context.logger.error(f'Iguazioig process wrapper function failed with method {user_data.instance}.{method} '
                                 f'for message {message} with error message {e}')
            return

        if message is None:
            break

    return message


def output(context: nuclio.Context, message: Dict) -> None:
    user_data = context.user_data
    for output_spec in user_data.config.get('outputs', {}):
        if 'streams' in output_spec:
            for stream_name in output_spec:
                try:
                    stream_container = user_data.config['streams'][stream_name]['container']
                    stream_path = user_data.config['streams'][stream_name]['path']
                except KeyError as e:
                    context.logger.error(f'Writing to stream {stream_name} failed due to missing key {e}')
                    continue

                partition_key_name = user_data.config.get('partition_key_name')
                output_stream(context, message, stream_container, stream_path, partition_key_name)

        elif 'https' in output_spec:
            context.logger.info('Http output is not currently implemented')
        elif 'function' in output_spec:
            context.logger.warn('Function output is NOT meant for use in production only for testing')
            print(message)
        else:
            context.logger.warn(f'Unsupported output type {output_spec["type"]} passed to '
                                f'function {user_data.config["function_name"]}')


def init_context(context: nuclio.Context):
    config = ast.literal_eval(os.getenv('STEP_CONFIG'))

    module_paths = config['module_paths'] if config['module_paths'] is not None else []
    for module_path in module_paths:
        sys.path.append(module_path)

    setattr(context.user_data, 'config', config)

    try:
        class_ = getattr(importlib.import_module(config['class_module']), config['class_name'])
        instance = class_(**config.get('class_init', {}))
    except Exception as e:
        context.logger.error(f'Class init failed for {config["class_module"]}.{config["class_name"]} '
                             f'with error {e}')
        raise Exception

    setattr(context.user_data, 'instance', instance)

    for method in config['methods']:
        try:
            setattr(context.user_data, f'methods_{method}', getattr(instance, method))
        except AttributeError as e:
            context.logger.error(f'Failed to find method {method} for class {config["class_name"]}')
            raise Exception

    if 'streams' in config.get('outputs', {}):
        setattr(context.user_data, 'headers', {
            "Content-Type": "application/json",
            "X-v3io-function": "PutRecords",
            "X-v3io-session-key": os.getenv("V3IO_ACCESS_KEY")
        })


def handler(context: nuclio.Context, event: nuclio.Event) -> None:

    if type(event.body) is bytes:
        body = event.body.decode('utf-8')
    else:
        body = event.body

    try:
        message = json.loads(body)
    except json.JSONDecodeError as e:
        context.logger.error(f'Json decoding failed for message {body} with error {e}')
    else:
        message = process(context, message)
        if message is not None:
            output(context, message)
