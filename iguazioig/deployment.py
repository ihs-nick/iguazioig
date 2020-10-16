import yaml
import v3io_frames as v3f
from mlrun import code_to_function, mount_v3io, mlconf
import os

from iguazioig.composer import composer
from iguazioig.apiv1alpha1 import create_streams_v1alpha1, _deploy_v1alpha1
from iguazioig.apiv2alpha1 import create_streams_v2alpha1, _deploy_v2alpha1
from iguazioig.apiv2alpha2 import create_streams_v2alpha2, _deploy_v2alpha2
from iguazioig.apiv2alpha3 import create_streams_v2alpha3, _deploy_v2alpha3
from iguazioig.apiv0_1 import create_streams_v0_1, _deploy_v0_1


def deploy(yaml_file=''):
    project_graph = yaml.load(open(yaml_file,'rb'),Loader=yaml.FullLoader)
    apiversion=project_graph['apiVersion']
    try:
        deploy_streams = "create_streams_%s(project_graph=project_graph)"% apiversion
        eval(deploy_streams)
    except:
        print("Failed to create streams")
        raise

    try:
        deploy_function = "_deploy_%s(project_graph=project_graph)"% apiversion
        eval(deploy_function)
    except:
        print("Failed to invoke deployment function")
        raise

    project = project_graph['project']['name']
    print ("Project %s Deployed"% project)
