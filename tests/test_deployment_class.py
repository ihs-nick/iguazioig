import pytest

from iguazioig.deployment_class import Deployment


def test_set_deployment_unsupported_exception():

    with pytest.raises(Exception) as exc_info:
        deployment = Deployment(api_version='not an api')

    assert 'not an api' in exc_info.value.args[0]
