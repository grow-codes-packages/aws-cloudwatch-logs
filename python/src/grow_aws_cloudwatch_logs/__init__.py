"""Grow Codes SDK for Amazon CloudWatch Logs.

>>> from grow_aws_cloudwatch_logs import CloudWatchLogsClient, CloudWatchLogsConfig
>>> client = CloudWatchLogsClient.from_env()  # doctest: +SKIP
"""

from . import _core
from ._core import *  # noqa: F403
from ._version import __version__
from .client import AsyncCloudWatchLogsClient, CloudWatchLogsClient
from .config import DEFAULT_URL, CloudWatchLogsConfig

__all__ = [
    "__version__",
    "CloudWatchLogsClient",
    "AsyncCloudWatchLogsClient",
    "CloudWatchLogsConfig",
    "DEFAULT_URL",
]
__all__ += _core.__all__

from . import _logging  # noqa: E402
from ._logging import *  # noqa: E402, F403

__all__ += _logging.__all__
