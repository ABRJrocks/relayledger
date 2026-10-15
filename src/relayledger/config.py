"""Explicit route configuration and environment-based signing secrets."""

import json
import os
from pathlib import Path
import re

from .inbox import _invalid, _pairs
from .transport import Route
from .validation import RelayError, label


def read_json(path):
    try:
        with Path(path).open('rb') as stream: raw = stream.read(1024*1024+1)
        if len(raw)>1024*1024: raise RelayError("JSON file exceeds one MiB")
        return json.loads(raw.decode('utf-8'),object_pairs_hook=_pairs,parse_constant=_invalid)
    except (OSError,UnicodeError,ValueError,RecursionError) as exc:
        if isinstance(exc,RelayError): raise
        raise RelayError("Cannot read a bounded UTF-8 JSON file") from exc


def load_routes(path, *, allow_http_loopback=False, environ=None):
    environ = os.environ if environ is None else environ
    value = read_json(path)
    if not isinstance(value,dict) or set(value)!={'version','routes'} or type(value['version']) is not int or value['version']!=1:
        raise RelayError("Expected version 1 route configuration")
    if not isinstance(value['routes'],dict) or not 1 <= len(value['routes']) <= 100:
        raise RelayError("Expected one to 100 routes")
    result = {}
    def environment(name):
        if not isinstance(name,str) or not re.fullmatch(r'[A-Z][A-Z0-9_]{0,127}',name):
            raise RelayError("Invalid environment variable name")
        data = environ.get(name)
        if not isinstance(data,str) or not data: raise RelayError("Required route environment variable is missing")
        return data
    for topic,route in value['routes'].items():
        label(topic)
        if not isinstance(route,dict) or set(route)-{'url','url_env','secret_env','timeout_seconds'} or 'secret_env' not in route:
            raise RelayError("Route has missing or unknown fields")
        if ('url' in route)==('url_env' in route): raise RelayError("Provide exactly one of url and url_env")
        url = route['url'] if 'url' in route else environment(route['url_env'])
        secret = environment(route['secret_env']).encode('utf-8')
        result[topic] = Route(url,secret,route.get('timeout_seconds',5),allow_http_loopback)
    return result
