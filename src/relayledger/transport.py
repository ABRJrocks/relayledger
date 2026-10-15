"""Signed HTTP POST with explicit loopback opt-in and no redirects."""

from dataclasses import dataclass, field
import http.client
import ipaddress
import math
import socket
import ssl
from urllib.parse import urlsplit

from .retry import retry_after_ms
from .signing import sign, validate_secret
from .validation import RelayError


@dataclass(frozen=True)
class Route:
    url: str = field(repr=False)
    secret: bytes = field(repr=False)
    timeout_seconds: float = 5
    allow_http_loopback: bool = False

    def __post_init__(self):
        validate_secret(self.secret)
        if type(self.timeout_seconds) not in (int,float) or not 0 < self.timeout_seconds <= 60 or not math.isfinite(self.timeout_seconds):
            raise RelayError("HTTP timeout must be between zero and 60 seconds")
        if type(self.allow_http_loopback) is not bool: raise RelayError("Loopback opt-in must be boolean")
        if not isinstance(self.url,str) or not self.url.isascii() or any(ord(c)<=32 or ord(c)==127 for c in self.url):
            raise RelayError("Route URL must be ASCII without control characters")
        try:
            parsed = urlsplit(self.url)
            if not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.fragment:
                raise ValueError('invalid URL')
            if parsed.port is not None and not 1 <= parsed.port <= 65535: raise ValueError('port')
            if parsed.scheme != 'https':
                if parsed.scheme != 'http' or not self.allow_http_loopback or not ipaddress.ip_address(parsed.hostname).is_loopback:
                    raise ValueError('HTTPS required')
        except ValueError as exc:
            raise RelayError("Route requires HTTPS; numeric loopback HTTP needs explicit opt-in") from exc


@dataclass(frozen=True)
class Delivery:
    accepted: bool
    retryable: bool
    code: str
    http_status: int | None = None
    retry_after_ms: int | None = None


def send(work, route, *, now_ms):
    parsed = urlsplit(route.url)
    connection = None
    try:
        if parsed.scheme == 'https':
            connection = http.client.HTTPSConnection(parsed.hostname,parsed.port,timeout=route.timeout_seconds,
                                                     context=ssl.create_default_context())
        else:
            connection = http.client.HTTPConnection(parsed.hostname,parsed.port,timeout=route.timeout_seconds)
        path = parsed.path or '/'
        if parsed.query: path += '?'+parsed.query
        connection.request('POST',path,body=work.body,headers={
            'Content-Type':'application/cloudevents+json','Idempotency-Key':work.event_id,
            'RelayLedger-Signature':sign(work.body,route.secret,now_ms=now_ms),
            'X-RelayLedger-Attempt':str(work.total_attempt),'User-Agent':'RelayLedger/0.1.0'})
        response = connection.getresponse()
        status = response.status
        if 200 <= status < 300: return Delivery(True,False,f'http_{status}',status)
        retryable = status in (408,425,429) or 500 <= status < 600
        delay = None
        if retryable:
            try:
                delay = retry_after_ms(response.getheader('Retry-After'),now_ms=now_ms)
            except RelayError:
                return Delivery(False,False,'retry_after_excessive',status)
        return Delivery(False,retryable,f'http_{status}',status,delay)
    except ssl.SSLError:
        return Delivery(False,False,'tls_error')
    except (socket.timeout,TimeoutError):
        return Delivery(False,True,'timeout')
    except http.client.HTTPException:
        return Delivery(False,True,'protocol_error')
    except OSError:
        return Delivery(False,True,'network_error')
    finally:
        if connection is not None: connection.close()
