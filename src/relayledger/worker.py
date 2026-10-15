"""One bounded delivery at a time, outside the claim transaction."""

from dataclasses import dataclass

from .outbox import claim, finish
from .retry import RetryPolicy
from .transport import Route, send
from .validation import LeaseLost, RelayError, instant, integer, label


@dataclass(frozen=True)
class WorkResult:
    state: str
    event_id: str | None = None
    code: str | None = None


class Worker:
    def __init__(self, database, routes, *, lease_ms=30_000, retry=None, clock=instant, transport=send):
        if not isinstance(routes,dict) or not routes or len(routes)>100:
            raise RelayError("Worker requires one to 100 configured routes")
        for topic,route in routes.items():
            label(topic)
            if not isinstance(route,Route): raise RelayError("Routes must be validated Route objects")
            if route.timeout_seconds*1000+1000 >= lease_ms:
                raise RelayError("Lease must exceed socket timeout by more than one second")
        integer(lease_ms,1001,3_600_000)
        self.database,self.routes,self.lease_ms = database,dict(routes),lease_ms
        self.retry,self.clock,self.transport = retry or RetryPolicy(),clock,transport

    def run_once(self):
        work = claim(self.database,lease_ms=self.lease_ms,now_ms=self.clock(),topics=self.routes)
        if work is None: return WorkResult('idle')
        delivery = self.transport(work,self.routes[work.topic],now_ms=self.clock())
        now = self.clock()
        retry_at = None
        if delivery.retryable:
            retry_at = now+max(self.retry.delay_ms(work.attempt),delivery.retry_after_ms or 0)
        try:
            state = finish(self.database,work,accepted=delivery.accepted,code=delivery.code,
                           retry_at_ms=retry_at,http_status=delivery.http_status,now_ms=now)
        except LeaseLost:
            return WorkResult('lease_lost',work.event_id,'lease_lost')
        return WorkResult(state,work.event_id,delivery.code)
