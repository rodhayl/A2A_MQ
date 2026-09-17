"""Optional OpenTelemetry instruments with a fixed, payload-free attribute set."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
import logging
import math
from threading import Lock
import time
from urllib.parse import urlsplit

from .domain import PermanentError, RetryableError, require, token

STAGES = frozenset({"publish", "callback", "reconcile", "replay", "retention"})
OUTCOMES = frozenset({"success", "retryable", "rejected", "error", "cancelled"})
GAUGES = {
    "activeTasks": "a2amq.tasks.active",
    "taskRecords": "a2amq.tasks.retained",
    "retiredIdentities": "a2amq.identities.retired",
    "pendingPublications": "a2amq.outbox.pending",
    "pendingActions": "a2amq.actions.pending",
    "oldestPublicationAgeSeconds": "a2amq.outbox.oldest_age",
}


class Telemetry:
    def __init__(self, meter_provider, tracer_provider):
        from opentelemetry.metrics import Observation
        self.meter = meter_provider.get_meter("a2a_mq", "0.1.0")
        self.tracer = tracer_provider.get_tracer("a2a_mq", "0.1.0")
        self.count = self.meter.create_counter("a2amq.operations", unit="{operation}")
        self.duration = self.meter.create_histogram("a2amq.operation.duration", unit="s")
        self._values, self._lock = {}, Lock()
        self._gauges = []
        for field, name in GAUGES.items():
            def sample(options, field=field):
                with self._lock:
                    value = self._values.get(field)
                return [] if value is None else [Observation(value)]
            self._gauges.append(self.meter.create_observable_gauge(name,
                callbacks=[sample], unit="s" if field.endswith("Seconds") else "{item}"))

    def update_backlog(self, values):
        cleaned = {k: v for k, v in values.items() if k in GAUGES
                   and type(v) in {int, float} and math.isfinite(v) and v >= 0}
        with self._lock:
            self._values = cleaned

    @contextmanager
    def operation(self, stage, envelope=None):
        from opentelemetry.trace import Status, StatusCode
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
        require(stage in STAGES, "INVALID_TELEMETRY_STAGE")
        carrier = {} if envelope is None else {k: v for k, v in envelope.as_dict().items()
                                               if k in {"traceparent", "tracestate"}}
        kind = "none" if envelope is None else envelope.as_dict()["kind"]
        require(kind in {"none", "event", "command"}, "INVALID_TELEMETRY_KIND")
        context = TraceContextTextMapPropagator().extract(carrier)
        started, outcome = time.monotonic(), "success"
        with self.tracer.start_as_current_span("a2amq." + stage, context=context,
                record_exception=False, set_status_on_exception=False,
                attributes={"a2amq.stage": stage, "a2amq.kind": kind}) as span:
            try:
                yield
            except asyncio.CancelledError:
                outcome = "cancelled"
                raise
            except RetryableError:
                outcome = "retryable"
                raise
            except PermanentError:
                outcome = "rejected"
                raise
            except BaseException:
                outcome = "error"
                raise
            finally:
                if outcome != "success":
                    span.set_status(Status(StatusCode.ERROR))
                attributes = {"stage": stage, "kind": kind, "outcome": outcome}
                self.count.add(1, attributes)
                self.duration.record(max(0, time.monotonic() - started), attributes)

    def handler(self, function):
        async def wrapped(envelope):
            with self.operation("callback", envelope):
                return await function(envelope)
        return wrapped

    def publisher(self, target):
        telemetry = self
        class Publisher:
            async def publish(self, envelope):
                with telemetry.operation("publish", envelope):
                    await target.publish(envelope)
        return Publisher()


class MetadataHandler(logging.Handler):
    """SDK diagnostic sink which cannot serialize an exception or task payload."""
    def emit(self, record):
        logging.getLogger("a2a_mq.sdk").log(record.levelno, '{"event":"sdk_diagnostic"}')


class OTLP:
    def __init__(self, endpoint, *, service_name, ca_file=None):
        url = urlsplit(endpoint)
        require(url.scheme == "https" and bool(url.hostname) and url.path in {"", "/"}
                and not url.username and not url.password and not url.query and not url.fragment,
                "OTLP_HTTPS_REQUIRED")
        token(service_name)
        try:
            import requests
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        except ImportError:
            raise PermanentError("OBSERVABILITY_EXTRA_REQUIRED") from None

        class Session(requests.Session):
            def request(self, *args, **kwargs):
                kwargs["allow_redirects"] = False
                return super().request(*args, **kwargs)

        self.sessions = [Session(), Session()]
        for session in self.sessions:
            session.trust_env = False
        resource = Resource({"service.name": service_name})
        endpoint = endpoint.rstrip("/")
        trace_exporter = OTLPSpanExporter(endpoint=endpoint + "/v1/traces", timeout=5,
                                         certificate_file=ca_file, session=self.sessions[0], headers={})
        metric_exporter = OTLPMetricExporter(endpoint=endpoint + "/v1/metrics", timeout=5,
                                            certificate_file=ca_file, session=self.sessions[1], headers={})
        self.traces = TracerProvider(resource=resource)
        self.traces.add_span_processor(BatchSpanProcessor(trace_exporter, max_queue_size=2048,
            max_export_batch_size=256, schedule_delay_millis=5000, export_timeout_millis=5000))
        reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=15000,
                                              export_timeout_millis=5000)
        self.metrics = MeterProvider(resource=resource, metric_readers=[reader])
        self.telemetry = Telemetry(self.metrics, self.traces)

    def close(self):
        try:
            self.metrics.shutdown(timeout_millis=10000)
        finally:
            try:
                self.traces.shutdown()
            finally:
                for session in self.sessions:
                    session.close()
