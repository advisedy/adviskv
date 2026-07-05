import re
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class MetricsTarget:
    name: str
    url: str


@dataclass
class HistogramSnapshot:
    count: float = 0.0
    sum_us: float = 0.0
    buckets: dict[str, float] = field(default_factory=dict)


@dataclass
class MetricsSample:
    target: MetricsTarget
    timestamp: float
    histograms: dict[str, HistogramSnapshot] = field(default_factory=dict)
    counters: dict[str, float] = field(default_factory=dict)
    error: str = ""


@dataclass
class HistogramSummary:
    target: str
    metric: str
    delta_count: float
    qps: float
    avg_us: Optional[float]
    p50_us: Optional[float]
    p95_us: Optional[float]
    p99_us: Optional[float]


@dataclass
class CounterSummary:
    target: str
    metric: str
    delta: float
    rate: float


@dataclass
class MetricsReport:
    started_at: float
    ended_at: float
    samples: list[MetricsSample]
    histogram_summary: list[HistogramSummary]
    counter_summary: list[CounterSummary]
    errors: list[str]

    @property
    def duration_s(self) -> float:
        return max(0.0, self.ended_at - self.started_at)


METRIC_LINE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+0-9.eE]+)"
)
LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def metric_display_name(name: str) -> str:
    if name.startswith("adviskv_"):
        name = name[len("adviskv_"):]
    if name.endswith("_latency_us"):
        name = name[:-len("_latency_us")]
    if name.endswith("_total"):
        name = name[:-len("_total")]
    return name


def parse_labels(raw: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    for match in LABEL_RE.finditer(raw):
        labels[match.group(1)] = match.group(2).replace('\\"', '"')
    return labels


def parse_prometheus_text(text: str) -> tuple[dict[str, HistogramSnapshot],
                                              dict[str, float]]:
    histograms: dict[str, HistogramSnapshot] = {}
    counters: dict[str, float] = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = METRIC_LINE_RE.match(line)
        if not match:
            continue
        name, raw_labels, raw_value = match.groups()
        try:
            value = float(raw_value)
        except ValueError:
            continue

        if name.endswith("_bucket"):
            base_name = name[:-len("_bucket")]
            labels = parse_labels(raw_labels or "")
            upper_bound = labels.get("le")
            if upper_bound is None:
                continue
            histograms.setdefault(base_name,
                                  HistogramSnapshot()).buckets[upper_bound] = value
        elif name.endswith("_count"):
            base_name = name[:-len("_count")]
            histograms.setdefault(base_name, HistogramSnapshot()).count = value
        elif name.endswith("_sum"):
            base_name = name[:-len("_sum")]
            histograms.setdefault(base_name, HistogramSnapshot()).sum_us = value
        else:
            counters[name] = value

    return histograms, counters


def scrape_target(target: MetricsTarget, timeout_s: float) -> MetricsSample:
    timestamp = time.time()
    request = urllib.request.Request(
        target.url, headers={"User-Agent": "adviskv-metrics-report/1"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            body = response.read().decode("utf-8", errors="replace")
        histograms, counters = parse_prometheus_text(body)
        return MetricsSample(target, timestamp, histograms, counters)
    except (OSError, urllib.error.URLError, TimeoutError) as exc:
        return MetricsSample(target, timestamp, error=str(exc))


def scrape_targets(targets: list[MetricsTarget],
                   timeout_s: float) -> list[MetricsSample]:
    return [scrape_target(target, timeout_s) for target in targets]


def positive_delta(current: float, previous: float) -> float:
    delta = current - previous
    if delta < 0:
        return 0.0
    return delta


def le_sort_key(label: str) -> float:
    if label == "+Inf":
        return float("inf")
    try:
        return float(label)
    except ValueError:
        return float("inf")


def percentile_upper_bound_us(buckets: dict[str, float], count: float,
                              percentile: float) -> Optional[float]:
    if count <= 0:
        return None
    target = count * percentile
    for label, value in sorted(buckets.items(), key=lambda item: le_sort_key(item[0])):
        if value >= target:
            upper_bound = le_sort_key(label)
            if upper_bound == float("inf"):
                return None
            return upper_bound
    return None


def histogram_delta(current: HistogramSnapshot,
                    previous: HistogramSnapshot) -> HistogramSnapshot:
    labels = set(current.buckets) | set(previous.buckets)
    buckets = {
        label: positive_delta(current.buckets.get(label, 0.0),
                              previous.buckets.get(label, 0.0))
        for label in labels
    }
    return HistogramSnapshot(
        count=positive_delta(current.count, previous.count),
        sum_us=positive_delta(current.sum_us, previous.sum_us),
        buckets=buckets,
    )


def histogram_summary(target: str, metric: str, duration_s: float,
                      delta: HistogramSnapshot) -> HistogramSummary:
    avg_us = delta.sum_us / delta.count if delta.count > 0 else None
    return HistogramSummary(
        target=target,
        metric=metric,
        delta_count=delta.count,
        qps=delta.count / duration_s if duration_s > 0 else 0.0,
        avg_us=avg_us,
        p50_us=percentile_upper_bound_us(delta.buckets, delta.count, 0.50),
        p95_us=percentile_upper_bound_us(delta.buckets, delta.count, 0.95),
        p99_us=percentile_upper_bound_us(delta.buckets, delta.count, 0.99),
    )


def valid_samples_by_target(samples: list[MetricsSample]) -> dict[str, list[MetricsSample]]:
    by_target: dict[str, list[MetricsSample]] = {}
    for sample in samples:
        if sample.error:
            continue
        by_target.setdefault(sample.target.name, []).append(sample)
    for target_samples in by_target.values():
        target_samples.sort(key=lambda sample: sample.timestamp)
    return by_target


def build_report(samples: list[MetricsSample]) -> MetricsReport:
    if samples:
        started_at = min(sample.timestamp for sample in samples)
        ended_at = max(sample.timestamp for sample in samples)
    else:
        started_at = ended_at = time.time()

    histogram_summaries: list[HistogramSummary] = []
    counter_summaries: list[CounterSummary] = []
    errors = [
        f"{time.strftime('%H:%M:%S', time.localtime(sample.timestamp))} "
        f"{sample.target.name}: {sample.error}"
        for sample in samples if sample.error
    ]

    for target_name, target_samples in valid_samples_by_target(samples).items():
        if len(target_samples) < 2:
            continue
        first = target_samples[0]
        last = target_samples[-1]
        duration_s = last.timestamp - first.timestamp
        if duration_s <= 0:
            continue

        for metric in set(last.histograms) | set(first.histograms):
            delta = histogram_delta(
                last.histograms.get(metric, HistogramSnapshot()),
                first.histograms.get(metric, HistogramSnapshot()),
            )
            if delta.count > 0:
                histogram_summaries.append(
                    histogram_summary(target_name, metric, duration_s, delta))

        for metric in set(last.counters) | set(first.counters):
            delta = positive_delta(last.counters.get(metric, 0.0),
                                   first.counters.get(metric, 0.0))
            if delta > 0:
                counter_summaries.append(CounterSummary(
                    target=target_name,
                    metric=metric,
                    delta=delta,
                    rate=delta / duration_s,
                ))

    histogram_summaries.sort(
        key=lambda item: (item.target, metric_display_name(item.metric)))
    counter_summaries.sort(
        key=lambda item: (item.target, metric_display_name(item.metric)))
    return MetricsReport(started_at, ended_at, samples, histogram_summaries,
                         counter_summaries, errors)


class MetricsSampler:
    def __init__(self, targets: list[MetricsTarget], interval_s: float = 1.0,
                 timeout_s: float = 0.7):
        self.targets = targets
        self.interval_s = max(0.1, interval_s)
        self.timeout_s = timeout_s
        self._samples: list[MetricsSample] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run,
                                        name="adviskv-metrics-sampler",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> list[MetricsSample]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_s + self.timeout_s + 0.5))
        self.sample_once()
        with self._lock:
            return list(self._samples)

    def sample_once(self) -> None:
        samples = scrape_targets(self.targets, self.timeout_s)
        with self._lock:
            self._samples.extend(samples)

    def _run(self) -> None:
        while not self._stop.is_set():
            self.sample_once()
            self._stop.wait(self.interval_s)


def avg_us(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}"


def bucket_us(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:.0f}"


def fmt_float(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


def format_aligned_table(header: list[str], rows: list[list[str]]) -> str:
    widths = [len(cell) for cell in header]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))
    lines = [
        "  " + "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(header)),
        "  " + "  ".join("-" * width for width in widths),
    ]
    for row in rows:
        lines.append("  " + "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(row)))
    return "\n".join(lines)


def format_report_text(report: MetricsReport) -> str:
    lines = [
        "AdvisKV metrics report",
        f"started_at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(report.started_at))}",
        f"duration_s: {report.duration_s:.1f}",
        f"samples: {len(report.samples)}",
        "notes:",
        "  - Latency rows are timer histograms exported as *_latency_us.",
        "  - count is the timed-event count; qps is timed events per second.",
        "  - avg_us is computed from histogram sum/count and uses microseconds.",
        "  - pXX_us<= is the histogram bucket upper bound containing that percentile.",
        "  - Counter rows are *_total deltas over this benchmark run.",
    ]
    if report.errors:
        lines.append(f"scrape_errors: {len(report.errors)}")
        lines.extend(f"  - {error}" for error in report.errors)

    lines.append("")
    lines.append("Latency histograms")
    if report.histogram_summary:
        rows = [[
            item.target,
            metric_display_name(item.metric),
            str(int(item.delta_count)),
            fmt_float(item.qps),
            avg_us(item.avg_us),
            bucket_us(item.p50_us),
            bucket_us(item.p95_us),
            bucket_us(item.p99_us),
        ] for item in report.histogram_summary]
        lines.append(format_aligned_table(
            ["target", "metric", "count", "qps", "avg_us", "p50_us<=", "p95_us<=", "p99_us<="],
            rows,
        ))
    else:
        lines.append("  no positive deltas")

    lines.append("")
    lines.append("Counters")
    if report.counter_summary:
        rows = [[
            item.target,
            metric_display_name(item.metric),
            str(int(item.delta)),
            fmt_float(item.rate),
        ] for item in report.counter_summary]
        lines.append(format_aligned_table(
            ["target", "metric", "delta", "rate/s"],
            rows,
        ))
    else:
        lines.append("  no positive deltas")

    return "\n".join(lines) + "\n"


def write_text_report(report: MetricsReport, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(format_report_text(report), encoding="utf-8")
