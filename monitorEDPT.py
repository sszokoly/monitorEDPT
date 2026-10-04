#!/usr/bin/env python3
"""Print synchronized Avaya endpoint UDP and media summaries via SNMP v2c."""

import argparse
import asyncio
import sys
import time
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional
from typing import Sequence, TextIO, Tuple, Union


COMMUNITY = "public"
DEFAULT_INTERVAL = 5.0
DEFAULT_TIMEOUT = 3.0
BRIGHT_RED = "\033[91m"
BRIGHT_YELLOW = "\033[93m"
RESET = "\033[0m"

udp_in_datagrams = "1.3.6.1.2.1.7.1.0"
udp_out_datagrams = "1.3.6.1.2.1.7.4.0"
lnq = "1.3.6.1.4.1.6889.2.69.6.1.140.0"
far_end_ip = "1.3.6.1.4.1.6889.2.69.6.1.28.0"
far_end_port = "1.3.6.1.4.1.6889.2.69.6.1.29.0"
local_audio_port = "1.3.6.1.4.1.6889.2.69.6.1.66.0"
codec_rx = "1.3.6.1.4.1.6889.2.69.6.1.13.0"
sip_user_id = "1.3.6.1.4.1.6889.2.69.6.7.27.0"
tmsec = "1.3.6.1.4.1.6889.2.69.6.1.94.0"

OIDS = (
    udp_in_datagrams,
    udp_out_datagrams,
    lnq,
    far_end_ip,
    far_end_port,
    local_audio_port,
    codec_rx,
    sip_user_id,
    tmsec,
)  # type: Tuple[str, ...]

RatePair = Tuple[str, str]
CounterPair = Tuple[int, int]
Row = Tuple[str, str, str, str, str, str, str, str, str, str, str]
Poller = Callable[[str, str, float], Awaitable[Optional["RawPhoneObservation"]]]


class RawPhoneObservation:
    def __init__(self, target: str, values: Mapping[str, str]) -> None:
        self.target = target
        self.udp_in_datagrams = _integer(values.get(udp_in_datagrams))
        self.udp_out_datagrams = _integer(values.get(udp_out_datagrams))
        self.lnq = values.get(lnq)
        self.far_end_ip = values.get(far_end_ip)
        self.far_end_port = values.get(far_end_port)
        self.local_audio_port = values.get(local_audio_port)
        self.codec_rx = values.get(codec_rx)
        self.sip_user_id = values.get(sip_user_id)
        self.tmsec = values.get(tmsec)


class CommandResult:
    def __init__(
        self,
        stdout: str = "",
        stderr: str = "",
        returncode: Optional[int] = None,
        timed_out: bool = False,
    ) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        self.timed_out = timed_out


def _integer(value: Optional[str]) -> Optional[int]:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def build_get_argv(
    target: str,
    community: str,
    executable: str = "snmpget",
) -> List[str]:
    return [
        executable,
        "-On",
        "-v",
        "2c",
        "-c",
        community,
        target,
    ] + list(OIDS)


def parse_numeric_output(stdout: str) -> Dict[str, str]:
    values = {}  # type: Dict[str, str]
    for line in stdout.splitlines():
        if " = " not in line:
            continue
        oid, value = line.split(" = ", 1)
        if "No Such" in value or "noSuch" in value:
            continue
        values[oid.strip().lstrip(".")] = _clean_value(value)
    return values


def _clean_value(value: str) -> str:
    value = value.strip()
    prefixes = (
        "STRING: ",
        "INTEGER: ",
        "Counter32: ",
        "Counter64: ",
        "Gauge32: ",
    )
    for prefix in prefixes:
        if value.startswith(prefix):
            value = value[len(prefix):]
            break
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1]
    return value


async def _stop_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        process.kill()
    except ProcessLookupError:
        return
    await process.wait()


async def run_command(argv: Sequence[str], timeout: float) -> CommandResult:
    process = None  # type: Optional[asyncio.subprocess.Process]
    try:
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout,
            )
        except asyncio.TimeoutError:
            await _stop_process(process)
            return CommandResult(timed_out=True)
        return CommandResult(
            stdout.decode(errors="replace"),
            stderr.decode(errors="replace"),
            process.returncode,
        )
    except asyncio.CancelledError:
        if process is not None:
            await _stop_process(process)
        raise
    except OSError as error:
        return CommandResult(stderr=str(error))
    finally:
        if process is not None and hasattr(process, "_transport"):
            transport = getattr(process, "_transport", None)
            if transport is not None:
                transport.close()
                setattr(process, "_transport", None)


async def poll_endpoint(
    target: str,
    community: str,
    timeout: float,
    executable: str = "snmpget",
) -> Optional[RawPhoneObservation]:
    result = await run_command(
        build_get_argv(target, community, executable),
        timeout,
    )
    values = parse_numeric_output(result.stdout)
    if not values:
        return None
    return RawPhoneObservation(target, values)


def _has_media(observation: RawPhoneObservation) -> bool:
    return all(
        value is not None and value.strip()
        for value in (
            observation.local_audio_port,
            observation.far_end_ip,
            observation.far_end_port,
            observation.codec_rx,
        )
    )


def update_rates(
    observation: RawPhoneObservation,
    baselines: Dict[str, CounterPair],
    interval: float,
) -> RatePair:
    if (
        observation.udp_in_datagrams is None
        or observation.udp_out_datagrams is None
    ):
        return "-", "-"
    current = (
        observation.udp_in_datagrams,
        observation.udp_out_datagrams,
    )
    previous = baselines.get(observation.target)
    baselines[observation.target] = current
    if (
        previous is None
        or current[0] < previous[0]
        or current[1] < previous[1]
    ):
        return "-", "-"
    udp_in_rate = max(0, current[0] - previous[0] - 1) / interval
    udp_out_rate = max(0, current[1] - previous[1] - 1) / interval
    return _format_rate(udp_out_rate), _format_rate(udp_in_rate)


def _format_rate(rate: float) -> str:
    if rate == int(rate):
        return "{0}pps".format(int(rate))
    return "{0:.1f}pps".format(rate)


def _color_rate(rate: str) -> str:
    if not rate.endswith("pps"):
        return rate
    value = float(rate[:-3])
    if value < 1:
        return BRIGHT_RED + rate + RESET
    if value < 45:
        return BRIGHT_YELLOW + rate + RESET
    return rate


def collect_rows(
    targets: Sequence[str],
    observations: Sequence[Optional[RawPhoneObservation]],
    baselines: Dict[str, CounterPair],
    interval: float,
) -> List[Row]:
    rows = []  # type: List[Row]
    for target, observation in zip(targets, observations):
        if observation is None:
            continue
        udp_out, udp_in = update_rates(observation, baselines, interval)
        if not _has_media(observation):
            continue
        rows.append((
            time.strftime("%H:%M:%S"),
            observation.sip_user_id or "",
            target,
            observation.local_audio_port or "",
            _color_rate(udp_out),
            _color_rate(udp_in),
            observation.codec_rx or "",
            observation.tmsec or "",
            observation.lnq or "",
            observation.far_end_ip or "",
            observation.far_end_port or "",
        ))
    return rows


def print_rows(
    rows: Sequence[Row],
    printed_before: bool,
    output: Optional[TextIO] = None,
) -> bool:
    if not rows:
        return printed_before
    if output is None:
        output = sys.stdout
    if printed_before:
        print("---", file=output)
    print(
        "time\text\tlocalIP\tlport\tudpOut\tudpIn\tcodec\ttmsec\tlnq"
        "\tremoteIP\trport",
        file=output,
    )
    for row in rows:
        print("\t".join(row), file=output)
    return True


async def collect_batch(
    targets: Sequence[str],
    community: str,
    timeout: float,
    poller: Poller = poll_endpoint,
) -> List[Union[RawPhoneObservation, BaseException, None]]:
    coroutines = [poller(target, community, timeout) for target in targets]
    return await asyncio.gather(*coroutines, return_exceptions=True)


async def monitor(
    targets: Sequence[str],
    community: str,
    interval: float,
    timeout: float,
    output: Optional[TextIO] = None,
) -> None:
    baselines = {}  # type: Dict[str, CounterPair]
    printed_before = False
    while True:
        started = time.monotonic()
        results = await collect_batch(targets, community, timeout)
        observations = [
            result if isinstance(result, RawPhoneObservation) else None
            for result in results
        ]
        rows = collect_rows(targets, observations, baselines, interval)
        printed_before = print_rows(rows, printed_before, output)
        remaining = interval - (time.monotonic() - started)
        if remaining > 0:
            await asyncio.sleep(remaining)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poll Avaya endpoint UDP traffic via SNMP v2c.",
    )
    parser.add_argument(
        "targets",
        nargs="+",
        metavar="IP",
        help="endpoint IP address",
    )
    parser.add_argument(
        "-c",
        "--community",
        default=COMMUNITY,
        help="SNMP v2c community (default: public)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL,
        help="poll interval in seconds (default: 5)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help="per-endpoint timeout in seconds (default: 3)",
    )
    args = parser.parse_args(argv)
    if args.interval < DEFAULT_INTERVAL:
        parser.error("--interval must be at least 5 seconds")
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")
    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    monitor_task = asyncio.ensure_future(
        monitor(
            args.targets,
            args.community,
            args.interval,
            args.timeout,
        ),
        loop=loop,
    )
    try:
        loop.run_until_complete(monitor_task)
    except KeyboardInterrupt:
        monitor_task.cancel()
        loop.run_until_complete(
            asyncio.gather(monitor_task, return_exceptions=True)
        )
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        asyncio.set_event_loop(None)


if __name__ == "__main__":
    main()
