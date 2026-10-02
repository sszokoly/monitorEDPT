import asyncio
import inspect
import logging
import time
from typing import Awaitable, Callable, Dict, Optional, Sequence

from .process import CommandResult, run_command
from .snmp import (
    ENDPT_APP_IN_USE,
    ENDPT_CODEC_RX,
    ENDPT_CODEC_TX,
    ENDPT_FAR_END_IP,
    ENDPT_FAR_END_PORT,
    ENDPT_LNQ,
    ENDPT_LOCAL_AUDIO_PORT,
    ENDPT_MODEL,
    ENDPT_SIP_USER_ACCOUNT,
    ENDPT_TMSEC,
    REQUIRED_OIDS,
    UDP_IN_DATAGRAMS,
    UDP_OUT_DATAGRAMS,
    SnmpCredentials,
    build_get_argv,
    interface_oids,
    parse_numeric_output,
)


logger = logging.getLogger(__name__)


class RawPhoneObservation:
    def __init__(self, target: str, values: Dict[str, str]) -> None:
        self.target = target
        self.udp_in_datagrams = int(values[UDP_IN_DATAGRAMS])
        self.udp_out_datagrams = int(values[UDP_OUT_DATAGRAMS])
        self.lnq = int(values[ENDPT_LNQ])
        self.far_end_ip = values[ENDPT_FAR_END_IP]
        self.far_end_port = values[ENDPT_FAR_END_PORT]
        self.local_audio_port = values[ENDPT_LOCAL_AUDIO_PORT]
        self.codec_rx = values[ENDPT_CODEC_RX]
        self.codec_tx = values[ENDPT_CODEC_TX]
        self.sip_user_account = values[ENDPT_SIP_USER_ACCOUNT]


class PhoneSample:
    def __init__(
        self,
        observation: RawPhoneObservation,
        observed_at: float,
        udp_in_rate: Optional[float],
        udp_out_rate: Optional[float],
        packetization_delay: Optional[str] = None,
        interface_in_rate: Optional[float] = None,
        interface_out_rate: Optional[float] = None,
    ) -> None:
        self.observation = observation
        self.observed_at = observed_at
        self.udp_in_rate = udp_in_rate
        self.udp_out_rate = udp_out_rate
        self.packetization_delay = packetization_delay
        self.interface_in_rate = interface_in_rate
        self.interface_out_rate = interface_out_rate


class TargetState:
    def __init__(self) -> None:
        self.last_sample = None  # type: Optional[PhoneSample]
        self.last_timestamp = None  # type: Optional[float]
        self.last_udp_in = None  # type: Optional[int]
        self.last_udp_out = None  # type: Optional[int]
        self.last_interface_in = None  # type: Optional[int]
        self.last_interface_out = None  # type: Optional[int]
        self.model = None  # type: Optional[str]
        self.firmware = None  # type: Optional[str]


def _rate(current: int, previous: Optional[int], elapsed: Optional[float]) -> Optional[float]:
    if previous is None or elapsed is None or elapsed <= 0 or current < previous:
        return None
    return float(current - previous) / elapsed


def parse_observation(target: str, stdout: str) -> RawPhoneObservation:
    values = parse_numeric_output(stdout)
    missing = [oid for oid in REQUIRED_OIDS if oid not in values]
    if missing:
        raise ValueError("missing required OIDs: {0}".format(", ".join(missing)))
    return RawPhoneObservation(target, values)


def update_state(
    state: TargetState,
    observation: RawPhoneObservation,
    observed_at: float,
    packetization_delay: Optional[str] = None,
    interface_values: Optional[Dict[str, str]] = None,
    if_index: int = 2,
) -> PhoneSample:
    elapsed = None if state.last_timestamp is None else observed_at - state.last_timestamp
    udp_in_rate = _rate(observation.udp_in_datagrams, state.last_udp_in, elapsed)
    udp_out_rate = _rate(observation.udp_out_datagrams, state.last_udp_out, elapsed)
    interface_in_rate = None
    interface_out_rate = None
    interface_in = None
    interface_out = None
    if interface_values is not None:
        in_oid, out_oid = interface_oids(if_index)
        try:
            interface_in = int(interface_values[in_oid])
            interface_out = int(interface_values[out_oid])
            interface_in_rate = _rate(interface_in, state.last_interface_in, elapsed)
            interface_out_rate = _rate(interface_out, state.last_interface_out, elapsed)
        except (KeyError, ValueError):
            pass

    sample = PhoneSample(
        observation,
        observed_at,
        udp_in_rate,
        udp_out_rate,
        packetization_delay,
        interface_in_rate,
        interface_out_rate,
    )
    state.last_sample = sample
    state.last_timestamp = observed_at
    state.last_udp_in = observation.udp_in_datagrams
    state.last_udp_out = observation.udp_out_datagrams
    if interface_in is not None and interface_out is not None:
        state.last_interface_in = interface_in
        state.last_interface_out = interface_out
    return sample


async def _optional_values(argv: Sequence[str], timeout: float) -> Dict[str, str]:
    result = await run_command(argv, timeout)
    if not result.succeeded:
        return {}
    return parse_numeric_output(result.stdout)


async def poll_target(
    target: str,
    credentials: SnmpCredentials,
    state: TargetState,
    timeout: float = 10.0,
    if_index: Optional[int] = None,
) -> Optional[PhoneSample]:
    required_result = await run_command(build_get_argv(target, credentials, REQUIRED_OIDS), timeout)
    if not required_result.succeeded:
        logger.warning("Required poll failed for %s: %s", target, required_result.stderr)
        return None
    try:
        observation = parse_observation(target, required_result.stdout)
    except (ValueError, KeyError) as error:
        logger.warning("Invalid required response for %s: %s", target, error)
        return None

    delay_values = await _optional_values(build_get_argv(target, credentials, (ENDPT_TMSEC,)), timeout)
    packetization_delay = delay_values.get(ENDPT_TMSEC)
    interface_values = None
    if if_index is not None:
        interface_values = await _optional_values(
            build_get_argv(target, credentials, interface_oids(if_index)), timeout)
    return update_state(
        state,
        observation,
        time.monotonic(),
        packetization_delay,
        interface_values,
        if_index or 2,
    )


async def discover_target(
    target: str,
    credentials: SnmpCredentials,
    state: TargetState,
    timeout: float = 10.0,
) -> bool:
    result = await run_command(build_get_argv(target, credentials, (ENDPT_MODEL, ENDPT_APP_IN_USE)), timeout)
    if not result.succeeded:
        return False
    values = parse_numeric_output(result.stdout)
    if ENDPT_MODEL not in values or ENDPT_APP_IN_USE not in values:
        return False
    state.model = values[ENDPT_MODEL]
    state.firmware = values[ENDPT_APP_IN_USE]
    return True


async def _publish(callback: Callable[[PhoneSample], object], sample: PhoneSample) -> None:
    result = callback(sample)
    if inspect.isawaitable(result):
        await result


async def poll_loop(
    target: str,
    credentials: SnmpCredentials,
    state: TargetState,
    publish: Callable[[PhoneSample], object],
    semaphore: asyncio.Semaphore,
    interval: float = 5.0,
    timeout: float = 10.0,
    if_index: Optional[int] = None,
) -> None:
    while True:
        started = time.monotonic()
        async with semaphore:
            sample = await poll_target(target, credentials, state, timeout, if_index)
        if sample is not None:
            await _publish(publish, sample)
        remaining = interval - (time.monotonic() - started)
        if remaining > 0:
            await asyncio.sleep(remaining)


def start_polling(
    targets: Sequence[str],
    credentials: SnmpCredentials,
    publish: Callable[[PhoneSample], object],
    interval: float = 5.0,
    timeout: float = 10.0,
    concurrency: int = 254,
    if_index: Optional[int] = None,
) -> Sequence[asyncio.Task]:
    semaphore = asyncio.Semaphore(concurrency)
    return tuple(
        asyncio.ensure_future(poll_loop(target, credentials, TargetState(), publish, semaphore,
                                        interval, timeout, if_index))
        for target in targets
    )
