import argparse
import asyncio
import logging
import signal
from typing import Optional, Sequence

from .polling import PhoneSample, start_polling
from .snmp import SnmpCredentials


# Replace this temporary default before using the tool outside a trusted network.
SNMP_COMMUNITY = "public"


def _print_sample(sample: PhoneSample) -> None:
    observation = sample.observation
    print(
        "{target} account={account} lnq={lnq} udp_in_pps={udp_in} "
        "udp_out_pps={udp_out} far_end={far_end}:{far_port}".format(
            target=observation.target,
            account=observation.sip_user_account,
            lnq=observation.lnq,
            udp_in=sample.udp_in_rate,
            udp_out=sample.udp_out_rate,
            far_end=observation.far_end_ip,
            far_port=observation.far_end_port,
        )
    )


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poll Avaya phone UDP counters via SNMP v2c.")
    parser.add_argument("targets", nargs="+", metavar="IP", help="phone IP address")
    parser.add_argument("--interval", type=float, default=5.0, help="poll interval in seconds (default: 5)")
    parser.add_argument("--timeout", type=float, default=10.0, help="SNMP command timeout in seconds (default: 10)")
    parser.add_argument("--concurrency", type=int, default=254, help="maximum simultaneous polls (default: 254)")
    parser.add_argument("--if-index", type=int, default=None, help="collect optional IF-MIB octet rates for this interface")
    parser.add_argument("--verbose", action="store_true", help="enable diagnostic logging")
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> None:
    credentials = SnmpCredentials("2c", ("-c", SNMP_COMMUNITY))
    tasks = start_polling(
        args.targets,
        credentials,
        _print_sample,
        interval=args.interval,
        timeout=args.timeout,
        concurrency=args.concurrency,
        if_index=args.if_index,
    )
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(_run(args))
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
