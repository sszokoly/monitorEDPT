from typing import Dict, List, Optional, Sequence


UDP_IN_DATAGRAMS = "1.3.6.1.2.1.7.1.0"
UDP_OUT_DATAGRAMS = "1.3.6.1.2.1.7.4.0"
ENDPT_LNQ = "1.3.6.1.4.1.6889.2.69.6.1.140.0"
ENDPT_FAR_END_IP = "1.3.6.1.4.1.6889.2.69.6.1.28.0"
ENDPT_FAR_END_PORT = "1.3.6.1.4.1.6889.2.69.6.1.29.0"
ENDPT_LOCAL_AUDIO_PORT = "1.3.6.1.4.1.6889.2.69.6.1.66.0"
ENDPT_CODEC_RX = "1.3.6.1.4.1.6889.2.69.6.1.13.0"
ENDPT_CODEC_TX = "1.3.6.1.4.1.6889.2.69.6.1.14.0"
ENDPT_SIP_USER_ACCOUNT = "1.3.6.1.4.1.6889.2.69.6.7.26.0"
ENDPT_TMSEC = "1.3.6.1.4.1.6889.2.69.6.1.94.0"
ENDPT_MODEL = "1.3.6.1.4.1.6889.2.69.6.1.52.0"
ENDPT_APP_IN_USE = "1.3.6.1.4.1.6889.2.69.6.1.4.0"

REQUIRED_OIDS = (
    UDP_IN_DATAGRAMS,
    UDP_OUT_DATAGRAMS,
    ENDPT_LNQ,
    ENDPT_FAR_END_IP,
    ENDPT_FAR_END_PORT,
    ENDPT_LOCAL_AUDIO_PORT,
    ENDPT_CODEC_RX,
    ENDPT_CODEC_TX,
    ENDPT_SIP_USER_ACCOUNT,
)


class SnmpCredentials:
    """Preformatted Net-SNMP authentication arguments supplied by the caller."""

    def __init__(self, version: str, arguments: Sequence[str]) -> None:
        if version not in ("1", "2c", "3"):
            raise ValueError("SNMP version must be 1, 2c, or 3")
        self.version = version
        self.arguments = tuple(arguments)


def build_get_argv(
    target: str,
    credentials: SnmpCredentials,
    oids: Sequence[str],
    executable: str = "snmpget",
) -> List[str]:
    return [executable, "-On", "-v", credentials.version] + list(credentials.arguments) + [target] + list(oids)


def interface_oids(if_index: int) -> Sequence[str]:
    if if_index < 1:
        raise ValueError("if_index must be positive")
    return (
        "1.3.6.1.2.1.31.1.1.1.6.{0}".format(if_index),
        "1.3.6.1.2.1.31.1.1.1.10.{0}".format(if_index),
    )


def _clean_value(value: str) -> str:
    value = value.strip()
    for prefix in ("STRING: ", "INTEGER: ", "Counter32: ", "Counter64: ", "Gauge32: "):
        if value.startswith(prefix):
            value = value[len(prefix):]
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1]
    return value


def parse_numeric_output(stdout: str) -> Dict[str, str]:
    values = {}  # type: Dict[str, str]
    for line in stdout.splitlines():
        if " = " not in line:
            continue
        oid, value = line.split(" = ", 1)
        oid = oid.strip().lstrip(".")
        if "No Such" in value or "noSuch" in value:
            continue
        values[oid] = _clean_value(value)
    return values
