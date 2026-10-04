# monitorEDPT

`monitorEDPT.py` polls Avaya SIP endpoints through Net-SNMP and prints a
synchronized UDP/media summary for endpoints with active media fields.

## Requirements

- Python 3.6
- Net-SNMP `snmpget` available on `PATH`

No Python packages need to be installed.

## Run

```sh
python3.6 monitorEDPT.py -c community 192.168.123.111 192.168.13.11
```

Options:

- `-c`, `--community`: SNMP v2c community. Defaults to `public`.
- `--interval`: delay after each completed batch in seconds. Defaults to `5`
  and cannot be lower than five seconds.
- `--timeout`: per-endpoint `asyncio.wait_for` deadline in seconds. Defaults
  to `3`.

All endpoint requests in a batch start concurrently. The script waits for all
results or timeouts before printing a fixed-width table, then waits the full
configured interval before the next batch. `udpOut` and `udpIn` are adjusted
UDP packets per second: the counter delta minus one SNMP packet, clamped at
zero, divided by the elapsed monotonic time between successful observations,
then rounded to the nearest integer using Python's standard `round()` behavior.
The first successful sample and a counter reset display `-` because no
comparable prior counter is available.
