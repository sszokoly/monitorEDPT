import asyncio
import logging
from typing import Optional, Sequence, Tuple


logger = logging.getLogger(__name__)


class CommandResult:
    def __init__(
        self,
        argv: Sequence[str],
        pid: Optional[int],
        stdout: str,
        stderr: str,
        returncode: Optional[int],
        timed_out: bool = False,
    ) -> None:
        self.argv = tuple(argv)  # type: Tuple[str, ...]
        self.pid = pid
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        self.timed_out = timed_out

    @property
    def succeeded(self) -> bool:
        return not self.timed_out and self.returncode == 0


async def _terminate(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        process.kill()
    except ProcessLookupError:
        return
    await process.wait()


async def run_command(argv: Sequence[str], timeout: float = 10.0) -> CommandResult:
    """Run argv without a shell and always return a process result."""
    if not argv:
        raise ValueError("argv must contain an executable")

    process = None  # type: Optional[asyncio.subprocess.Process]
    command = tuple(argv)
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout)
        except asyncio.TimeoutError:
            await _terminate(process)
            return CommandResult(command, process.pid, "", "", process.returncode, True)
        return CommandResult(
            command,
            process.pid,
            stdout.decode(errors="replace"),
            stderr.decode(errors="replace"),
            process.returncode,
        )
    except asyncio.CancelledError:
        if process is not None:
            await _terminate(process)
        raise
    except OSError as error:
        logger.error("Unable to start command %r: %s", command, error)
        return CommandResult(command, None, "", str(error), None)
    finally:
        if process is not None and hasattr(process, "_transport"):
            transport = getattr(process, "_transport", None)
            if transport is not None:
                transport.close()
                setattr(process, "_transport", None)
