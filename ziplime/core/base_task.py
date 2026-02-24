import asyncio
from abc import abstractmethod
from logging import Logger

from ziplime.core.entities.enums.task_status import TaskStatus
async def safe_wrapper(c):
    try:
        return await c
    except asyncio.CancelledError:
        raise
    except Exception as e:
       print("A")


class BaseTask:

    def __init__(self, logger: Logger, refresh_interval_seconds: float = 0.0):
        self._refresh_interval_seconds = refresh_interval_seconds
        self._logger = logger
        self._status = TaskStatus.READY
        self._stop_event = asyncio.Event()

    @property
    def status(self) -> TaskStatus:
        return self._status

    def stop(self):
        if self._status != TaskStatus.STOPPED:
            self._status = TaskStatus.STOPPED
            self._stop_event.set()

    async def start(self):
        if self._status == TaskStatus.READY:
            self._stop_event.clear()
            self._status = TaskStatus.STARTING
            self._status = TaskStatus.RUNNING
            # TODO: handle exceptions
            # asyncio.ensure_future(safe_wrapper(self._refresh_interval_loop()))
            await self._refresh_interval_loop()

    async def _refresh_interval_loop(self):
        await self.on_start()
        while not self._stop_event.is_set():
            try:
                await self.on_refresh_interval()
            except Exception as e:
                self._logger.exception(e)
            # finally:
            #     await asyncio.sleep(self._refresh_interval_seconds)
        await self.on_stop()

    @abstractmethod
    async def on_refresh_interval(self):
        ...

    async def on_start(self):
        ...

    async def on_stop(self):
        ...
