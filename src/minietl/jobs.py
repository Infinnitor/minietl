from typing import Optional, Callable, Any, Union, Iterable
from enum import StrEnum
import inspect
from types import SimpleNamespace


class JobKind(StrEnum):
    CALLABLE = "JobKind.CALLABLE"
    ASYNC = "JobKind.ASYNC"
    AGGREGATE = "JobKind.AGGREGATE"
    FILTER = "JobKind.FILTER"
    SCALAR = "JobKind.SCALAR"
    SPLITTER = "JobKind.SPLITTER"


class Job:
    job: Callable
    kind: JobKind

    def __init__(self, job: Callable, kind: JobKind = JobKind.CALLABLE):
        self.job = job
        self.kind = kind

    @staticmethod
    def _is_iterable(x: Any) -> bool:
        try:
            iter(x)
            return True
        except TypeError as _:
            return False

    def _pass_args(self, data: Any, state: SimpleNamespace, sig_len: int) -> Any:
        match sig_len:
            case 1:
                return self.job(data)
            case 2:
                return self.job(data, state)
            case 0:
                return self.job()
            case _:
                raise TypeError("Jobs must not have more than 3 arguments")

    def run(self, data: Any, state: SimpleNamespace) -> Union[Iterable[Any], Any]:
        sig = inspect.signature(self.job)
        sig_len = len(sig.parameters)

        match self.kind:
            case JobKind.CALLABLE:
                return (self._pass_args(d, state, sig_len) for d in data)

            case JobKind.AGGREGATE:
                return self._pass_args(data, state, sig_len)

            case JobKind.FILTER:
                return (d for d in data if self._pass_args(d, state, sig_len))

            case JobKind.SCALAR:
                return self._pass_args(data, state, sig_len)

            case JobKind.SPLITTER:
                return (d for d in self._pass_args(data, state, sig_len))


JobType = Job | Callable
