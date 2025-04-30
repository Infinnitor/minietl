from typing import Any, Optional, Callable, Self, Iterator
from types import SimpleNamespace
from os import PathLike
from pathlib import Path

from .jobs import Job, JobKind, JobType
from . import hints
from . import data
from .data import PathType

import csv
import json

from requests import Session
from requests_cache import CachedSession

import logging
from pprint import pprint, pformat


class Pipeline:
    _input_path: Path
    _output_path: Path
    _state: SimpleNamespace
    _jobs: list[Job]
    _request_session: Session | CachedSession
    _result: Optional[Any]
    _log_level: Optional[int]
    attach: hints.HintSingleton
    should_output_result: bool

    def __init__(
        self,
        source: PathType,
        dest: Optional[PathType] = None,
        state: dict[str, Any] = {},
        request_session: Optional[Session | CachedSession | PathType] = None,
        log_level: Optional[int] = None,
        should_output_result = True
    ):
        # Set input and output paths
        self._input_path = Path(source)
        self._output_path = Path(dest) if dest else None

        self._state = SimpleNamespace(**state)

        # Setup request session
        self._request_session = request_session

        # If path is provided, session should be cached
        if isinstance(request_session, str) or isinstance(request_session, Path):
            self._request_session = CachedSession(request_session)

        # Set var in state
        self._state.session = self._request_session

        self._log_level = log_level
        if self._log_level:
            logging.basicConfig(level=self._log_level)

        self.attach = hints.HintSingleton(self)
        self._jobs = []
        self._result = None
        self.should_output_result = should_output_result

    def run_with(self, *jobs: JobType):
        self.add_many(*jobs)
        result = self.run()
        self.close()
        return result

    def add_job(self, job: JobType):
        job = job if isinstance(job, Job) else hints.jcallable(job)

        logging.debug(f"pipeline: adding job {job.job.__name__} - {job.kind}")
        self._jobs.append(job)

    def add_many(self, *jobs: JobType):
        for job in jobs:
            self.add_job(job)

    def _get_data(self) -> Any:
        if not self._input_path.exists():
            msg = f"Input file {self._input_path} not found"
            raise FileNotFoundError(msg)

        return data.get_data_auto(self._input_path)

    def run(self):
        logging.info(f"getting data from file {self._input_path}")
        data_current = self._get_data()

        for job in self._jobs:
            logging.debug(f"generating job {job.job.__name__}")
            data_current = job.run(data_current, self._state)

        self._result = data_current
        if isinstance(self._result, Iterator):
            logging.debug("pipeline result was a generator, collecting...")
            self._result = list(self._result)

        return self._result

    def _dump_to_output(self):
        if not self._result:
            logging.warning(f"no results were produced")
            return

        if self.should_output_result:
            pprint(self._result)
            logging.debug(pformat(self._result))

        if not self._output_path:
            logging.warning(f"no output path was specified, will not save")
            return

        with open(self._output_path, "w+", encoding="utf-8", newline="") as fp:
            match self._output_path.suffix:
                case ".csv":
                    writer = csv.writer(fp)
                    for d in self._result:
                        row = list(d) if not isinstance(d, list) else d
                        writer.writerow(row)
                case ".json":
                    json.dump(self._result, fp)

        logging.info("finished outputting to file!")

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_) -> None:
        self._dump_to_output()

    def close(self):
        self._dump_to_output()

    def __repr__(self):
        return f"Pipeline(\n{',\n'.join(j.job.__name__ for j in self._jobs)})"
