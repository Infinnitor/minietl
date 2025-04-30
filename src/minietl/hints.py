from .jobs import Job, JobKind


def jcallable(func):
    return Job(func, JobKind.CALLABLE)


def jasync(func):
    return Job(func, JobKind.ASYNC)


def jaggregate(func):
    return Job(func, JobKind.AGGREGATE)


def jfilter(func):
    return Job(func, JobKind.FILTER)


def jscalar(func):
    return Job(func, JobKind.SCALAR)


def jsplitter(func):
    return Job(func, JobKind.SPLITTER)


class HintSingleton:
    def __init__(self, parent):
        self._parent = parent

        self.callable = self._factory(jcallable)
        self.asyncf = self._factory(jasync)
        self.aggregate = self._factory(jaggregate)
        self.filter = self._factory(jfilter)
        self.scalar = self._factory(jscalar)
        self.splitter = self._factory(jsplitter)

    def _factory(self, func):
        return lambda job: self._parent.add_job(func(job))

    def __call__(self, func):
        return self.callable(func)
