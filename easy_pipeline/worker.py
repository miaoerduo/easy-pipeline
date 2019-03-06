# -*- coding: utf-8 -*-

from .task import Task, EmptyTask, StopTask
import multiprocessing as mp


class Worker(object):
    def __init__(self):
        pass

    def process(self, task):
        pass


class SimpleWorker(Worker):
    def __init__(self, work_fn, init_fn=None):
        super(SimpleWorker, self).__init__()
        self.work_fn = work_fn
        self.resource = None
        if init_fn is not None:
            self.resource = init_fn()

    def process(self, task):
        if not isinstance(task, Task):
            raise Exception("Input is not a Task: {}".format(type(task)))
        if isinstance(task, EmptyTask):
            return EmptyTask()
        if isinstance(task, StopTask):
            return StopTask()
        return self.work_fn(self.resource, task)


class SimpleWorkerProcess(mp.Process):
    def __init__(self, work_fn, init_fn, job_queue, result_queue):
        super(SimpleWorkerProcess, self).__init__()
        self.worker = SimpleWorker(work_fn, init_fn)
        self.job_queue = job_queue
        self.result_queue = result_queue

    def run(self):
        while True:
            task = self.job_queue.get()
            if isinstance(task, StopTask):
                self.result_queue.put(StopTask())
                break
            result = self.worker.process(task)
            self.result_queue.put(result)
