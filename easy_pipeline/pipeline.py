# -*- coding: utf-8 -*-

from .worker import SimpleWorkerProcess
from .task import StopTask

import multiprocessing as mp


class Pipeline(object):
    def __init__(self):
        pass

    def process(self, task):
        pass

    def start(self):
        pass


class PipelineItem(object):
    def __init__(self, work_fn, init_fn, worker_num, result_max_length=-1):
        self.work_fn = work_fn
        self.init_fn = init_fn
        self.worker_num = worker_num
        self.result_max_length = result_max_length


class SimplePipeline(object):
    def __init__(self, items, job_queue):
        super(SimplePipeline, self).__init__()
        self.job_queue = job_queue
        self.process_pool = []
        self.result_queue = None
        self.manager = mp.Manager()
        for idx in range(len(items)):
            pipeline_item = items[idx]
            task_process_pool = []

            # curr and next worker_num will be used to stop processes safely
            curr_worker_num = mp.Value('i', pipeline_item.worker_num)
            if idx < len(items) - 1:
                next_worker_num = mp.Value('i', items[idx + 1].worker_num)
            else:
                next_worker_num = mp.Value('i', 1)

            self.result_queue = self.manager.Queue(pipeline_item.result_max_length)

            for pid in range(pipeline_item.worker_num):
                task_process_pool.append(
                    SimpleWorkerProcess(
                        pipeline_item.work_fn,
                        pipeline_item.init_fn,
                        self.job_queue,
                        self.result_queue,
                        curr_worker_num,
                        next_worker_num
                    ))
            self.job_queue = self.result_queue
            self.process_pool.append(task_process_pool)

    def start(self):
        for task_process_pool in self.process_pool:
            for task_process in task_process_pool:
                task_process.start()

    def stop(self):
        if len(self.process_pool) > 0:
            task_process_pool = self.process_pool[0]
            for task_process in task_process_pool:
                task_process.job_queue.put(StopTask())
        

    def get_result_queue(self):
        return self.result_queue
