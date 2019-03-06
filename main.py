from easy_pipeline import SimplePipeline, PipelineItem, Task, StopTask, EmptyTask

import multiprocessing as mp


# define our Task
class NumTask(Task):
    def __init__(self, x):
        super(NumTask, self).__init__()
        self.val = x


# init function, here we use closure to get different function
def get_init_fn(x):
    def init():
        return x
    return init


# operations
def plus(res, task):
    return NumTask(task.val + res)


def mul(res, task):
    return NumTask(task.val * res)


def minus(res, task):
    return NumTask(task.val - res)


def div(res, task):
    return NumTask(task.val / res)


if __name__ == '__main__':

    # job queue
    manager = mp.Manager()
    job_queue = manager.Queue(1000)

    # define pipeline and start

    # x = （(x + 1) * 2 - 3）/ 5
    pipeline_items = [
        PipelineItem(plus, get_init_fn(1), 1, 10),      # plus 1
        PipelineItem(mul, get_init_fn(2), 1, 10),       # mul 2
        PipelineItem(minus, get_init_fn(3), 1, 10),     # minus 3
        PipelineItem(div, get_init_fn(5.), 1, 10),      # div 5
    ]

    pipeline = SimplePipeline(pipeline_items, job_queue)
    pipeline.start()
    result_queue = pipeline.get_result_queue()

    # Feed jobs anytime (before StopTask)
    for i in range(100):
        job_queue.put(NumTask(i))

    # get output
    for i in range(100):
        result = result_queue.get()
        if isinstance(result, StopTask):
            break
        if isinstance(result, EmptyTask):
            continue
        print(result.val)

    # stop all processes
    # the StopTask number is usually the same as workers
    for p in pipeline_items:
        for _ in range(p.worker_num):
            job_queue.put(StopTask())
