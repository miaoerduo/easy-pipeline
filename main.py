from easy_pipeline import SimplePipeline, PipelineItem, Task, StopTask, EmptyTask

import multiprocessing as mp


# 定义自己的Task
class NumTask(Task):
    def __init__(self, x):
        self.val = x


# 初始化函数
def get_init_fn(x):
    def init():
        return x
    return init

# 由于


def plus(res, task):
    result = task.val + res
    return NumTask(result)


def mul(res, task):
    result = task.val * res
    return NumTask(result)


def minus(res, task):
    result = task.val - res
    return NumTask(result)


if __name__ == '__main__':

    mp.freeze_support()

    # x = (x + 1) * 2 - 3
    pipeline_items = [
        PipelineItem(plus, get_init_fn(1), 1, 10),      # plus 1
        PipelineItem(mul, get_init_fn(2), 1, 10),       # mul 2
        PipelineItem(minus, get_init_fn(3), 1, 10),     # minus 3
    ]

    manager = mp.Manager()
    job_queue = manager.Queue(1000)

    # 构建Pipeline 并 启动
    pipeline = SimplePipeline(pipeline_items, job_queue)
    pipeline.start()

    # Feed Task
    for i in range(100):
        job_queue.put(NumTask(i))
    job_queue.put(StopTask())

    # 获取输出
    result_queue = pipeline.get_result_queue()
    for i in range(100):
        result = result_queue.get()
        if isinstance(result, StopTask):
            break
        if isinstance(result, EmptyTask):
            continue
        print(result.val)
