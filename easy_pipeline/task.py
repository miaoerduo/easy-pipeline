# -*- coding: utf-8 -*-


class Task(object):
    def __init__(self):
        pass


class EmptyTask(Task):
    def __init__(self):
        super(EmptyTask, self).__init__()


class StopTask(Task):
    def __init__(self):
        super(StopTask, self).__init__()
