# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
from time import sleep

from celery import chain
from celery import shared_task, group
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


@shared_task(bind=True)
def echo(self, name):
    print(name)
    return name


@shared_task(bind=True)
def a(self):
    subtasks = [b.s(n) for n in (1, 2)]
    ch = chain(
        group(*subtasks),
        e.s()
    )
    raise self.replace(ch)


@shared_task(bind=True)
def b(self, n):
    subtasks = [c.s(s, n) for s in ('a', 'b')]
    ch = chain(
        group(*subtasks),
        d.s()
    )
    raise self.replace(ch)


@shared_task(bind=True)
def c(self, s, n):
    return s * n


@shared_task(bind=True)
def d(self, c_results):
    return '|'.join(c_results)


@shared_task(bind=True)
def e(self, d_results):
    return '\n'.join(d_results)


@shared_task
def add(x, y):
    """Add two numbers."""
    return x + y


@shared_task(bind=True)
def add_replaced(self, x, y):
    """Add two numbers (via the add task)."""
    raise self.replace(add.s(x, y))


@shared_task(bind=True)
def add_to_all(self, nums, val):
    """Add the given value to all supplied numbers."""
    subtasks = [add.s(num, val) for num in nums]
    raise self.replace(group(*subtasks))


@shared_task
def print_unicode(log_message='håå®ƒ valmuefrø', print_message='hiöäüß'):
    """Task that both logs and print strings containing funny characters."""
    logger.warning(log_message)
    print(print_message)


@shared_task
def sleeping(i, **_):
    """Task sleeping for ``i`` seconds, and returning nothing."""
    sleep(i)


@shared_task(bind=True)
def ids(self, i):
    """Returns a tuple of ``root_id``, ``parent_id`` and
    the argument passed as ``i``."""
    return self.request.root_id, self.request.parent_id, i


@shared_task(bind=True)
def collect_ids(self, res, i):
    """Used as a callback in a chain or group where the previous tasks
    are :task:`ids`: returns a tuple of::

        (previous_result, (root_id, parent_id, i))

    """
    return res, (self.request.root_id, self.request.parent_id, i)
