from __future__ import absolute_import, unicode_literals
import pytest
from celery import chain, chord, group
from celery.exceptions import TimeoutError
from celery.result import AsyncResult, GroupResult
from .conftest import flaky
from .tasks import add, add_replaced, add_to_all, collect_ids, ids, echo, a

TIMEOUT = 120


class test_chain:

    @flaky
    def test_simple_chain(self, manager):
        c = add.s(4, 4) | add.s(8) | add.s(16)
        assert c().get(timeout=TIMEOUT) == 32

    @flaky
    def test_complex_chain(self, manager):
        c = (
            add.s(2, 2) | (
                add.s(4) | add_replaced.s(8) | add.s(16) | add.s(32)
            ) |
            group(add.s(i) for i in range(4))
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == [64, 65, 66, 67]

    @flaky
    def test_parent_ids(self, manager, num=10):
        assert manager.inspect().ping()
        c = chain(ids.si(i=i) for i in range(num))
        c.freeze()
        res = c()
        try:
            res.get(timeout=TIMEOUT)
        except TimeoutError:
            print(manager.inspect.active())
            print(manager.inspect.reserved())
            print(manager.inspect.stats())
            raise
        self.assert_ids(res, num - 1)

    def assert_ids(self, res, size):
        i, root = size, res
        while root.parent:
            root = root.parent
        node = res
        while node:
            root_id, parent_id, value = node.get(timeout=30)
            assert value == i
            if node.parent:
                assert parent_id == node.parent.id
            assert root_id == root.id
            node = node.parent
            i -= 1


class test_group:

    @flaky
    def test_parent_ids(self, manager):
        assert manager.inspect().ping()
        g = (
            ids.si(i=1) |
            ids.si(i=2) |
            group(ids.si(i=i) for i in range(2, 50))
        )
        res = g()
        expected_root_id = res.parent.parent.id
        expected_parent_id = res.parent.id
        values = res.get(timeout=TIMEOUT)

        for i, r in enumerate(values):
            root_id, parent_id, value = r
            assert root_id == expected_root_id
            assert parent_id == expected_parent_id
            assert value == i + 2


def assert_ids(r, expected_value, expected_root_id, expected_parent_id):
    root_id, parent_id, value = r.get(timeout=TIMEOUT)
    assert expected_value == value
    assert root_id == expected_root_id
    assert parent_id == expected_parent_id


class test_chord:

    @flaky
    def test_group_chain(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        c = (
            add.s(2, 2) |
            group(add.s(i) for i in range(4)) |
            add_to_all.s(8)
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == [12, 13, 14, 15]

    @flaky
    def test_parent_ids(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        root = ids.si(i=1)
        expected_root_id = root.freeze().id
        g = chain(
            root, ids.si(i=2),
            chord(
                group(ids.si(i=i) for i in range(3, 50)),
                chain(collect_ids.s(i=50) | ids.si(i=51)),
            ),
        )
        self.assert_parentids_chord(g(), expected_root_id)

    @flaky
    def test_parent_ids__OR(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        root = ids.si(i=1)
        expected_root_id = root.freeze().id
        g = (
            root |
            ids.si(i=2) |
            group(ids.si(i=i) for i in range(3, 50)) |
            collect_ids.s(i=50) |
            ids.si(i=51)
        )
        self.assert_parentids_chord(g(), expected_root_id)

    @flaky
    def test_chord_of_chords_with_replacement(self, manager):
        # TODO: make this reuse existing tasks as much as possible, productionize, and submit upstream PR
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        res = a.delay()
        assert res.get(timeout=TIMEOUT) == 'a|b\naa|bb'

    def test_chord_of_chords_no_replacement(self, manager):
        # TODO: make this reuse existing tasks as much as possible, productionize, and submit upstream PR
        # Repro from https://github.com/celery/celery/pull/4301, but note that
        # our modification to freeze() fixes both the replaced-task case, and
        # the repro below, rendering the suggested change to apply_async() by
        # zpl redundant.

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        ch = chord(
            [
                chord(
                    [
                        chord([echo.si(name='111'), echo.si(name='112'), ], body=echo.si(name='11_')),
                        chord([echo.si(name='121'), echo.si(name='122'), ], body=echo.si(name='12_')),
                    ],
                    body=echo.si(name='1_end')
                ),
                chord(
                    [
                        chord([echo.si(name='211'), echo.si(name='212'), ], body=echo.si(name='21_')),
                        chord([echo.si(name='221'), echo.si(name='222'), ], body=echo.si(name='22_')),
                    ],
                    body=echo.si(name='2_end')
                ),
                chord(
                    [
                        chord([echo.si(name='311'), echo.si(name='312'), ], body=echo.si(name='31_')),
                        chord([echo.si(name='321'), echo.si(name='322'), ], body=echo.si(name='32_')),
                    ],
                    body=echo.si(name='3_end')
                ),
            ],
            body=echo.si(name='end')
        )
        result = ch.apply_async()
        print(result.get(timeout=TIMEOUT))

    def assert_parentids_chord(self, res, expected_root_id):
        assert isinstance(res, AsyncResult)
        assert isinstance(res.parent, AsyncResult)
        assert isinstance(res.parent.parent, GroupResult)
        assert isinstance(res.parent.parent.parent, AsyncResult)
        assert isinstance(res.parent.parent.parent.parent, AsyncResult)

        # first we check the last task
        assert_ids(res, 51, expected_root_id, res.parent.id)

        # then the chord callback
        prev, (root_id, parent_id, value) = res.parent.get(timeout=30)
        assert value == 50
        assert root_id == expected_root_id
        # started by one of the chord header tasks.
        assert parent_id in res.parent.parent.results

        # check what the chord callback recorded
        for i, p in enumerate(prev):
            root_id, parent_id, value = p
            assert root_id == expected_root_id
            assert parent_id == res.parent.parent.parent.id

        # ids(i=2)
        root_id, parent_id, value = res.parent.parent.parent.get(timeout=30)
        assert value == 2
        assert parent_id == res.parent.parent.parent.parent.id
        assert root_id == expected_root_id

        # ids(i=1)
        root_id, parent_id, value = res.parent.parent.parent.parent.get(
            timeout=30)
        assert value == 1
        assert root_id == expected_root_id
        assert parent_id is None
