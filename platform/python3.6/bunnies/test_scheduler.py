import pytest
import bunnies.scheduler as S

@pytest.fixture
def sched():
    import bunnies.scheduler as S
    return S.Scheduler()

def test_ready_leaf():
    sched = S.Scheduler()
    n1 = sched.add_node(1)
    n2 = sched.add_node(2)
    sched.initialize()
    stat = sched.status()
    assert len(stat['ready']) == 2


def test_existing_node(sched):
    n1 = sched.add_node(1)
    n2 = sched.add_node(1)
    assert n1 is n2, "should be same node"


def test_simple_dep(sched):
    """
    A -> B
    """
    a = sched.add_node(1)
    b = sched.add_node(2)
    a.depends_on(b)
    sched.initialize()
    stat = sched.status()
    assert stat['ready'] == [b]
    b.done()
    stat = sched.status()
    assert stat['ready'] == [a]
    assert stat['done'] == [b]
    a.done()
    stat = sched.status()
    assert stat['ready'] == []
    assert len(stat['done']) == 2


def test_two_deps(sched):
    """
    A -> B1
    A -> B2
    """
    a = sched.add_node(1)
    b1 = sched.add_node(2)
    b2 = sched.add_node(3)

    a.depends_on(b1)
    a.depends_on(b2)
    sched.initialize()
    stat = sched.status()
    assert b1 in stat['ready']
    assert b2 in stat['ready']
    assert a in stat['waiting']

    b1.done()
    stat = sched.status()
    assert b2 in stat['ready']
    assert b1 in stat['done']
    assert a in stat['waiting']
    b2.done()
    stat = sched.status()
    assert a in stat['ready']


def test_same_dep(sched):
    """
    A1 -> B
    A2 -> B
    """
    a1 = sched.add_node('a1')
    b = sched.add_node('b')
    a2 = sched.add_node('a2')

    a1.depends_on(b)
    a2.depends_on(b)
    assert len(b.rdeps) == 2
    sched.initialize()
    stat = sched.status()
    assert a1 in stat['waiting']
    assert a2 in stat['waiting']
    assert b in stat['ready']

    b.done()
    assert b.state == "done"
    stat = sched.status()
    assert a1 in stat['ready']
    assert a2 in stat['ready']
    assert b in stat['done']


def test_cancel_propagate(sched):
    """
    A -> B
    A -> B2
    cancel B. A should be cancelled.
    """

    a = sched.add_node('a')
    b = sched.add_node('b')
    b2 = sched.add_node('b2')
    a.depends_on(b)
    a.depends_on(b2)

    sched.initialize()
    b.cancel()
    assert a.state == 'cancelled'
    assert b2.state == 'ready'


def test_bad_submit_state(sched):
    """
    A -> B

    """
    a = sched.add_node('a')
    b = sched.add_node('b')
    a.depends_on(b)
    sched.initialize()

    assert a.state == "waiting"
    with pytest.raises(S.SchedError):
        a.submit()


def test_sibling_cancels(sched):
    """
    A -> B
    A -> B2

    B cancels. -> B2 should remain ready.
    """
    a = sched.add_node('a')
    b = sched.add_node('b')
    b2 = sched.add_node('b2')
    a.depends_on(b)
    a.depends_on(b2)

    sched.initialize()
    b.submit()
    assert b2.state == "ready"
    assert b.state == "submitted"
    b.cancel()
    assert a.state == "cancelled"
    assert b2.state == "ready"


def test_failure(sched):
    """
    A -> B
    A -> B2
    fail B

    B should be ready after a failure.
    """
    a = sched.add_node('a')
    b = sched.add_node('b')
    b2 = sched.add_node('b2')
    a.depends_on(b)
    a.depends_on(b2)

    sched.initialize()
    b.submit()
    stat = sched.status()
    assert stat['submitted'] == [b]

    b.failed('timeout')
    assert a.state == 'waiting'
    assert b2.state == 'ready'
    assert b.state == 'ready'
    assert len(b.failures) == 1


def setup_module(module):
    """ setup any state specific to the execution of the given module."""
    print(2)


def teardown_module(module):
    """ teardown any state that was previously setup with a setup_module
    method.
    """
    print(3)


