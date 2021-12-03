import pytest

from funcx_endpoint.executors.high_throughput.funcx_worker import MaxResultSizeExceeded


def large_result_producer(size: int) -> str:
    return bytearray(size)


def large_arg_consumer(data: str) -> int:
    return len(data)


@pytest.mark.parametrize("size", [200, 2000, 20000, 200000])
def test_allowed_result_sizes(fx, endpoint, size):
    """funcX should allow all listed result sizes which are under 512KB limit"""

    future = fx.submit(large_result_producer, size, endpoint_id=endpoint)
    assert len(future.result(timeout=60)) == size


def test_result_size_too_large(fx, endpoint, size=550000):
    """
    funcX should raise a MaxResultSizeExceeded exception when results exceeds 512KB
    limit
    """
    future = fx.submit(large_result_producer, size, endpoint_id=endpoint)
    with pytest.raises(MaxResultSizeExceeded):
        future.result(timeout=60)


@pytest.mark.parametrize("size", [200, 2000, 20000, 200000])
def test_allowed_arg_sizes(fx, endpoint, size):
    """funcX should allow all listed result sizes which are under 512KB limit"""

    future = fx.submit(large_arg_consumer, bytearray(size), endpoint_id=endpoint)
    assert future.result(timeout=60) == size


@pytest.mark.skip(reason="As of 0.3.4, an arg size limit is not being enforced")
def test_arg_size_too_large(fx, endpoint, size=55000000):
    """funcX should raise an exception for objects larger than some limit,
    which we are yet to define. This does not work right now.
    """

    future = fx.submit(large_arg_consumer, bytearray(size), endpoint_id=endpoint)
    with pytest.raises(Exception):
        future.result(timeout=60)