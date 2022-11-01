from collections.abc import Iterable
from collections.abc import Mapping
from datetime import timedelta
from os import getenv
from time import sleep
from typing import Any
from typing import cast

from loguru import logger
from prefect import flow
from prefect import task
from prefect.context import TaskRunContext
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task_input_hash


@task(
    name="My Example Task",
    description="An example task for a tutorial.",
    version=cast(str, getenv("GIT_COMMIT_SHA")),
    tags=["tutorial", "tag-test"],
)
def my_task() -> None:
    pass


@flow
def my_flow() -> None:
    my_task()


my_flow()


@task(retries=2, retry_delay_seconds=60)
def failure() -> None:
    logger.info("running")
    raise ValueError("bad code")


@flow
def test_retries() -> None:
    return failure()


# test_retries()  # noqa


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1))
def hello_task(name_input: str, /) -> str:
    # Doing some work
    logger.info("Saying hello {name_input}", name_input=name_input)
    return "hello " + name_input


@flow
def hello_flow(name_input: str, /) -> None:
    _ = hello_task(name_input)


hello_flow("Marvin")
hello_flow("Marvin")
hello_flow("Trillian")


def cache_key_from_sum(
    context: TaskRunContext, parameters: Mapping[str, Any], /
) -> str | None:
    _ = context
    logger.info("{parameters}", parameters=parameters)
    return str(sum(parameters["nums"]))


@task(cache_key_fn=cache_key_from_sum, cache_expiration=timedelta(minutes=1))
def cached_task(nums: Iterable[int], /) -> int:
    logger.info("running an expensive operation")
    sleep(3)
    return sum(list(nums))


@flow
def test_caching(nums: Iterable[int], /) -> None:
    _ = cached_task(list(nums))


test_caching([2, 2])
test_caching([2, 2])
test_caching([1, 3])
test_caching([2, 3])


@task
def first_task(num: Any, /) -> Any:
    return num + num


@task
def second_task(num: Any, /) -> Any:
    return num * num


@flow(name="My Example Flow", task_runner=SequentialTaskRunner())
def my_flow_2(num: int, /) -> None:
    plusnum = first_task.submit(num)
    sqnum = second_task.submit(plusnum)
    logger.info(
        "add: {plusnum_res}, square: {sqnum_res}",
        plusnum_res=plusnum.result(),
        sqnum_res=sqnum.result(),
    )


my_flow_2(5)
