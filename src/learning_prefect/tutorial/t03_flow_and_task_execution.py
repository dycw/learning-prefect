from collections.abc import Iterable
from time import sleep

from loguru import logger
from prefect import flow
from prefect import task
from prefect.task_runners import SequentialTaskRunner
from prefect_dask.task_runners import DaskTaskRunner


@task
def my_task() -> int:
    return 1


@flow
def my_flow() -> None:
    _ = my_task.submit()


my_flow()


@task
def print_values(values: Iterable[str], /) -> None:
    for value in values:
        sleep(0.5)
        logger.info(value, end="\r")


@flow
def my_flow_2() -> None:
    _ = print_values.submit(["AAAA"] * 15)
    _ = print_values.submit(["BBBB"] * 10)


# my_flow_2()  # noqa


@flow(task_runner=SequentialTaskRunner())
def my_flow_3() -> None:
    _ = print_values.submit(["AAAA"] * 15)
    _ = print_values.submit(["BBBB"] * 10)


# my_flow_3()  # noqa


@flow(task_runner=DaskTaskRunner())
def my_flow_4() -> None:
    _ = print_values.submit(["AAAA"] * 15)
    _ = print_values.submit(["BBBB"] * 10)


if __name__ == "__main__":
    my_flow_4()
