from collections.abc import Mapping
from typing import Any

from loguru import logger
from prefect import flow
from prefect import task
from pydantic import BaseModel
from requests import get


@flow
def my_favorite_function() -> int:
    logger.info("What is your favorite number?")
    return 42


logger.info(my_favorite_function())


@task
def call_api(url: str, /) -> dict[str, Any]:
    response = get(url)
    logger.info(response.status_code)
    return response.json()


@task
def parse_fact(response: dict[str, Any], /) -> str:
    fact = response["fact"]
    logger.info(fact)
    return fact


@flow
def api_flow(url: str, /) -> str:
    fact_json = call_api(url)
    return parse_fact(fact_json)


_ = api_flow("http://catfact.ninja/fact")


@flow
def common_flow(config: Mapping[str, Any], /) -> int:
    _ = config
    logger.info("I am a subgraph that shows up in lots of places!")
    return 42


@flow
def main_flow() -> None:
    # do some things
    # then call another flow function
    _ = common_flow({})
    # do more things


main_flow()


@task
def printer(obj: Any, /) -> None:
    logger.info("Received a {type} with value {obj}", type=type(obj), obj=obj)


# note that we define the flow with type hints
@flow
def validation_flow(x: int, y: str, /) -> None:
    printer(x)
    printer(y)


_ = validation_flow("42", 100)  # type: ignore


class Model(BaseModel):
    a: int
    b: float
    c: str


@flow
def model_validator(model: Model) -> None:
    printer(model)


model_validator({"a": 42, "b": 0, "c": 55})  # type: ignore
