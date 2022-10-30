from httpx import get
from loguru import logger
from prefect import flow
from prefect import task


@task(retries=3)
def get_stars(repo: str) -> None:
    url = f"https://api.github.com/repos/{repo}"
    count = get(url).json()["stargazers_count"]
    logger.info("{repo} has {count} stars!", repo=repo, count=count)


@flow(name="GitHub Stars")
def github_stars(repos: list[str]) -> None:
    for repo in repos:
        get_stars(repo)


# run the flow!
github_stars(["PrefectHQ/Prefect"])
