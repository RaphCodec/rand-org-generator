import argparse
import random

import duckdb
import factory
import factory.random
import numpy as np
import polars as pl
from loguru import logger
from rich.progress import BarColumn, Progress, TextColumn, TimeRemainingColumn

# uncomment the followings line to enable logging to a file
# logger.add(
#     "org.log",
#     format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
#     rotation="10 MB",
#     backtrace=True,
#     diagnose=True,
# )


class UserFactory(factory.Factory):
    class Meta:
        model = dict

    class Params:
        female_name = factory.Faker("first_name_female")
        male_name = factory.Faker("first_name_male")

    id = factory.Sequence(lambda n: n + 1)
    sex = factory.LazyFunction(lambda: random.choice(["male", "female"]))
    first_name = factory.LazyAttribute(
        lambda obj: obj.male_name if obj.sex == "male" else obj.female_name
    )
    last_name = factory.Faker("last_name")
    full_name = factory.LazyAttribute(lambda obj: f"{obj.last_name}, {obj.first_name}")
    birthdate = factory.Faker("date_between", start_date="-73y", end_date="-18y")
    birthplace = factory.Faker("city")
    race = factory.LazyFunction(
        lambda: random.choice(
            ["White", "Black", "Hispanic", "Asian", "2 or more races"]
        )
    )
    username = factory.LazyAttribute(
        lambda obj: f"{obj.first_name[0].lower()}{obj.last_name.lower()}"
    )
    email = factory.LazyAttribute(lambda obj: f"{obj.username}@randorggen.com")
    phone = factory.LazyFunction(
        lambda: f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(0, 9999):04d}"
    )
    office_phone = factory.Sequence(lambda n: "222-222-%04d" % n)
    remote_status = factory.LazyFunction(
        lambda: random.choice(["remote", "office", "hybrid"])
    )
    work_location = factory.LazyAttribute(
        lambda obj: "New York City"
        if obj.remote_status in ["office", "hybrid"]
        else obj.birthplace
    )
    start_date = factory.Faker("date_between", start_date=birthdate, end_date="today")
    is_active = factory.LazyFunction(lambda: random.choice([True, False]))
    profile_picture = factory.LazyAttribute(
        lambda obj: f"https://robohash.org/{obj.first_name}{obj.last_name}?set=set5"
    )


def make_org(size: int = 5_000) -> pl.DataFrame:
    org = []

    logger.info(f"Generating data for {size:,} people")

    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
        TimeRemainingColumn(),
    ) as progress:
        task_id = progress.add_task("[cyan]Generating People Data", total=size)
        for _ in range(size):
            person = UserFactory()
            org.append(person)
            progress.update(task_id, advance=1)

    logger.success("People Data Generated. Creating hierarchy...")

    df = pl.DataFrame(org)

    # getting a list of active ids to pick a root from
    active_ids = (
        df.filter(pl.col("is_active") == True).select("id").to_numpy().flatten()
    )

    root = random.choice(active_ids)

    df = df.with_columns(
        pl.when(pl.col("id") == root)
        .then(pl.lit(None))
        .otherwise(pl.lit(np.random.randint(1, size, df.height)))
        .alias("manager_id"),
        # added a check to ensure that start date is after 18th birthdate
        pl.when(
            (pl.col("birthdate") + pl.duration(days=18 * 366)) >= pl.col("start_date")
        )
        .then(pl.col("birthdate") + pl.duration(days=18 * 366))
        .otherwise(pl.col("start_date"))
        .alias("start_date"),
    )

    logger.success("Hierarchy created")

    return df


def export_data(df: pl.DataFrame, path: str, type: str) -> None:
    if type == "csv":
        df.write_csv(path)
    elif type == "json":
        df.write_json(path)
    elif type == "parquet":
        df.write_parquet(path)
    elif type == "duckdb":
        con = duckdb.connect(path)
        con.execute("CREATE TABLE org AS SELECT * FROM df")
        con.close()
    else:
        raise ValueError(f"Unsupported export type: {type}")


if __name__ == "__main__":
    factory.random.reseed_random(0)
    random.seed(0)
    parser = argparse.ArgumentParser(description="Generate random org data")
    parser.add_argument(
        "-s",
        "--size",
        type=int,
        default=5_000,
        help="Number of people to generate (default: 5000)",
    )
    args = parser.parse_args()

    logger.info("Script started")
    try:
        file = "org.parquet"
        file_type = "parquet"
        org = make_org(size=args.size)
        logger.info(f"Exporting data to {file} as {file_type}")
        export_data(org, file, file_type)
        logger.success(f"Script finished. {file} created")
    except Exception as e:
        logger.exception(f"{e}")
        logger.error("Script failed")
        raise e
