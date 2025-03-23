import factory.random
import factory
from icecream import ic
import random
import polars as pl
import numpy as np
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from loguru import logger

logger.add(
    "org.log",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    rotation="10 MB",
    backtrace=True,
    diagnose=True,
    mode="w"
)

factory.random.reseed_random(0)
random.seed(0)


class UserFactory(factory.Factory):
    class Meta:
        model = dict
    class Params:
        female_name = factory.Faker("first_name_female")
        male_name = factory.Faker("first_name_male")

    id = factory.Sequence(lambda n: n + 1)
    sex = factory.LazyFunction(lambda: random.choice(["male", "female"]))
    first_name = factory.LazyAttribute(lambda obj: obj.male_name if obj.sex == 'male' else obj.female_name)
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

    df = (
        df
        .with_columns(
            pl.when(pl.col("id") == root)
            .then(pl.lit(None))
            .otherwise(pl.lit(np.random.randint(1, size, df.height)))
            .alias("manager_id"),
        )
    )
    
    logger.success("Hierarchy created")

    return df


if __name__ == "__main__":
    logger.info("Script started")
    try:
        org = make_org()
        org.write_csv("org.csv")
        logger.success("Script finished")
    except Exception as e:
        logger.exception(f"{e}")
        logger.error("Script failed")
        raise e
