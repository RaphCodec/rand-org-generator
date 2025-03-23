import factory.random
import factory
from icecream import ic
import random
import polars as pl
import json
import numpy as np

factory.random.reseed_random(0)


class UserFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Sequence(lambda n: n + 1)
    name = factory.Faker("name")
    birthdate = factory.Faker("date_between", start_date="-73y", end_date="-18y")
    birthplace = factory.Faker("city")
    race = factory.LazyFunction(
        lambda: random.choice(
            ["White", "Black", "Hispanic", "Asian", "2 or more races"]
        )
    )
    username = factory.LazyAttribute(
        lambda obj: f"{obj.name[0].lower()}{obj.name.split()[1].lower()}"
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


def make_org(size: int = 25):
    org = []

    for _ in range(size):
        person = UserFactory()
        person["birthdate"] = person["birthdate"].isoformat()  # Convert date to string
        person["start_date"] = person[
            "start_date"
        ].isoformat()  # Convert date to string
        org.append(person)

    df = pl.DataFrame(org)

    # getting a list of active ids to pick a root from
    active_ids = (
        df.filter(pl.col("is_active") == True).select("id").to_numpy().flatten()
    )

    root = random.choice(active_ids)
    ic(root)

    df = (
        df
        .with_columns(
            pl.when(pl.col("id") == root)
            .then(pl.lit(None))
            .otherwise(pl.lit(np.random.randint(1, size, df.height)))
            .alias("manager_id"),
        )
    )

    ic(df)

    return df


if __name__ == "__main__":
    org = make_org()
    # ic(org)
    org.write_csv("org.csv")
