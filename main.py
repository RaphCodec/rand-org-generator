import factory.random
import factory
from icecream import ic
import random
import json

factory.random.reseed_random(0)

class UserFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Sequence(lambda n: n + 1)
    name = factory.Faker('name')
    birthdate = factory.Faker('date_between', start_date='-73y', end_date='-18y')
    birthplace = factory.Faker('city')
    race = factory.LazyFunction(lambda: random.choice(['White', 'Black', 'Hispanic', 'Asian', '2 or more races']))
    username = factory.LazyAttribute(lambda obj: f'{obj.name[0].lower()}{obj.name.split()[1].lower()}')
    email = factory.LazyAttribute(lambda obj: f'{obj.username}@randorggen.com')
    phone = factory.LazyFunction(lambda: f'{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(0, 9999):04d}')
    office_phone = factory.Sequence(lambda n: '222-222-%04d' % n)
    remote_status = factory.LazyFunction(lambda: random.choice(['remote', 'office', 'hybrid']))
    work_location = factory.LazyAttribute(lambda obj: 'New York City' if obj.remote_status in ['office', 'hybrid'] else obj.birthplace)
    start_date = factory.Faker('date_between', start_date=birthdate, end_date='today')
    is_active = factory.LazyFunction(lambda: random.choice([True, False]))

def make_org(size: int = 5):
    org = []
    
    for _ in range(size):
        person = UserFactory()
        person['birthdate'] = person['birthdate'].isoformat()  # Convert date to string
        person['start_date'] = person['start_date'].isoformat()  # Convert date to string
        org.append(person)

    return org

if __name__ == '__main__':
    org = make_org()
    ic(org)
    json.dump(org, open('org.json', 'w'), indent=2)