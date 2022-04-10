from tortoise import Tortoise

db_url = 'postgres://postgres:password@127.0.0.1:5432/project'


async def create_db():
    await Tortoise.init(db_url=db_url, modules={"models": ["models"]})

    # Generate the schema.
    await Tortoise.generate_schemas()
