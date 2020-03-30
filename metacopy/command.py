import asyncio

from collections import defaultdict
import json

from cleo import Command
from adbc.database import Database
from adbc.config import get_config

try:
    import uvloop
except ImportError:
    uvloop = None


tables = {
    "card": "report_card",
    "table": "metabase_table",
    "field": "metabase_field",
    "database": "metabase_database",
    "collection": "collection",
    "dashboard": "report_dashboard",
    "dashboard_card": "report_dashboardcard",
}


async def get_model(db, name):
    return await db.model("public", tables.get(name, name))


async def copy_collection(db, collection, databases, collections, cards):
    Collection = await get_model(db, "collection")
    collection_id = collection["id"]
    for target in databases.keys():
        new_collection = await remap_collection(
            db, collection, target, collections, databases
        )
        new_collection = await Collection.body(new_collection).take("id").add()
        collections[collection_id][target] = new_collection["id"]

    await copy_collection_items(db, collection_id, databases, collections, cards)


async def copy_card(db, card, databases, collections, cards):
    Card = await get_model(db, "card")
    card_id = card["id"]
    for target in databases.keys():
        new_card = await remap_card(db, card, target, collections, cards)
        new_card = await Card.body(new_card).take("id").add()
        cards[card_id][target] = new_card["id"]


async def copy_dashboard(db, dashboard, databases, collections, dashboards):
    Dashboard = await get_model(db, "dashboard")
    dashboard_id = dashboard["id"]
    for target in databases.keys():
        new_dashboard = await remap_dashboard(db, dashboard, target, collections)
        new_dashboard = await Dashboard.body(new_dashboard).take("id").add()
        dashboards[dashboard_id][target] = new_dashboard["id"]


async def copy_collection_items(db, collection_id, databases, collections, cards):
    Card = await get_model(db, "card")
    Dashboard = await get_model(db, "dashboard")
    DashboardCard = await get_model(db, "dashboard_card")

    dashboard_ids = []
    # dashboard ids must only be stored within a single collection context
    # cards must be stored across all collections because cards can
    # indirectly depend on other cards
    dashboards = defaultdict(dict)

    for card in await Card.where({"collection_id": collection_id}).sort("id").get():
        await copy_card(db, card, databases, collections, cards)
    for dashboard in (
        await Dashboard.where({"collection_id": collection_id}).sort("id").get()
    ):
        await copy_dashboard(db, dashboard, databases, collections, dashboards)

    for target in dashboard_ids:
        for link in await DashboardCard.where({"dashboard_id": target}).get():
            await copy_dashboardcard(db, link, databases, cards, dashboards)


async def copy_dashboardcard(db, link, databases, cards, dashboards):
    DashboardCard = await get_model(db, "dashboard_card")
    for target in databases.keys():
        new_link = await remap_dashboardcard(db, link, target, cards)
        await DashboardCard.body(new_link).add()


def remap_collection_location(location, target, collections):
    parts = [l for l in location.split("/") if l]
    # remap starting with the 2nd part
    # the first part will remain the same
    for i in range(1, len(parts)):
        parts[i] = collections[int(parts[i])][target]
    result = "/".join([str(p) for p in parts])
    result = f"/{result}/"
    return result


def slugify(name):
    for char in ('.', '_', '(', ')'):
        name = name.replace(char, '_')
    return name


async def remap_collection(db, collection, target, collections, databases):
    collection = dict(collection.items())
    collection["location"] = remap_collection_location(
        collection["location"], target, collections
    )
    if collection["location"].count("/") == 3:
        new_name = databases[target]
        name = collection['name']
        collection["name"] = new_name
        collection["description"] = collection['description'].replace(
            slugify(name), slugify(new_name)
        )
    collection.pop("id", None)
    return collection


async def remap_dashboardcard(db, link, target, cards, dashboards):
    link = dict(link.items())
    # map card/dashboard_id over

    link["card_id"] = cards[link["card_id"]][target]
    link["dashboard_id"] = dashboards[link["dashboard_id"]][target]

    query = json.loads(link["parameter_mappings"])
    query = await remap_query(db, query, target, cards)
    # query also has card_id for some reason...
    query["card_id"] = link["card_id"]
    link["parameter_mappings"] = json.dumps(query)
    link.pop("id", None)
    return link


async def remap_dashboard(db, dashboard, target, collections):
    dashboard = dict(dashboard.items())
    dashboard.pop("id", None)
    collection_id = dashboard["collection_id"]
    dashboard["collection_id"] = collections[collection_id][target]
    return dashboard


async def remap_field(db, field_id, target, cards):
    Field = await get_model(db, "field")
    field = await Field.take("name", "table_id").get(field_id)
    target_table = await remap_table(db, field["table_id"], target, cards)
    return (
        await Field.where({"name": field["name"], "table_id": target_table})
        .field("id")
        .one()
    )


async def remap_table(db, table_id, target, cards):
    Table = await get_model(db, "table")
    if isinstance(table_id, str) and table_id.startswith("card__"):
        card_id = int(table_id.replace("card__", ""))
        new_id = cards[card_id][target]
        return f"card__{new_id}"
    else:
        table = await Table.take("name", "schema").get(table_id)
        schema = table["schema"]
        name = table["name"]
        return (
            await Table.where({"schema": schema, "name": name, "db_id": target})
            .field("id")
            .one()
        )


async def remap_query(db, query, target, cards):
    if isinstance(query, list):
        if len(query) == 2 and query[0] == "field-id":
            field = await remap_field(db, query[1], target, cards)
            print("remap field", query[1], field)
            return ["field-id", field]
        else:
            return [await remap_query(db, q, target, cards) for q in query]
    elif isinstance(query, dict):
        result = {}
        for key, value in query.items():
            new_value = value
            if key == "database":
                new_value = target
                print("remap database", value, new_value)
            elif key == "source-table":
                new_value = await remap_table(db, value, target, cards)
                print("remap table", value, new_value)
            elif key == "fingerprint":
                new_value = None
            else:
                new_value = await remap_query(db, value, target, cards)
            result[key] = new_value
        query = result

    return query


async def remap_card(db, card, target, collections, cards):
    card = dict(card.items())
    query = json.loads(card["dataset_query"])
    query = await remap_query(db, query, target, cards)
    card["dataset_query"] = json.dumps(query)
    card["collection_id"] = collections[card["collection_id"]][target]
    card.pop("id")
    return card


async def copy(alls, base, filename=None):
    config = get_config(filename)
    db = Database(config=config["databases"]["metabase"], verbose=True)
    connection = await db.get_connection()
    db.use(connection)

    DB = await get_model(db, "database")
    Collection = await get_model(db, "collection")

    databases = await DB.take("id", "name").get()
    base_database_id = None
    base_name = f"{base} "
    for row in databases:
        if row["name"].lower().startswith(base_name):
            if base_database_id:
                raise ValueError(f'found database conflicts for "{base}"')
            base_database_id = row["id"]

    if not base_database_id:
        raise ValueError(f'No base database named "{base}"')

    databases = {
        r["id"]: r["name"]
        for r in databases
        if r["id"] != base_database_id and r["name"] == "master"
    }
    environments = (
        await Collection.field("id")
        .where({"location": "/", "name": alls})
        .one()
    )
    base_collection = await Collection.where(
        {
            "location": {"starts.with": f"/{environments}/"},
            "name": {"istarts.with": base_name},
        }
    ).one()
    base_collection_id = base_collection["id"]
    base_collections = await Collection.where(
        {"location": {"starts.with": f"/{environments}/{base_collection_id}/"}}
    ).get()
    base_collections.append(base_collection)

    collections = defaultdict(dict)
    cards = defaultdict(dict)
    async with connection.transaction():
        for collection in sorted(
            base_collections, key=lambda x: x["location"].count("/")
        ):
            await copy_collection(db, collection, databases, collections, cards)
        raise Exception("rollback transaction")


class Copy(Command):
    """Copies Metabase collections across environments

    copy
        {--a|all=: all environments collection name}
        {--b|base=: base environments collection name}
        {--c|config=adbc.yml : config filename}
    """

    def handle(self):
        if uvloop:
            uvloop.install()
        base = self.option('base')
        alls = self.option('all')
        asyncio.run(copy(alls, base))
