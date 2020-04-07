import asyncio

from collections import defaultdict
import json
import re

from cleo import Command
from adbc.database import Database
from adbc.config import get_config

try:
    import uvloop
except ImportError:
    uvloop = None


COLLECTION_PERMISSION_REGEX = re.compile(r"/collection/([0-9]+)/(.*)")
TABLES = {
    "card": "report_card",
    "table": "metabase_table",
    "field": "metabase_field",
    "database": "metabase_database",
    "collection": "collection",
    "permissions": "permissions",
    "dashboard": "report_dashboard",
    "dashboard_card": "report_dashboardcard",
    "card_series": "dashboardcard_series",
}


async def get_model(db, name):
    return await db.model("public", TABLES.get(name, name))


async def reset_sequences(db):
    for model in (
        "collection",
        "card",
        "dashboard",
        "dashboard_card",
        "card_series",
        "permissions",
    ):
        model = await get_model(db, model)
        table = model.table
        schema = table.namespace.name
        pk = table.pks[0]
        seq = f"{table.name}_id_seq"
        await db.execute(
            f"SELECT setval('{schema}.{seq}',"
            f'(SELECT COALESCE(MAX("{pk}"), 1) + 100 FROM {table.sql_name}))'
        )


async def drop_collections(
    db, databases, root_collection_id, base_collection_id, only=None
):
    Collection = await get_model(db, "collection")
    Card = await get_model(db, "card")
    Dashboard = await get_model(db, "dashboard")
    DashboardCard = await get_model(db, "dashboard_card")
    CardSeries = await get_model(db, "card_series")
    Permissions = await get_model(db, "permissions")

    where = {
        ".and": [
            {"location": {"starts.with": f"/{root_collection_id}/"}},
            {
                ".not": {
                    "location": {
                        "starts.with": f"/{root_collection_id}/{base_collection_id}/"
                    }
                }
            },
            {".not": {"id": base_collection_id}},
        ]
    }
    # collection_ids - get cloned collection ids
    # and all of their descendant IDs
    collections = await Collection.where(where).take("id", "name").get()
    collection_ids = [c["id"] for c in collections if should_process(c["name"], only)]
    if not collection_ids:
        return

    permissions = await permissions_for(Permissions, collection_ids)
    permission_ids = [p["id"] for p in permissions]
    card_ids = (
        await Card.where({"collection_id": {"in": collection_ids}}).field("id").get()
    )
    dashboard_ids = (
        await Dashboard.where({"collection_id": {"in": collection_ids}})
        .field("id")
        .get()
    )
    dashboardcard_ids = (
        await DashboardCard.where({"dashboard_id": {"in": dashboard_ids}})
        .field("id")
        .get()
    )
    cardseries_ids = (
        await CardSeries.where({"dashboardcard_id": {"in": dashboardcard_ids}})
        .field("id")
        .get()
    )
    if permission_ids:
        await Permissions.where({"id": {"in": permission_ids}}).delete()
    if cardseries_ids:
        await CardSeries.where({"id": {"in": cardseries_ids}}).delete()
    if dashboardcard_ids:
        await DashboardCard.where({"id": {"in": dashboardcard_ids}}).delete()
    if dashboard_ids:
        await Dashboard.where({"id": {"in": dashboard_ids}}).delete()
    if card_ids:
        await Card.where({"id": {"in": card_ids}}).delete()
    if collection_ids:
        await Collection.where({"id": {"in": collection_ids}}).delete()


async def permissions_for(Permissions, collection_ids):
    permissions = await Permissions.where(
        {"object": {"starts.with": "/collection/"}}
    ).get()
    return [
        p for p in permissions if any([f"/{c}/" in p["object"] for c in collection_ids])
    ]


def remap_permissions(permission, collections):
    permissions = []
    collection = COLLECTION_PERMISSION_REGEX.match(permission["object"])
    collection_id = int(collection.group(1))
    remainder = collection.group(2)
    collections = collections[collection_id]
    for target, new_id in collections.items():
        new_permission = dict(permission.items())
        new_permission["object"] = f"/collection/{new_id}/{remainder}"
        new_permission.pop("id")
        permissions.append(new_permission)
    return permissions


async def copy_permissions(db, collections):
    Permissions = await get_model(db, "permissions")
    collection_ids = collections.keys()
    permissions = await permissions_for(Permissions, collection_ids)
    for permission in permissions:
        new_permissions = remap_permissions(permission, collections)
        if new_permissions:
            await Permissions.body(new_permissions).add()


async def copy_collections(db, databases, base, collections, cards, dashboards):
    Collection = await get_model(db, "collection")
    for collection in sorted(base, key=lambda x: x["location"].count("/")):
        collection_id = collection["id"]
        for target in databases.keys():
            new_collection = await remap_collection(
                db, collection, target, collections, databases
            )
            new_collection = await Collection.body(new_collection).take("id").add()
            collections[collection_id][target] = new_collection["id"]

        await copy_collection_items(
            db, collection_id, databases, collections, cards, dashboards
        )


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


async def copy_collection_items(
    db, collection_id, databases, collections, cards, dashboards
):
    Card = await get_model(db, "card")
    Dashboard = await get_model(db, "dashboard")

    for card in await Card.where({"collection_id": collection_id}).sort("id").get():
        await copy_card(db, card, databases, collections, cards)
    for dashboard in (
        await Dashboard.where({"collection_id": collection_id}).sort("id").get()
    ):
        await copy_dashboard(db, dashboard, databases, collections, dashboards)


async def remap_cardseries(db, link, target, dashboardcards, cards):
    link = dict(link.items())
    try:
        link["card_id"] = cards[link["card_id"]][target]
    except:
        import pdb

        pdb.set_trace()
        raise
    link["dashboardcard_id"] = dashboardcards[link["dashboardcard_id"]][target]
    link.pop("id")
    return link


async def copy_cardseries(db, databases, dashboardcards, cards):
    Series = await get_model(db, "card_series")
    for link in await Series.where(
        {"dashboardcard_id": {"in": list(dashboardcards.keys())}}
    ).get():
        for target in databases.keys():
            new_link = await remap_cardseries(db, link, target, dashboardcards, cards)
            await Series.body(new_link).add()


async def copy_dashboardcards(db, databases, dashboards, cards, dashboardcards):
    DashboardCard = await get_model(db, "dashboard_card")
    for link in await DashboardCard.where(
        {"dashboard_id": {"in": list(dashboards.keys())}}
    ).get():
        link_id = link["id"]
        for target in databases.keys():
            new_link = await remap_dashboardcard(db, link, target, dashboards, cards)
            new_link = await DashboardCard.body(new_link).take("id").add()
            dashboardcards[link_id][target] = new_link["id"]


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
    for char in (".", "_", "(", ")"):
        name = name.replace(char, "_")
    return name


async def remap_collection(db, collection, target, collections, databases):
    collection = dict(collection.items())
    collection["location"] = remap_collection_location(
        collection["location"], target, collections
    )
    if collection["location"].count("/") == 2:
        new_name = databases[target]
        name = collection["name"]
        collection["name"] = new_name
        collection["description"] = collection["description"].replace(
            slugify(name), slugify(new_name)
        )
    collection.pop("id", None)
    return collection


async def remap_dashboardcard(db, link, target, dashboards, cards):
    link = dict(link.items())
    # map card/dashboard_id over
    old_card_id = link["card_id"]
    if old_card_id:
        link["card_id"] = cards[old_card_id][target]
    link["dashboard_id"] = dashboards[link["dashboard_id"]][target]

    query = json.loads(link["parameter_mappings"])
    query = await remap_query(db, query, target, cards)

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
    if field_id not in db._cache["fields_by_id"]:
        field = await Field.take("name", "table_id").get(field_id)
        db._cache["fields_by_id"][field_id] = field

    field = db._cache["fields_by_id"][field_id]
    target_table = await remap_table(db, field["table_id"], target, cards)

    key = (target_table, field["name"])
    if key not in db._cache["fields_by_name"]:
        db._cache["fields_by_name"][key] = (
            await Field.where({"name": field["name"], "table_id": target_table})
            .field("id")
            .one()
        )

    return db._cache["fields_by_name"][key]


async def remap_table(db, table_id, target, cards):
    Table = await get_model(db, "table")
    if isinstance(table_id, str) and table_id.startswith("card__"):
        card_id = int(table_id.replace("card__", ""))
        try:
            new_id = cards[card_id][target]
        except KeyError:
            print(f'error resolving card {card_id}')
            raise
        return f"card__{new_id}"
    else:
        if table_id not in db._cache["tables_by_id"]:
            db._cache["tables_by_id"][table_id] = await Table.take(
                "name", "schema"
            ).get(table_id)
        table = db._cache["tables_by_id"][table_id]
        schema = table["schema"]
        name = table["name"]
        key = (target, schema, name)
        if key not in db._cache["tables_by_name"]:
            db._cache["tables_by_name"][key] = (
                await Table.where({"schema": schema, "name": name, "db_id": target})
                .field("id")
                .one()
            )
        return db._cache["tables_by_name"][key]


async def remap_query(db, query, target, cards):
    if isinstance(query, list):
        if len(query) == 2 and query[0] == "field-id":
            field = await remap_field(db, query[1], target, cards)
            return ["field-id", field]
        else:
            return [await remap_query(db, q, target, cards) for q in query]
    elif isinstance(query, dict):
        result = {}
        for key, value in query.items():
            new_value = value
            if key == "database":
                new_value = target
            elif key == "source-table":
                new_value = await remap_table(db, value, target, cards)
            elif key == "fingerprint":
                new_value = None
            elif key == "card_id":
                if value is not None:
                    new_value = cards[value][target]
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


def should_process(name, only):
    if not only:
        return True

    name = name.lower()
    for o in only:
        o = o.strip().lower()
        if name.startswith(f"{o} ") or name == o:
            return True
    return False


def setup_cache(db):
    db._cache = {
        "fields_by_id": {},
        "fields_by_name": {},
        "tables_by_name": {},
        "tables_by_id": {},
    }


async def copy(
    alls,
    base,
    verbose=False,
    config=None,
    url=None,
    only=None,
    rollback=False,
    prompt=False,
):
    connection_kwargs = {"verbose": verbose, "prompt": prompt}
    if config:
        config = get_config(config)
        connection_kwargs["config"] = config["databases"]["metabase"]
    elif url:
        connection_kwargs["url"] = url

    db = Database(**connection_kwargs)
    setup_cache(db)
    connection = await db.get_connection()
    db.use(connection)

    DB = await get_model(db, "database")
    Collection = await get_model(db, "collection")

    databases = await DB.take("id", "name").get()
    base = base.lower()
    base_database_id = None
    base_name = f"{base} "
    for row in databases:
        db_name = row["name"].lower()
        if db_name.startswith(base_name) or db_name == base:
            if base_database_id:
                raise ValueError(f'found database conflicts for "{base}"')
            base_database_id = row["id"]

    if not base_database_id:
        raise ValueError(f'No base database named "{base}"')

    if only:
        only = only.split(",")

    databases = {
        r["id"]: r["name"]
        for r in databases
        if r["id"] != base_database_id and should_process(r["name"], only)
    }
    root_collection_id = (
        await Collection.field("id").where({"location": "/", "name": alls}).one()
    )
    base_collection = await Collection.where(
        {
            ".and": [
                {"location": {"starts.with": f"/{root_collection_id}/"}},
                {".or": [{"name": {"istarts.with": base_name}}, {"name": base}]},
            ]
        }
    ).one()
    base_collection_id = base_collection["id"]
    base_collections = await Collection.where(
        {"location": {"starts.with": f"/{root_collection_id}/{base_collection_id}/"}}
    ).get()
    base_collections.append(base_collection)

    collections = defaultdict(dict)
    cards = defaultdict(dict)
    dashboards = defaultdict(dict)
    dashboardcards = defaultdict(dict)
    async with connection.transaction():
        await drop_collections(
            db, databases, root_collection_id, base_collection_id, only
        )
        await reset_sequences(db)
        await copy_collections(
            db, databases, base_collections, collections, cards, dashboards
        )
        await copy_permissions(db, collections)
        await copy_dashboardcards(db, databases, dashboards, cards, dashboardcards)
        await copy_cardseries(db, databases, dashboardcards, cards)
        if rollback:
            raise Exception("rollback transaction")


class Copy(Command):
    """Copies Metabase collections across environments

    copy
        {--a|all= : all environments collection name}
        {--b|base= : base environments collection name}
        {--c|config=adbc.yml : config filename}
        {--d|dry : dry run, rollback all changes after run}
        {--o|only= : only these collections}
        {--p|prompt= : prompt before all queries}
        {--u|url= : DB connection string}
    """

    def handle(self):
        if uvloop:
            uvloop.install()
        base = self.option("base")
        alls = self.option("all")
        config = self.option("config")
        url = self.option("url")
        dry = self.option("dry")
        verbose = self.option("verbose")
        prompt = self.option("prompt")
        only = self.option("only")
        asyncio.run(
            copy(
                alls,
                base,
                url=url,
                only=only,
                config=config,
                rollback=dry,
                verbose=verbose,
                prompt=prompt,
            )
        )
