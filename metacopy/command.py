import asyncio

from collections import defaultdict
import json
import re

from cleo import Command
from adbc.store import Database
from adbc.config import get_config

try:
    import uvloop
except ImportError:
    uvloop = None


COLLECTION_PERMISSION_REGEX = re.compile(r"/collection/([0-9]+)/(.*)")
TABLES = {
    "card": "report_card",
    "question": "report_card",
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
    return await db.get_model(TABLES.get(name, name))


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
        "and": [
            {"like": ['location', f"'/{root_collection_id}/%'"]},
            {
                "not": {
                    'like':
                    [
                        'location', f"'/{root_collection_id}/{base_collection_id}/%'"
                    ]
                }
            },
            {
                "not": {
                    "=": ['id', f"'{base_collection_id}'"]
                }
            },
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
    q_permission_ids = quote_all(permission_ids)

    q_collection_ids = quote_all(collection_ids)
    card_ids = (
        await Card.where({
            "in": ["collection_id", q_collection_ids]
        }).field("id").get()
    )
    q_card_ids = quote_all(card_ids)
    dashboard_ids = (
        await Dashboard.where({
            "in": ["collection_id", q_collection_ids]
        })
        .field("id")
        .get()
    )
    q_dashboard_ids = quote_all(dashboard_ids)
    dashboardcard_ids = (
        await DashboardCard.where({
            "in": ["dashboard_id", q_dashboard_ids]
        })
        .field("id")
        .get()
    )
    q_dashboardcard_ids = quote_all(dashboardcard_ids)
    other_dashboardcard_ids = (
        await DashboardCard.where({
            "in": ["card_id", q_card_ids]
        })
        .field("id")
        .get()
    )
    q_other_dashboardcard_ids = quote_all(other_dashboardcard_ids)
    cardseries_ids = (
        await CardSeries.where({
            "in": ["dashboardcard_id", q_dashboardcard_ids]
        })
        .field("id")
        .get()
    )
    q_cardseries_ids = quote_all(cardseries_ids)
    if permission_ids:
        await Permissions.where({"in": ["id", q_permission_ids]}}).delete()
    if cardseries_ids:
        await CardSeries.where({"in": ["id", q_cardseries_ids]}).delete()
    if dashboardcard_ids:
        await DashboardCard.where({"in": ["id", q_dashboardcard_ids]}).delete()
    if other_dashboardcard_ids:
        await DashboardCard.where({"in": ["id", q_other_dashboardcard_ids]}).delete()
    if dashboard_ids:
        await Dashboard.where({"in": ["id", q_dashboard_ids]}).delete()
    if card_ids:
        await Card.where({"in": ["id", q_card_ids]}).delete()
    if collection_ids:
        await Collection.where({"in": ["id", q_collection_ids]}).delete()


async def permissions_for(Permissions, collection_ids):
    permissions = await Permissions.where(
        {"like": ["object", "'/collection/%'"}}
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


async def copy_card(db, card, databases, cards=None, collections=None):
    Card = await get_model(db, "card")
    card_id = card["id"]
    for target in databases.keys():
        new_card = await remap_card(
            db, card, target, cards=cards, collections=collections
        )
        new_card = await Card.body(new_card).take("id").add()
        if cards is not None:
            cards[card_id][target] = new_card["id"]
    return new_card["id"]


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

    for card in await Card.where({
        "=": ['collection_id', quote(collection_id)]
    }).sort("id").get():
        await copy_card(db, card, databases, cards=cards, collections=collections)
    for dashboard in (
        await Dashboard.where({
            "=": ["collection_id", quote(collection_id)]
        }).sort("id").get()
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
    for link in await Series.where({
        "in": ["dashboardcard_id", quote_all(list(dashboardcards.keys()))]
    }).get():
        for target in databases.keys():
            new_link = await remap_cardseries(db, link, target, dashboardcards, cards)
            await Series.body(new_link).add()


async def copy_dashboardcards(db, databases, dashboards, cards, dashboardcards):
    DashboardCard = await get_model(db, "dashboard_card")
    for link in await DashboardCard.where({
        "in": ["dashboard_id", quote_all(list(dashboards.keys()))]
    }).get():
        link_id = link["id"]
        for target in databases.keys():
            new_link = await remap_dashboardcard(db, link, target, dashboards, cards)
            new_link = await DashboardCard.body(new_link).take("id").add()
            dashboardcards[link_id][target] = new_link["id"]


def remap_collection_location(location, target, collections):
    parts = [l for l in location.split("/") if l]
    can_fail = True
    # try to remap each segment
    for i in range(len(parts)):
        part = int(parts[i])
        if part in collections:
            parts[i] = collections[part][target]
            # once a segment is remapped, all other segments
            # must be remappable
            can_fail = False
        else:
            if not can_fail:
                raise ValueError(
                    f'failed to remap location {location} segment: {part}'
                )
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
        try:
            link["card_id"] = cards[old_card_id][target]
        except KeyError:
            raise ValueError(
                f'Failed to remap dashboardcard on source card: {old_card_id}'
            )
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
    target_table = await remap_table(db, field["table_id"], target, cards=cards)

    key = (target_table, field["name"])
    if key not in db._cache["fields_by_name"]:
        db._cache["fields_by_name"][key] = (
            await Field.where({
                "and": [
                    {"=": ["name", quote(field["name"])]},
                    {'=': ["table_id", quote(target_table)]
                ]
            })
            .field("id")
            .one()
        )

    return db._cache["fields_by_name"][key]


async def remap_table(db, table_id, target, cards=None):
    Table = await get_model(db, "table")
    if isinstance(table_id, str) and table_id.startswith("card__"):
        card_id = int(table_id.replace("card__", ""))
        if cards is not None:
            # assume this card was already remapped and exists in `cards`
            try:
                new_id = cards[card_id][target]
            except KeyError:
                print(
                    f"error resolving card#{card_id} while remapping a table "
                    f"to DB#{target}"
                )
                print(f'(cards = {cards.keys()})')
                raise
        else:
            # recursively copy this card
            Card = await get_model(db, "card")
            card = await Card.key(card_id).one()
            new_id = await copy_card(db, card, {target: 1})

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
                await Table.where({
                    'and': [
                        "=": ["schema", quote(schema)],
                        '=': ["name", quote(name)],
                        '=': ["db_id", quote(target)]
                    ]
                })
                .field("id")
                .one()
            )
        return db._cache["tables_by_name"][key]


async def remap_query(db, query, target, cards=None):
    if isinstance(query, list):
        if len(query) == 2 and query[0] == "field-id":
            field = await remap_field(db, query[1], target, cards=cards)
            return ["field-id", field]
        else:
            return [await remap_query(db, q, target, cards=cards) for q in query]
    elif isinstance(query, dict):
        result = {}
        for key, value in query.items():
            new_value = value
            if key == "database":
                new_value = target
            elif key == "source-table":
                try:
                    new_value = await remap_table(db, value, target, cards=cards)
                except KeyError:
                    print(f"error remapping table during remap_query {query}")
                    raise
            elif key == "fingerprint":
                new_value = None
            elif key == "card_id":
                if value is not None:
                    if cards is not None:
                        new_value = cards[value][target]
                    else:
                        Card = await get_model(db, 'card')
                        card = await Card.key(card_id).one()
                        new_value = await copy_card(db, card, {target: 1})
            else:
                new_value = await remap_query(db, value, target, cards=cards)
            result[key] = new_value
        query = result

    return query


async def remap_card(db, card, target, cards=None, collections=None):
    card = dict(card.items())
    query = json.loads(card["dataset_query"])
    try:
        query = await remap_query(db, query, target, cards=cards)
    except Exception:
        print(f'error remapping card#{card["id"]} to db#{target}')
        raise

    card["dataset_query"] = json.dumps(query)
    if collections:
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


def get_database(verbose=False, prompt=False, config=None, url=None):
    connection_kwargs = {"verbose": verbose, "prompt": prompt}
    if config:
        config = get_config(config)
        connection_kwargs.update(config["databases"]["metabase"])
    elif url:
        connection_kwargs["url"] = url
    return Database(**connection_kwargs)


async def copy_collection(
    collection,
    database,
    url=None,
    config=None,
    rollback=False,
    verbose=False,
    prompt=False
):
    db = get_database(verbose=verbose, prompt=prompt, config=config, url=url)
    setup_cache(db)
    Collection = await get_model(db, 'collection')
    DB = await get_model(db, 'database')

    collection_id = int(collection)
    source_collection = await Collection.key(collection_id).one()
    source_location = source_collection['location']
    source_collections = await Collection.where(
        {"like": ["location", f'"{source_location}{collection_id}/%"']
    }).get()
    source_collections.append(source_collection)

    target_database = (
        await DB.take("id", "name")
        .where({
            "or": [
                {"like": ["name", f'"{database}%"']},
                {'=': ['name', quote(database)]}
            ]
        })
        .one()
    )
    target_database_id = target_database['id']
    databases = {target_database_id: target_database["name"]}
    collections = defaultdict(dict)
    cards = defaultdict(dict)
    dashboards = defaultdict(dict)
    dashboardcards = defaultdict(dict)

    new_id = None
    connection = await db.get_connection()
    db.use(connection)
    async with connection.transaction():
        await copy_collections(
            db, databases, source_collections, collections, cards, dashboards
        )
        await copy_permissions(db, collections)
        await copy_dashboardcards(db, databases, dashboards, cards, dashboardcards)
        await copy_cardseries(db, databases, dashboardcards, cards)

        new_id = collections[collection_id][target_database_id]
        if verbose:
            num_cards = len(cards)
            num_subcollections = len(collections) - 1
            num_dashboards = len(dashboards)
            print(f'new collection ID: {new_id}')
            if num_subcollections:
                print(f'+ {num_subcollections} sub-collections')
            if num_cards:
                print(f'+ {num_cards} cards')
            if num_dashboards:
                print(f'+ {num_dashboards} dashboards')
        if rollback:
            raise Exception("rollback transaction")
    return new_id


async def copy_question(
    question,
    database,
    url=None,
    config=None,
    verbose=False,
    prompt=False,
):
    db = get_database(verbose=verbose, prompt=prompt, config=config, url=url)
    setup_cache(db)
    Card = await get_model(db, "card")
    DB = await get_model(db, "database")

    question = int(question)
    source_card = await Card.key(question).one()
    target_database = (
        await DB.take("id", "name")
        .where({
            "or": [
                {"like": ["name", f'"{database}%"']},
                {"=": ["name", quote(database)]}
            ]
        })
        .one()
    )

    databases = {target_database["id"]: target_database["name"]}
    new_id = await copy_card(db, source_card, databases)
    if verbose:
        print(f"new card ID: {new_id}")
    return new_id


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
    db = get_database(verbose=verbose, prompt=prompt, config=config, url=url)
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
        await Collection.field("id").where({
            "and": [
                {'=': ["location", "'/'"]},
                {'=': ["name", quote(alls)]}
            ]
        }).one()
    )
    base_collection = await Collection.where(
        {
            "and": [
                {"like": ["location", f"'/{root_collection_id}/%'"]},
                {
                    "or": [
                        {"ilike": ["name", quote(base_name)]},
                        {"=": ["name", quote(base)]}
                    ]
                },
            ]
        }
    ).one()
    base_collection_id = base_collection["id"]
    base_collections = await Collection.where(
        {"like": ["location", f"'/{root_collection_id}/{base_collection_id}/%'"]}
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

class CopyCollection(Command):
    """Copies Metabase Collection + sub-collections + Questions + Dashboards

    copy-collection
        {collection : ID of collection (e.g. 12345)}
        {database : name of database to target (e.g: foo)}
        {--r|rollback : dry run, rollback after run}
        {--c|config=adbc.yml : config filename}
        {--p|prompt : prompt before all queries}
        {--u|url : Metabase DB connection string}
    """
    def handle(self):
        if uvloop:
            uvloop.install()
        collection = self.argument("collection")
        database = self.argument("database")
        rollback = self.option("rollback")
        config = self.option("config")
        prompt = self.option("prompt")
        url = self.option("url")
        verbose = self.option("verbose")
        asyncio.run(
            copy_collection(
                collection,
                database,
                url=url,
                config=config,
                rollback=rollback,
                verbose=verbose,
                prompt=prompt,
            )
        )

class CopyQuestion(Command):
    """Copies single Metabase question within the same collection, changing datasource

    copy-question
        {question : ID of question (e.g. 12345)}
        {database : name of database to target (e.g: foo)}
        {--r|rollback : dry run, rollback after run}
        {--c|config=adbc.yml : config filename}
        {--p|prompt : prompt before all queries}
        {--u|url= : Metabase DB connection string}
    """

    def handle(self):
        if uvloop:
            uvloop.install()
        question = self.argument("question")
        database = self.argument("database")
        rollback = self.option("rollback")
        config = self.option("config")
        prompt = self.option("prompt")
        url = self.option("url")
        verbose = self.option("verbose")
        asyncio.run(
            copy_question(
                question,
                database,
                url=url,
                config=config,
                verbose=verbose,
                prompt=prompt,
            )
        )


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
