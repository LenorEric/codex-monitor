"""Build a released preparation layout for historical migration fixtures only."""

from monitor_delivery import initialize_delivery


def released_delivery(store):
    with store.index.transaction() as db:
        for name, in list(db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'final_%'")):
            db.execute(f'DROP TRIGGER "{name}"')
        for table in ("delivery_items", "delivery_pending", "delivery_models"):
            db.execute(f'DROP TABLE IF EXISTS "{table}"')
        db.execute("DELETE FROM delivery_meta WHERE name='finalRows'")
        initialize_delivery(db)
