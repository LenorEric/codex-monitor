"""Empty table scaffolding used only by released migration steps."""

def initialize_delivery(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS dashboard_days(view TEXT,day INTEGER,revision INTEGER NOT NULL,dirty INTEGER NOT NULL,PRIMARY KEY(view,day));
        CREATE TABLE IF NOT EXISTS dashboard_parts(view TEXT,day INTEGER,level INTEGER,digest TEXT NOT NULL,PRIMARY KEY(view,day,level));
        CREATE TABLE IF NOT EXISTS dashboard_day_meta(view TEXT,day INTEGER,data TEXT NOT NULL,PRIMARY KEY(view,day));
        CREATE TABLE IF NOT EXISTS dashboard_blobs(digest TEXT PRIMARY KEY,body BLOB NOT NULL,compressed BLOB NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS dashboard_manifests(scope TEXT PRIMARY KEY,signature TEXT NOT NULL,data TEXT NOT NULL,updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS dashboard_rollups(signature TEXT PRIMARY KEY,digest TEXT NOT NULL,created REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS dashboard_quota_time ON processing_quota_points(view,event_at,account);
        CREATE INDEX IF NOT EXISTS dashboard_display_time ON processing_display(view,kind,event_at,account);
    """)

