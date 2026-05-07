import os

# Keep Superset metadata on local SQLite for stable dev startup.
# Postgres remains the analytics source queried by dashboards.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "this_needs_to_be_changed_for_prod")
