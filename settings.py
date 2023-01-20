import os

env = os.environ.copy()

home = env.get("HOME", "/var/task")

s3_region = env.get("AWS_REGION", "eu-central-1")
s3_access_key_id = env.get("AWS_ACCESS_KEY_ID", "")
s3_secret_access_key = env.get("AWS_SECRET_ACCESS_KEY", "")
s3_session_token = env.get("AWS_SESSION_TOKEN", "")

is_offline = env.get("IS_OFFLINE", "")

duckdb_path = env.get("DUCKDB_PATH", "/tmp/db.duckdb")
duckdb_data_path = env.get("DUCKDB_DATA_PATH", "/var/duckdb")
duckdb_cached = os.path.exists(duckdb_path)
