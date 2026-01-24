
# Local Airflow — Quick Start

For local testing you can start Airflow quickly using the provided script at `scripts/start_airflow_local.sh`.

- Run from the repository root:

```bash
./scripts/start_airflow_local.sh
```

- Change the port (default is 8080):

```bash
AIRFLOW_PORT=8090 ./scripts/start_airflow_local.sh
```

- Set a custom `AIRFLOW_HOME`:

```bash
AIRFLOW_HOME=/tmp/airflow_dev ./scripts/start_airflow_local.sh
```

- Use the `Makefile` target:

```bash
make start-airflow
```

Behavior:

- The script will use `airflow standalone` if the installed Airflow version supports it.
- If `airflow standalone` is not available, the script runs `airflow db init`, creates an admin user (`admin`/`admin`), starts the `scheduler` in the background and the `webserver` in the foreground.

Notes:

- Make sure to activate the virtualenv or conda environment where Airflow is installed before running the script.
- The scheduler logs are written to `AIRFLOW_HOME/logs`.
