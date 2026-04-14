"""Streamlit app that queries Microsoft SQL Server via SQLAlchemy."""

import os
from datetime import datetime
from typing import Any, Sequence
from urllib.parse import quote_plus

import altair as alt
import pandas as pd
import streamlit as st
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

REQUIRED_ENV_VARS: Sequence[str] = (
    "MSSQL_SERVER",
    "MSSQL_DATABASE",
    "MSSQL_USERNAME",
    "MSSQL_PASSWORD",
)

NUMERIC_SQL_TYPES = {
    "bigint",
    "decimal",
    "float",
    "int",
    "money",
    "numeric",
    "real",
    "smallint",
    "smallmoney",
    "tinyint",
    "bit",
}

TEMPORAL_SQL_TYPES = {
    "date",
    "datetime",
    "datetime2",
    "datetimeoffset",
    "smalldatetime",
    "time",
}

AGGREGATION_OPTIONS = {
    "Average": "mean",
    "Sum": "sum",
    "Median": "median",
    "Minimum": "min",
    "Maximum": "max",
    "Count rows": "size",
}

CHART_TYPE_MARKS = {
    "Line": "line",
    "Area": "area",
    "Bar": "bar",
    "Scatter": "point",
}


def normalize_odbc_boolean(value: str | None, default: str) -> str:
    """Return 'yes' or 'no' for ODBC boolean attributes."""
    if value is None:
        return default
    normalized = value.strip().lower()
    truthy = {"1", "true", "yes", "y", "on"}
    falsy = {"0", "false", "no", "n", "off"}
    if normalized in truthy:
        return "yes"
    if normalized in falsy:
        return "no"
    return default


def build_connection_url() -> str:
    """Construct the SQLAlchemy URL based on environment variables."""
    missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
    if missing:
        plural = "s" if len(missing) > 1 else ""
        raise RuntimeError(
            f"Missing environment variable{plural}: {', '.join(sorted(missing))}"
        )

    driver = quote_plus(os.getenv("MSSQL_DRIVER", os.getenv("SQL_DRIVER", "ODBC Driver 18 for SQL Server")))
    encrypt = normalize_odbc_boolean(os.getenv("SQL_ENCRYPT"), "yes")
    trust_server_certificate = normalize_odbc_boolean(
        os.getenv("TRUST_SERVER_CERTIFICATE", os.getenv("SQL_TRUST_SERVER_CERTIFICATE")), "no"
    )
    extra_parameters = os.getenv("SQL_ODBC_EXTRA", "")

    username = quote_plus(os.environ["MSSQL_USERNAME"])
    password = quote_plus(os.environ["MSSQL_PASSWORD"])
    server = os.environ["MSSQL_SERVER"]
    database = os.environ["MSSQL_DATABASE"]
    port = os.getenv("MSSQL_PORT", os.getenv("SQL_PORT"))

    # Connection options become part of the query string appended to the URL.
    query_params = [
        f"driver={driver}",
        f"Encrypt={encrypt}",
        f"TrustServerCertificate={trust_server_certificate}",
    ]
    if extra_parameters:
        query_params.append(extra_parameters.lstrip("&"))

    server_target = f"{server}:{port}" if port else server
    return (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server_target}/{database}?{'&'.join(query_params)}"
    )


@st.cache_resource(show_spinner=False)
def get_engine(url: str) -> Engine:
    """Create a cached SQLAlchemy engine so Streamlit reuses the connection pool."""
    return create_engine(url, pool_pre_ping=True)


def verify_connection(url: str) -> None:
    """Run a lightweight probe to confirm the database connection works."""
    engine = get_engine(url)
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))


@st.cache_data(show_spinner=False)
def run_query(url: str, sql: str) -> pd.DataFrame:
    """Execute the user's query and return a DataFrame with the results."""
    engine = get_engine(url)
    with engine.connect() as connection:
        return pd.read_sql(text(sql), connection)


@st.cache_data(show_spinner=False)
def load_schema_metadata(url: str) -> pd.DataFrame:
    """Retrieve column-level metadata to power the schema explorer."""
    schema_sql = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            c.name AS column_name,
            ty.name AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity,
            OBJECTPROPERTY(t.object_id, 'TableHasPrimaryKey') AS has_primary_key,
            dc.definition AS column_default,
            c.column_id
        FROM sys.schemas AS s
        JOIN sys.tables AS t
            ON t.schema_id = s.schema_id
        JOIN sys.columns AS c
            ON c.object_id = t.object_id
        JOIN sys.types AS ty
            ON ty.user_type_id = c.user_type_id
        LEFT JOIN sys.default_constraints AS dc
            ON dc.parent_object_id = c.object_id
           AND dc.parent_column_id = c.column_id
        WHERE t.is_ms_shipped = 0
        ORDER BY s.name, t.name, c.column_id;
    """
    engine = get_engine(url)
    with engine.connect() as connection:
        return pd.read_sql(text(schema_sql), connection)


@st.cache_data(show_spinner=False)
def load_table_sample(url: str, schema_name: str, table_name: str, limit: int) -> pd.DataFrame:
    """Fetch a limited sample from the requested table for visualization purposes."""
    limit = max(1, int(limit))
    qualified_name = f"[{schema_name}].[{table_name}]"
    sample_sql = text(f"SELECT TOP ({limit}) * FROM {qualified_name}")
    engine = get_engine(url)
    with engine.connect() as connection:
        return pd.read_sql(sample_sql, connection)


def infer_column_roles(table_catalog: pd.DataFrame) -> dict[str, list[str]]:
    """Classify table columns into numeric, temporal, and categorical roles."""
    numeric: list[str] = []
    temporal: list[str] = []
    categorical: list[str] = []

    for entry in table_catalog.itertuples():
        data_type = str(entry.data_type).lower()
        column_name = entry.column_name
        if data_type in NUMERIC_SQL_TYPES:
            numeric.append(column_name)
        elif data_type in TEMPORAL_SQL_TYPES:
            temporal.append(column_name)
        else:
            categorical.append(column_name)

    return {
        "numeric": sorted(set(numeric)),
        "temporal": sorted(set(temporal)),
        "categorical": sorted(set(categorical)),
    }


def prepare_chart_dataset(
    source: pd.DataFrame,
    x_column: str,
    metric_column: str | None,
    aggregator: str,
    group_column: str | None,
    temporal_columns: Sequence[str],
) -> pd.DataFrame:
    """Aggregate the raw sample into a dataset ready for visualization."""
    working = source.copy()
    if x_column not in working.columns:
        raise ValueError("The selected X-axis column is not available in the data sample.")

    if group_column and group_column not in working.columns:
        raise ValueError("The selected grouping column is not available in the data sample.")

    if aggregator != "Count rows":
        if not metric_column:
            raise ValueError("Choose a metric column to aggregate.")
        if metric_column not in working.columns:
            raise ValueError("The selected metric column is not available in the data sample.")
        working["_metric"] = pd.to_numeric(working[metric_column], errors="coerce")
        working = working.dropna(subset=[x_column, "_metric"])
    else:
        working = working.dropna(subset=[x_column])

    if x_column in temporal_columns:
        working[x_column] = pd.to_datetime(working[x_column], errors="coerce")
        working = working.dropna(subset=[x_column])

    if working.empty:
        raise ValueError("No rows remain after aligning the dataset with the selected options.")

    group_fields = [x_column]
    if group_column:
        group_fields.append(group_column)

    if aggregator == "Count rows":
        aggregated = (
            working.groupby(group_fields, dropna=False)
            .size()
            .reset_index(name="value")
        )
    else:
        pandas_agg = AGGREGATION_OPTIONS[aggregator]
        aggregated = (
            working.groupby(group_fields, dropna=False)["_metric"]
            .agg(pandas_agg)
            .reset_index(name="value")
        )

    if x_column in temporal_columns:
        aggregated = aggregated.sort_values(x_column)

    return aggregated


# ---------------------------------------------------------------------------
# User experience configuration
# ---------------------------------------------------------------------------

# Provide a curated onboarding query that matches the SOPRA CRUD project.
DEFAULT_QUERY: str = (
    "SELECT TOP (10)\n"
    "    RabattID,\n"
    "    Kunde,\n"
    "    MengeVon,\n"
    "    MengeBis,\n"
    "    RabattProzent,\n"
    "    GiltVon,\n"
    "    GiltBis\n"
    "FROM list_views.V_LIST_B2B_DISCOUNT\n"
    "ORDER BY RabattID DESC;"
)

# Curated examples available from the sidebar to speed up exploration.
SAMPLE_QUERIES: dict[str, str] = {
    "SOPRA latest discounts": DEFAULT_QUERY,
    "SOPRA customer dropdown values": (
        "SELECT TOP (25)\n"
        "    CUSTOMER_ID,\n"
        "    CUSTOMER_LONG\n"
        "FROM dbo.LOV_CUSTOMER\n"
        "ORDER BY CUSTOMER_ID;"
    ),
    "SOPRA active discounts": (
        "SELECT TOP (25)\n"
        "    RabattID,\n"
        "    Kunde,\n"
        "    MengeVon,\n"
        "    MengeBis,\n"
        "    RabattProzent,\n"
        "    GiltVon,\n"
        "    GiltBis\n"
        "FROM list_views.V_LIST_B2B_DISCOUNT\n"
        "WHERE GiltVon <= CAST(GETDATE() AS date)\n"
        "  AND (GiltBis IS NULL OR GiltBis >= CAST(GETDATE() AS date))\n"
        "ORDER BY Kunde, MengeVon;"
    ),
    "Available base tables": (
        "SELECT TOP (25)\n"
        "    TABLE_SCHEMA,\n"
        "    TABLE_NAME,\n"
        "    TABLE_TYPE\n"
        "FROM INFORMATION_SCHEMA.TABLES\n"
        "ORDER BY TABLE_SCHEMA, TABLE_NAME;"
    ),
}

# Keys used for Streamlit session state so values survive reruns.
SESSION_QUERY_KEY = "query_text"
SESSION_HISTORY_KEY = "query_history"
SESSION_SAMPLE_KEY = "sample_query_choice"


def initialize_session_state(default_sql: str) -> None:
    """Seed Streamlit session state with structured defaults for first-time visitors."""
    if SESSION_QUERY_KEY not in st.session_state:
        st.session_state[SESSION_QUERY_KEY] = default_sql
    if SESSION_HISTORY_KEY not in st.session_state:
        st.session_state[SESSION_HISTORY_KEY] = []
    if SESSION_SAMPLE_KEY not in st.session_state:
        st.session_state[SESSION_SAMPLE_KEY] = next(iter(SAMPLE_QUERIES))


def set_sample_query() -> None:
    """Update the SQL editor with the curated sample selected in the sidebar."""
    choice = st.session_state.get(SESSION_SAMPLE_KEY)
    if choice:
        st.session_state[SESSION_QUERY_KEY] = SAMPLE_QUERIES[choice]


def reset_query_editor() -> None:
    """Restore the SQL editor to the original onboarding query."""
    st.session_state[SESSION_QUERY_KEY] = DEFAULT_QUERY


def render_sidebar_controls(connection_ready: bool, error_text: str | None) -> dict[str, Any]:
    """Render sidebar elements and return the configuration chosen by the analyst."""
    st.sidebar.header("Connection overview", divider="blue")
    if connection_ready:
        st.sidebar.success("Connected to Microsoft SQL Server.")
    else:
        st.sidebar.error("Connection unavailable.")
        if error_text:
            st.sidebar.code(error_text)
        st.sidebar.info(
            "Verify the credentials in `.env` or update your VPN/firewall configuration."
        )

    st.sidebar.header("Query controls", divider="blue")
    row_limit = int(
        st.sidebar.number_input(
            "Maximum rows to display",
            min_value=10,
            max_value=10_000,
            value=500,
            step=10,
            help="Rows above this limit remain downloadable but are hidden from the grid.",
            disabled=not connection_ready,
        )
    )
    show_metrics = st.sidebar.checkbox(
        "Show result metrics",
        value=True,
        help="Display key performance indicators (rows, columns, execution time).",
        disabled=not connection_ready,
    )
    show_profiler = st.sidebar.checkbox(
        "Show data profiler",
        value=False,
        help="Calculates summary statistics for visible columns.",
        disabled=not connection_ready,
    )
    track_history = st.sidebar.checkbox(
        "Track query history",
        value=True,
        help="Maintain a private log of executed statements in this session.",
    )

    st.sidebar.selectbox(
        "Curated SQL playbook",
        options=list(SAMPLE_QUERIES.keys()),
        key=SESSION_SAMPLE_KEY,
        on_change=set_sample_query,
        help="Load a vetted example query tailored for operational monitoring.",
        disabled=not connection_ready,
    )
    st.sidebar.button(
        "Reset editor to default query",
        on_click=reset_query_editor,
        disabled=not connection_ready,
    )

    history_entries: list[dict[str, Any]] = st.session_state.get(SESSION_HISTORY_KEY, [])
    if history_entries:
        with st.sidebar.expander("Recent queries", expanded=False):
            if not track_history:
                st.caption(
                    "History tracking is paused. Toggle it back on to capture future runs."
                )
            for entry in history_entries:
                st.markdown(f"**{entry['timestamp']}** — {entry['row_count_displayed']} rows · {entry['duration_ms']:.0f} ms")
                st.code(entry["query"], language="sql")

    st.sidebar.divider()
    st.sidebar.markdown(
        "Need onboarding documentation? Visit the internal data platform portal or "
        "[review the uv setup guide](https://github.com/kirenz/uv-setup)."
    )

    return {
        "row_limit": row_limit,
        "show_metrics": show_metrics,
        "show_profiler": show_profiler,
        "track_history": track_history,
    }


# ---------------------------------------------------------------------------
# Page rendering
# ---------------------------------------------------------------------------

st.set_page_config(page_title="SOPRA SQL Server Workspace", layout="wide")
initialize_session_state(DEFAULT_QUERY)

connection_url: str | None = None
error_message: str | None = None
connection_ready = False

try:
    connection_url = build_connection_url()
    verify_connection(connection_url)
    connection_ready = True
except (RuntimeError, SQLAlchemyError) as exc:
    error_message = str(exc)

sidebar_preferences = render_sidebar_controls(connection_ready, error_message)

st.title("SOPRA SQL Server Workspace")
st.caption(
    "Small Streamlit workspace for testing read queries against the SOPRA SQL Server database."
)

if not connection_ready:
    st.warning(
        "Connection failed. Update your credentials or network access and rerun the app."
    )
    if error_message:
        st.code(error_message)
    st.stop()

schema_catalog = load_schema_metadata(connection_url)
if not schema_catalog.empty:
    schema_catalog = schema_catalog.assign(
        is_nullable=schema_catalog["is_nullable"].astype(bool),
        is_identity=schema_catalog["is_identity"].astype(bool),
        has_primary_key=schema_catalog["has_primary_key"].astype(bool),
    )
    table_summary = (
        schema_catalog.groupby(["schema_name", "table_name"], as_index=False)
        .agg(
            column_count=("column_name", "count"),
            has_primary_key=("has_primary_key", "max"),
        )
        .sort_values(["schema_name", "table_name"])
    )
    table_summary["has_primary_key"] = table_summary["has_primary_key"].astype(bool)
    schema_names = sorted(schema_catalog["schema_name"].unique())
else:
    table_summary = pd.DataFrame()
    schema_names: list[str] = []

schema_choices = ["All schemas"] + schema_names
chart_schema_choices = ["Select schema"] + schema_names

query_tab, schema_tab, analytics_tab = st.tabs(
    ["Query workspace", "Schema explorer", "Visual analytics"]
)

with query_tab:
    st.subheader("Query workspace")
    st.markdown(
        "Use the curated SOPRA queries from the sidebar or paste your own statement. "
        "All executions are limited to the privileges of your database user."
    )

    with st.form("sql-form", clear_on_submit=False):
        st.text_area(
            "SQL editor",
            key=SESSION_QUERY_KEY,
            height=260,
            help="The editor preserves state across reruns so you can iterate quickly.",
        )
        query_submitted = st.form_submit_button("Run query", type="primary")

    if query_submitted:
        query_to_run = st.session_state[SESSION_QUERY_KEY].strip()
        if not query_to_run:
            st.warning("Enter a SQL statement before running the query.")
        else:
            execution_start = time.perf_counter()
            try:
                result = run_query(connection_url, query_to_run)
            except SQLAlchemyError as exc:
                st.error("The database returned an error. Review the message below.")
                st.code(str(exc))
            else:
                execution_time_ms = (time.perf_counter() - execution_start) * 1000
                row_limit = sidebar_preferences["row_limit"]
                trimmed_result = result.head(row_limit)

                st.success(f"Query executed successfully. Retrieved {len(result)} total row(s).")
                st.dataframe(trimmed_result, width="stretch")

                if len(result) > row_limit:
                    st.info(
                        f"Displaying the first {row_limit} row(s). Download the full result set for complete data."
                    )

                if sidebar_preferences["show_metrics"]:
                    metric_cols = st.columns(3)
                    metric_cols[0].metric("Rows displayed", len(trimmed_result))
                    metric_cols[1].metric("Columns", len(result.columns))
                    metric_cols[2].metric("Execution time (ms)", f"{execution_time_ms:.0f}")

                csv_data = result.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Download results as CSV",
                    data=csv_data,
                    file_name="query_results.csv",
                    mime="text/csv",
                )

                if sidebar_preferences["show_profiler"] and not trimmed_result.empty:
                    try:
                        profile = trimmed_result.describe(include="all").transpose()
                    except ValueError:
                        profile = pd.DataFrame()
                    if not profile.empty:
                        st.subheader("Data profiler")
                        st.caption("Summary statistics calculated on the displayed rows.")
                        st.dataframe(profile, width="stretch")
                    else:
                        st.caption(
                            "Data profiler could not compute summary statistics for the returned dataset."
                        )

                if sidebar_preferences["track_history"]:
                    history_entry = {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "row_count": len(result),
                        "row_count_displayed": len(trimmed_result),
                        "duration_ms": execution_time_ms,
                        "query": query_to_run,
                    }
                    st.session_state[SESSION_HISTORY_KEY].insert(0, history_entry)
                    st.session_state[SESSION_HISTORY_KEY] = st.session_state[SESSION_HISTORY_KEY][:10]

with schema_tab:
    st.subheader("Schema explorer")
    st.markdown(
        "Understand the shape of your data warehouse before writing queries. "
        "Filter by schema and table to review column definitions, data types, and defaults."
    )

    if schema_catalog.empty:
        st.info("No user tables were detected. Confirm your database permissions.")
    else:
        metric_columns = st.columns(3)
        metric_columns[0].metric("Schemas", schema_catalog["schema_name"].nunique())
        metric_columns[1].metric(
            "Tables", schema_catalog[["schema_name", "table_name"]].drop_duplicates().shape[0]
        )
        metric_columns[2].metric("Columns", schema_catalog.shape[0])

        selected_schema = st.selectbox(
            "Schema",
            options=schema_choices,
            help="Select a schema to focus the catalog view.",
        )

        filtered_summary = table_summary.copy()
        if selected_schema != "All schemas":
            filtered_summary = filtered_summary[filtered_summary["schema_name"] == selected_schema]

        table_choices = ["All tables"] + [
            f"{row.schema_name}.{row.table_name}"
            for row in filtered_summary.itertuples()
        ]
        selected_table = st.selectbox(
            "Table",
            options=table_choices,
            help="Choose a specific table to inspect its column definitions.",
        )

        st.caption("Table catalog")
        st.dataframe(
            filtered_summary.rename(
                columns={
                    "schema_name": "Schema",
                    "table_name": "Table",
                    "column_count": "Columns",
                    "has_primary_key": "Primary key",
                }
            ),
            hide_index=True,
            width="stretch",
        )

        if selected_table != "All tables":
            schema_name, table_name = selected_table.split(".", maxsplit=1)
            column_details = schema_catalog[
                (schema_catalog["schema_name"] == schema_name)
                & (schema_catalog["table_name"] == table_name)
            ].copy()
        elif selected_schema != "All schemas":
            column_details = schema_catalog[schema_catalog["schema_name"] == selected_schema].copy()
        else:
            column_details = schema_catalog.copy()

        column_details = column_details[
            [
                "schema_name",
                "table_name",
                "column_name",
                "data_type",
                "max_length",
                "precision",
                "scale",
                "is_nullable",
                "is_identity",
                "column_default",
            ]
        ].rename(
            columns={
                "schema_name": "Schema",
                "table_name": "Table",
                "column_name": "Column",
                "data_type": "Data type",
                "max_length": "Max length",
                "precision": "Precision",
                "scale": "Scale",
                "is_nullable": "Nullable",
                "is_identity": "Identity",
                "column_default": "Default value",
            }
        )

        column_filter = (
            st.text_input(
                "Column filter",
                value="",
                placeholder="Search by column name, data type, or default value.",
                help="Case-insensitive filter applied to the column metadata below.",
            ).strip()
        )
        if column_filter:
            lowered = column_filter.lower()
            column_details = column_details[
                column_details.apply(
                    lambda row: lowered in " ".join(row.astype(str).tolist()).lower(), axis=1
                )
            ]

        st.caption("Column definitions")
        st.dataframe(column_details, hide_index=True, width="stretch")

        schema_csv = column_details.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download schema as CSV",
            data=schema_csv,
            file_name="schema_catalog.csv",
            mime="text/csv",
        )

with analytics_tab:
    st.subheader("Visual analytics")
    st.markdown(
        "Assemble executive-ready charts without leaving the workspace. "
        "Select a schema, table, and aggregation settings to visualize key metrics."
    )

    if schema_catalog.empty:
        st.info("Schema metadata is unavailable, so charting cannot be initialized.")
    else:
        schema_selection = st.selectbox(
            "Schema for analysis",
            options=chart_schema_choices,
            help="Choose the schema that contains the table you want to visualize.",
        )

        if schema_selection == "Select schema":
            st.info("Select a schema to surface charting options.")
        else:
            available_tables = table_summary[
                table_summary["schema_name"] == schema_selection
            ]["table_name"].tolist()
            table_options = ["Select table"] + available_tables
            table_selection = st.selectbox(
                "Table",
                options=table_options,
                help="Pick the table that includes the measures or dimensions you need.",
            )

            if table_selection == "Select table":
                st.info("Select a table to continue configuring the chart.")
            else:
                table_catalog = schema_catalog[
                    (schema_catalog["schema_name"] == schema_selection)
                    & (schema_catalog["table_name"] == table_selection)
                ].copy()

                column_roles = infer_column_roles(table_catalog)
                dimension_options = sorted(
                    set(column_roles["temporal"] + column_roles["categorical"])
                )
                metric_options = column_roles["numeric"]
                if not dimension_options:
                    st.warning(
                        "The selected table does not expose columns suitable for the X-axis. "
                        "Choose a different table or request additional metadata."
                    )
                else:
                    chart_type = st.selectbox(
                        "Chart type",
                        options=list(CHART_TYPE_MARKS.keys()),
                        help="Choose how the aggregated data should be visualized.",
                    )

                    aggregation_choices = list(AGGREGATION_OPTIONS.keys())
                    if not metric_options:
                        aggregation_choices = ["Count rows"]

                    aggregator = st.selectbox(
                        "Aggregation",
                        options=aggregation_choices,
                        help="Select the aggregation strategy applied to the metric.",
                    )

                    x_axis = st.selectbox(
                        "X-axis dimension",
                        options=dimension_options,
                        help="Pick the column that provides the timeline or categorical grouping on the X-axis.",
                    )

                    needs_metric = aggregator != "Count rows"
                    metric_selection: str | None = None
                    chart_ready = True
                    if needs_metric:
                        if not metric_options:
                            st.warning(
                                "No numeric columns were found in this table, so aggregation is unavailable."
                            )
                            chart_ready = False
                        else:
                            metric_selection = st.selectbox(
                                "Metric column",
                                options=metric_options,
                                help="Numeric column that will be aggregated according to the selected strategy.",
                            )

                    group_candidates = ["None"] + [
                        column for column in column_roles["categorical"] if column != x_axis
                    ]
                    group_selection = st.selectbox(
                        "Color grouping",
                        options=group_candidates,
                        help="Optional dimension to split the series by color for comparison.",
                    )

                    row_limit = st.slider(
                        "Rows to sample",
                        min_value=200,
                        max_value=10_000,
                        value=2_000,
                        step=200,
                        help="The workspace loads a limited sample to keep visualizations responsive.",
                    )

                    if not chart_ready:
                        st.info("Adjust the aggregation or metric selection to generate a chart.")
                    else:
                        with st.spinner("Loading sample data for visualization..."):
                            table_sample = load_table_sample(
                                connection_url, schema_selection, table_selection, row_limit
                            )

                        if table_sample.empty:
                            st.info("No rows were retrieved from the selected table using the configured sample size.")
                        else:
                            group_column = None if group_selection == "None" else group_selection
                            try:
                                aggregated = prepare_chart_dataset(
                                    table_sample,
                                    x_axis,
                                    metric_selection,
                                    aggregator,
                                    group_column,
                                    column_roles["temporal"],
                                )
                            except ValueError as exc:
                                st.warning(str(exc))
                            else:
                                y_title = (
                                    "Row count"
                                    if aggregator == "Count rows"
                                    else f"{aggregator} of {metric_selection}"
                                )

                                base_chart = alt.Chart(aggregated)
                                mark_type = CHART_TYPE_MARKS[chart_type]
                                if mark_type == "line":
                                    chart_obj = base_chart.mark_line(point=True)
                                elif mark_type == "area":
                                    chart_obj = base_chart.mark_area(opacity=0.7)
                                elif mark_type == "bar":
                                    chart_obj = base_chart.mark_bar()
                                else:
                                    chart_obj = base_chart.mark_point(size=70, filled=True)

                                encoding_kwargs: dict[str, Any] = {
                                    "x": alt.X(
                                        field=x_axis,
                                        type="temporal" if x_axis in column_roles["temporal"] else "nominal",
                                        title=x_axis,
                                    ),
                                    "y": alt.Y(
                                        field="value",
                                        type="quantitative",
                                        title=y_title,
                                    ),
                                    "tooltip": [
                                        alt.Tooltip(x_axis, title=x_axis),
                                        alt.Tooltip("value", title=y_title),
                                    ],
                                }

                                if group_column:
                                    encoding_kwargs["color"] = alt.Color(
                                        field=group_column,
                                        type="nominal",
                                        title=group_column,
                                    )
                                    encoding_kwargs["tooltip"].insert(
                                        1, alt.Tooltip(group_column, title=group_column)
                                    )

                                chart = chart_obj.encode(**encoding_kwargs).properties(height=420)
                                st.altair_chart(chart, width="stretch")

                                aggregated_preview = aggregated.head(100)
                                st.caption("Aggregated dataset preview")
                                st.dataframe(aggregated_preview, hide_index=True, width="stretch")

                                download_name = f"{schema_selection}_{table_selection}_{aggregator.replace(' ', '_').lower()}_{chart_type.lower()}.csv"
                                st.download_button(
                                    label="Download aggregated data as CSV",
                                    data=aggregated.to_csv(index=False).encode("utf-8"),
                                    file_name=download_name,
                                    mime="text/csv",
                                )

                                with st.expander("Sample data (first 50 rows)"):
                                    st.dataframe(table_sample.head(50), hide_index=True, width="stretch")
