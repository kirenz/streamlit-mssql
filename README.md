# SQL Server Streamlit Example

Use this repository to learn how to build a Streamlit interface against Microsoft SQL Server with the help of `uv`.

> [!IMPORTANT]
> You need to have uv installed on your machine (go to [this repo](https://github.com/kirenz/uv-setup) for installation instructions).

## Step-by-step instructions

If you are on macOS, open the built-in **Terminal** app. On Windows, open **Git Bash**.

1. Clone the repository

   ```bash
   git clone https://github.com/kirenz/streamlit-mssql.git
   ```

   Change into the repository folder

   ```bash
   cd streamlit-mssql
   ```

2. Sync the Python environment defined in `pyproject.toml`

   ```bash
   uv sync
   ```

   This installs Streamlit, the SQL Server drivers, and all other required packages in an isolated environment managed by `uv`.

3. Prepare your environment variables (this will copy the example file and create a new `.env` file)

   ```bash
   cp .env.example .env
   ```

4. Open VS Code in the current folder

   ```bash
   code .
   ```

   You can also open the folder manually from within VS Code.

   Open the new `.env` file and replace the placeholder values with the SQL Server hostname, database, username, password, driver, and optional port provided by your instructor.

5. Launch the Streamlit app (you may use the integrated terminal in VS Code or your previous terminal window)

   ```bash
   uv run streamlit run app.py
   ```

   If you are asked to provide your email, you can simply press Enter to skip this step.

   Streamlit prints a local URL (typically `http://localhost:8501`). Open it in your browser to load the app.

6. Test a SOPRA query

   The app includes sample `SELECT` statements for `list_views.V_LIST_B2B_DISCOUNT` and `dbo.LOV_CUSTOMER`. Click **Run query** to verify the connection and permissions. Update the SQL text area to explore other objects you have access to and rerun the command above.



## Files

- `app.py` – Streamlit app that authenticates with SQL Server, runs SOPRA-oriented ad-hoc queries, and displays the results in a table.
- `.env.example` – template with placeholders for your connection parameters. Copy this to `.env` and update the values before running the app.
- `pyproject.toml` – dependency definition for `uv sync`. Don't edit this file directly; use `uv add <package>` to add new packages.

## Python packages used

- `streamlit` – renders the browser-based user interface.
- `sqlalchemy` – builds the connection engine atop the ODBC driver and offers a composable SQL toolkit.
- `pyodbc` – provides the ODBC driver bindings that let Python talk to Microsoft SQL Server.
- `pandas` – turns query results into DataFrames so you can inspect and manipulate the data comfortably.
- `python-dotenv` – reads connection values from the `.env` file into environment variables before the app runs.
