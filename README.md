# Sales Pipeline ETL & Tableau/Power BI Dashboard: GearQuest Analysis

This project demonstrates the process of extracting, transforming, and loading (ETL) data from Excel files into both a `.hyper` Tableau extract file and a PostgreSQL database. This facilitates seamless data integration within Tableau and Power BI, enabling enhanced business intelligence visualizations and dashboard creation.

**Key Highlights/Features:**

*   **End-to-End ETL Pipeline:** Implements a complete ETL process from Excel data sources to Tableau Hyper files and PostgreSQL databases.
*   **Dual Output:** Generates data outputs suitable for both Tableau and Power BI platforms, showcasing versatility.
*   **Automated Data Transformation:** Leverages `pyspark` for efficient and scalable data cleaning, transformation, and integration.
*   **Cloud-Ready:** Uses a modular design that can be adapted to cloud environments such as AWS, GCP, or Azure.
*   **Reproducible:** Fully automated setup and execution, leveraging a `requirements.txt` and `.env` file for ease of use.
*   **Interactive Dashboards:** Provides pre-built interactive dashboards in Tableau and Power BI for immediate data exploration and insights.


## Tableau Dashboard

📊 **[View Interactive Dashboard](https://public.tableau.com/views/gearquest/Dashboard?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)** - Explore the live dashboard on Tableau Public

The Tableau dashboard was connected to a `.hyper` file populated by this ETL process.

![GearQuest Sales Dashboard](dashboard_2_tableau.png)

## Power BI Dashboard

📊 **[View Interactive Dashboard](https://app.powerbi.com/view?r=eyJrIjoiNmYyZjMyNWMtYmE1OS00MjRlLWIxMzAtZjJlMmU4ZWQ4NmFhIiwidCI6IjVkNGNmODgzLTJlMzQtNGZlNi04ZDExLWE0ZWE5NTk0ZTQ0YyIsImMiOjF9)** - Explore the live dashboard on Power BI

The Power BI dashboard was connected to a PostgreSQL database populated by this ETL process.

![GearQuest Sales Dashboard](dashboard_1_power_bi.png)

---

## Project Structure

### **Data Pipeline**

This project includes two main pipelines:

1.  **Hyper Pipeline (`xlsx_to_hyper.py`):**
    *   **Data Extraction**: Extracts data from multiple Excel files (Orders, Order Status, Products, Geo Lookup, and Customers) using `xlsx2csv`.
    *   **Data Transformation**: Uses `pyspark` to perform transformations such as dropping duplicates and joining tables.
    *   **Data Loading**: Loads the transformed data into a Tableau Hyper extract file (`.hyper`) using the `tableauhyperapi` library. The data is first converted to CSV files using Spark, and then ingested into Hyper.

2.  **PostgreSQL Pipeline (`xlsx_to_postgres.py`):**
    *   **Data Extraction**: Extracts data from multiple Excel files (Orders, Order Status, Products, Geo Lookup, and Customers) using `xlsx2csv`.
    *   **Data Transformation**: Uses `pyspark` to perform transformations such as dropping duplicates and joining tables.
    *   **Data Loading**: Loads the transformed data into a PostgreSQL database using `psycopg2` and Spark's JDBC capabilities.

The original development notebook (`etl_to_hyper_postgres.ipynb`), used to create the final pipelines, is available in the `pipelines` folder.

### **Key Steps**

-   **Data Loading**: Loads data from Excel files into Spark DataFrames.
-   **Combining Tables**: Merges related tables (Orders + Order Status, Customers + Geo Lookup) to create combined DataFrames.
-   **Data Type Handling**: Inspects and adjusts data types as necessary for compatibility with Tableau Hyper and PostgreSQL formats. Converts relevant columns to Decimal.
-   **Export to .hyper**: Creates a `.hyper` file and inserts the transformed data into the respective tables using `xlsx_to_hyper.py`.
-   **Export to PostgreSQL**: Connects to a PostgreSQL database and overwrites the transformed data into the respective tables using `xlsx_to_postgres.py`.

---

## Installation and Setup

1.  **Clone the repository:**

    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Create a virtual environment (optional but recommended):**

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

    Additionally, you need to install the PostgreSQL JDBC driver for Spark to interact with the PostgreSQL database. Download the appropriate JAR file (e.g., `postgresql-<version>.jdbc42.jar`) from the [pgJDBC Download Page](https://mvnrepository.com/artifact/org.postgresql/postgresql) and place it in the `$SPARK_HOME/jars` directory.  If `$SPARK_HOME` is not set, you can place the jar in a location of your choosing, but you will need to ensure that Spark can find it.

    Alternatively, you can specify the driver when running the spark jobs:
    ```
    spark-submit --driver-class-path /path/to/postgresql-<version>.jdbc42.jar  ...
    ```

4.  **Set up PostgreSQL:**

    -   Ensure you have PostgreSQL installed and running.
    -   Create a database named `gearquest` (or configure the `.env` file accordingly).

5.  **Configure Environment Variables:**

    -   Create a `.env` file in the project root directory.
    -   Add the following environment variables, replacing the values with your PostgreSQL credentials:

        ```
        PG_HOST=<your_postgres_host>
        PG_USER=<your_postgres_user>
        PG_PASSWORD=<your_postgres_password>
        PG_DATABASE=<your_postgres_database>
        ```

    **Note:** The `.env` file is used to securely store your database credentials. Make sure to add `.env` to your `.gitignore` file to prevent committing your credentials to a public repository.

6.  **Run the pipelines:**

    To run the Hyper pipeline:

    ```bash
    python pipelines/xlsx_to_hyper.py
    ```

    To run the PostgreSQL pipeline:

    ```bash
    python pipelines/xlsx_to_postgres.py
    ```

---

## PostgreSQL Export Details

The `xlsx_to_postgres.py` script exports the transformed data to a PostgreSQL database. The following steps are performed:

1.  **Connect to PostgreSQL:** The script uses the `psycopg2` library to connect to the PostgreSQL database using the credentials specified in the `.env` file.
2.  **Create Tables:** If the tables (`orders`, `customers`, `products`) do not exist, they are created with the appropriate schema.
3.  **Overwrite Data:** The script overwrites the data in the tables.

---

## Data Source

The data used in this project is sourced from the [GearQuest Sales and Customer Insights Dataset on Kaggle](https://www.kaggle.com/datasets/dipunmohapatra/gearquest-sales-and-customer-insights-dataset). The dataset consists of several Excel files: Orders.xlsx, Order\_Status.xlsx, Products.xlsx, Geo\_lookup.xlsx, and Customers.xlsx. These files should be placed in the `data/raw/` directory. The pipelines read directly from these files.

**Note:** These files are assumed to be available and are not included in this repository. Please download them from the Kaggle link provided.

---

## Interactive Dashboards

The goal of this project is to enable the creation of interactive dashboards in both Tableau and Power BI. The Tableau dashboard utilizes the generated `.hyper` file, while the Power BI dashboard connects directly to the PostgreSQL database. These dashboards provide insights into the GearQuest sales pipeline and related data.