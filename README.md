# Porfolio
Greetings! My name is Neil, a data enthusiast. Welcome to my Github repository showcasing a compilation of major projects reflecting my journey and expertise in the realm of business intelligence and data engineering. Within this repositoy, you will find a diversity of projects I have put together throughout my data professional career, from building robust data pipelines, designing databased to crafting insightful business dashboards. Wether you're a fellow data enthusiast, a potential employer, or simply curios about the possibilities of data, please feel free to explore and reach out with any questions or opportunities for collaboration. 🙂

<details>

<summary>TICKETING PLATFORM DATA PIPELINES</summary>

### 1. Background:
This is a project I did for a company specializing in online ticket sales, catering to sports and recreational events. While the initial database architecture effectively supported the platform's backend operations, this setup wasn't designed with analytical purposes in mind. Not only did data were stored scatteringly, but also the naming convention was a disaster. This lack of a standard data model hindered the company's ability to analyze the data and derive meaningful insights from it. 

**`In a nutshell, my mission was to reshape the data and make it usuable for analytics`**. 

### 2. Objectives:
#### Objective 1: Put together an OLAP database which: 
- stakeholders can run ad-hoc queries against.
- is the data source of the business dashboard.
- is the foundation to build a CRM system.
#### Objective 2: Build a robust and automated data pipeline that refreshes the data on a daily basis.
#### Objective 3: Build a business dashboard which:
- tracks daily sales performace by monitoring sales KPIs and key financial metrics.
- presents an analysis of customers behaviour and segmentation.
  
### 3. Constrains & Challenges:
It's worth metioning the constraints and challenges I encountered before and during the project because they are the decisive factors of the approach I adopted.

First among them was the dependency on the head DevOps of the company for the access to the raw data. In particular, I was restricted from directly acessing the company's transactional database stored in MySQL. Instead, the head DevOps would dump each entire table from the database into JSON files, before uploading them to an AWS S3 bucket and overwriting the existing files. This constrain made micro-batch processing and incremental loading almost impossible. I needed to come up with a workaround that had to be memory-efficient and scalable when the size of those JSON files grew.

The second most significant constrain I encountered was that the new data would only arrive in the S3 bucket once a day at midnight, so theoretically the data I would receive was only up to 23:59:59 the day before. This was definitely something I had to work with the stakeholders to manage their expectations.

Last on the list is the communication with the stakeholders, which is a common challenge faced by many other data engineers.  

### 4. Approach:
#### 4.1. Planning:

#### 4.2. Extract:
##### As mentioned above, the data source I was working with is a collection of JSON files in an AWS S3 bucket, some of them were heavily nested. I had two options:
- Build a fully managed pipeline with AWS Glue and AWS Crawler, then create a databse using AWS Athena. Or if I had wanted to have more flexibility, I could have built used AWS Lambda function.
- Code a pipeline with Python and open-source tools.
  
##### I did try both of them, and finally decided to go the second option because:
- I was going to use Power BI to build a business dashboard from the cleaned data, and set up a daily schedule refresh to automatically refresh the dashboard underlying dataset. To achieve this, I needed to either install the Power BI gateway on the host operating system (OS) to which I would later deploy my pipeline or utilize a cloud-based data source such as Google BigQuery or Snowflake. Unfortunately, the Power BI gateway is only compatible with Windows OS, whereas my pipeline would be containerized within a Docker container running on Linux. Consequently, I had to opt for a serverless data warehouse as the target database for this project. While I favor Athena for its robust distributed Presto SQL engine, I had to exclude it from consideration in this case. This decision was due to Power BI's limitation of connecting to Athena solely via an ODBC driver installed locally, necessitating the installation of a Power BI gateway for scheduling daily refreshes.
- I planned to integrate Data Build Tool (dbt) to manage the transformation of raw data in the later stages of the pipeline, with Dagster orchestrating the entire process.
  
##### The steps I took:
- **Step 1**: I took the initiative to create empty tables in BigQuery for the raw data. This preemptive step allowed me to declare the expected schema of the incoming data beforehand. By leveraging the BigQuery APIs in Python, I established a schema validation protocol ensuring that only columns declared in the target tables in BigQuery would be extracted from the source JSON files. While I usually utilize SQLALchemy ORM for schema validation, I opted for Google APIs due to their comprehensive built-in methods, simplifying the process effectively.
```Python
class BigQueryOps:
    """ 
    This class consists of methods working with tables in Google Cloud BigQuery 
    """
    def __init__(self, table: str) -> None:
        """ Instanciate a BigQuery instance with the declared table """
        self.client = bigquery.Client()
        try:
            self.table = self.client.get_table(table)
        except Exception as e:
            raise ValueError(f"Error fetching table: {e}")

    def get_columns(self) -> list:
        """ 
        Retrive a list columns of the table 
        """
        if self.table:
            self.columns = [field.name for field in self.table.schema]
            return self.columns
        else:
            raise ValueError("Error fetching columns!")

...

```
- **Step 2:** The JSON file from the AWS S3 bucket was read into a streaming body before being read into a Pandas DataFrame. This approach not only eliminated the need for local disk space, but also enabled me to employ other resource optimization methods, such as reading data in chunks. Additionally, loading the file into a streaming body was significantly faster than downloading it locally, taking only 5-8 seconds compared to 30-50 seconds. This allowed multiple downloads to happen simultanously, without using too much resource.
```Python
class S3Ops:
  """ 
  This class consists of methods working with files in AWS S3 
  """
    def __init__(self, bucket: str, key: str, columns: str = None) -> None:
        """ 
        Instanciate an s3 session and a resource instance with default configs 
        """
        self.bucket = bucket
        self.key = key
        self.columns = columns
        self.s3_config = Config(s3={"use_accelerate_endpoint": True})
        self.session = boto3.Session()
        self.s3 = self.session.resource("s3", config=self.s3_config)

    def get_data(self) -> pd.DataFrame:
        """ Download json file as streaming object before reading into a Pandas DataFrame """
        obj = self.s3.Object(self.bucket, self.key)
        json_data = json.loads(obj.get()["Body"].read().decode())
        if self.columns is None:
            data_dict = json.dumps(json_data)
            df = pd.DataFrame(json_data)
        else:
            data_dict = json.dumps(
                [{col: entry[col] for col in self.columns} for entry in json_data]
            )
            df = pd.read_json(data_dict)
        return df
```

#### 4.3. Load:
Once the DataFrame was created, basic transformations such as data type conversion, duplicate and null value removal were executed. Subsequently, the transformed DataFrame was loaded into the target table in BigQuery utilizing a method from the Google APIs. It's worth mentioning that I used Dagster MaterializeResult class to generate a graph illustrating the total rows loaded during each run. This enabled me to track day-over-day changes effectively.

```Python
@asset(compute_kind="Python", group_name="extract")
def raw_sale_orders(context: AssetExecutionContext) -> MaterializeResult:
    gc = BigQueryOps(table="activeTix.raw.raw_sale_orders") # Connect to the target table
    s3 = S3Ops(
        bucket="activetix",
        key="datalakehouse/saleorder/saleorder.json",
        columns=gc.get_columns(),                           # Only read columns that are in the target table
    )
    data = s3.get_data()
    try:
        gc.load_table(dataframe=data)
    except Exception as e:
        raise f"Failed to load table: {e}"
    finally:
        context.log.info(f"Total rows {data.shape[0]}")
    return MaterializeResult(metadata={"number_of_rows": data.shape[0]})
```
#### 4.4. Transform:
In this project I chose Data Build Tool (dbt) for the transformation part for a number of reasons:
- SQL!
- dbt makes it extremely easy to document my models, including the source and target schema, the data integrity checks, and the compiled query behind it.
- Built-in data quality check.
- Reuseable macros.

### 5. Technologies, Tools, and Frameworkes:
This project leverages a variety of open-source technologies (Dagster and dbt) and cloud services (GCP, AWS and Azure), with Python and SQL being the major programming languages. The final application is running on Docker to ensure scalability.

![activeTix](https://github.com/khoinguyenvo/Porfolio/assets/133230440/c5faa94d-b56d-4d25-a8dc-d874f25af15c)

### 6. Github repository:
[Project 1 - Online Ticketing Platform - Data Pipeline](https://github.com/khoinguyenvo/Porfolio/tree/43fd579549de6aa6ee7cce5cc29fddbea419636d/Project%201)
<details>

