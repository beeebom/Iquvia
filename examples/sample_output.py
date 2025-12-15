# Generated PySpark Code - Microsoft Fabric Compatible
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark session (if not already available in Fabric)
# spark = SparkSession.builder.appName('QlikConverter').getOrCreate()

# Variables
vYear = 2024
vDataPath = 'data/'

# Mapping: CountryMap
map_CountryMap = spark.read.csv('countries.csv', header=True, inferSchema=True)
map_CountryMap = map_CountryMap.select('CountryCode', 'CountryName')
map_CountryMap = map_CountryMap.filter(col('Active') == 1)
map_CountryMap_dict = map_CountryMap.rdd.collectAsMap()

# Table: Customers
df_customers = spark.read.csv('data/customers.csv', header=True, inferSchema=True)
df_customers = df_customers.select(
    col('CustomerID'),
    col('CustomerName'),
    upper(col('Email')).alias('Email'),
    col('CountryCode'),
    # ApplyMap would be applied here using the mapping dictionary
)

# Table: Orders
df_orders = spark.read.csv('data/orders.csv', header=True, inferSchema=True)
df_orders = df_orders.select(
    col('OrderID'),
    col('CustomerID'),
    col('OrderDate'),
    year(col('OrderDate')).alias('OrderYear'),
    month(col('OrderDate')).alias('OrderMonth'),
    col('Amount'),
    (col('Amount') * 1.2).alias('AmountWithTax')
)
df_orders = df_orders.filter(year(col('OrderDate')) == 2024)

# Table: OrderDetails (LEFT JOIN with Orders)
df_orderdetails = spark.read.csv('data/order_details.csv', header=True, inferSchema=True)
df_orders = df_orders.join(df_orderdetails, df_orders['OrderID'] == df_orderdetails['OrderID'], 'left')

# Table: SalesSummary
df_salessummary = df_orders.groupBy(
    col('CustomerID'),
    col('OrderYear')
).agg(
    count(col('OrderID')).alias('TotalOrders'),
    sum(col('Amount')).alias('TotalAmount'),
    avg(col('Amount')).alias('AvgAmount')
)

# Table: Categories (Inline data)
df_categories_schema = StructType([
    StructField('CategoryID', StringType(), True),
    StructField('CategoryName', StringType(), True)
])
df_categories_data = [
    ['1', 'Electronics'],
    ['2', 'Clothing'],
    ['3', 'Food'],
    ['4', 'Books']
]
df_categories = spark.createDataFrame(df_categories_data, df_categories_schema)

# Table: Products
df_products = spark.read.csv('data/products.csv', header=True, inferSchema=True)

# Table: UniqueCustomers
df_uniquecustomers = df_customers
df_uniquecustomers = df_uniquecustomers.select(
    col('CustomerID'),
    col('CustomerName'),
    col('Country')
)
df_uniquecustomers = df_uniquecustomers.distinct()

# Display results (optional)
# Uncomment to view tables:
# df_customers.show()
# df_orders.show()
# df_salessummary.show()
# df_categories.show()
# df_products.show()
# df_uniquecustomers.show()

