# Databricks notebook source
# DBTITLE 1,IMPORTS
from pyspark.sql.functions import col, count, max, sha2, from_utc_timestamp, min, explode, from_json
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # -------------- DIRETÓRIOS DE LEITURA E ESCRITA DOS ARQUIVOS --------------

# COMMAND ----------

# DBTITLE 1,monta o diretório do S3 onde serão escritos os arquivos do datalake
import urllib.parse
ACCESS_KEY = "AKIARXNHLHYJAZHIWFBJ"
SECRET_KEY = "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"
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY, "")
AWS_BUCKET_NAME = "teste-data-architect"
MOUNT_NAME = "datalake_test_data_architect"
dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# DBTITLE 1,monta o diretório do S3 no DBFS para consumo dos arquivos base
import urllib.parse
ACCESS_KEY = "AKIARXNHLHYJAZHIWFBJ"
SECRET_KEY = "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"
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY, "")
AWS_BUCKET_NAME = "ifood-data-architect-test-source"
MOUNT_NAME = "transient_zone"
dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# DBTITLE 1,visualiza o diretorio de consumo
display(dbutils.fs.ls("/mnt/transient_zone"))

# COMMAND ----------

# DBTITLE 1,visualiza o diretorio de escrita
display(dbutils.fs.ls("/mnt/datalake_test_data_architect"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # -------------- ZONA RAW --------------

# COMMAND ----------

# DBTITLE 1,lê os arquivos e cria colunas de particionamento
customer = spark.read.csv("dbfs:/mnt/transient_zone/consumer.csv.gz",  header=True, inferSchema=True)\
                .withColumn('date_partition', col('created_at').cast(DateType()).cast(StringType()))

status = spark.read.json("dbfs:/mnt/transient_zone/status.json.gz")\
              .withColumn('date_partition', col('created_at').cast(DateType()).cast(StringType()))

merchant = spark.read.csv("dbfs:/mnt/transient_zone/restaurant.csv.gz",  header=True, inferSchema=True)\
                .withColumn('date_partition', col('created_at').cast(DateType()).cast(StringType()))

order = spark.read.json("dbfs:/mnt/transient_zone/order.json.gz")\
              .withColumn('date_partition', col('order_created_at').cast(DateType()).cast(StringType()))

# COMMAND ----------

# DBTITLE 1,escreve as tabelas da zona raw
dbutils.fs.rm('/mnt/datalake_test_data_architect/raw_zone/customer_raw.parquet', True)
customer.write.partitionBy('date_partition').parquet('/mnt/datalake_test_data_architect/raw_zone/customer_raw.parquet')

dbutils.fs.rm('/mnt/datalake_test_data_architect/raw_zone/status_raw.parquet', True)
status.write.partitionBy('date_partition').parquet('/mnt/datalake_test_data_architect/raw_zone/status_raw.parquet')

dbutils.fs.rm('/mnt/datalake_test_data_architect/raw_zone/merchant_raw.parquet', True)
merchant.write.partitionBy('date_partition').parquet('/mnt/datalake_test_data_architect/raw_zone/merchant_raw.parquet')

dbutils.fs.rm('/mnt/datalake_test_data_architect/raw_zone/order_raw.parquet', True)
order.write.partitionBy('date_partition').parquet('/mnt/datalake_test_data_architect/raw_zone/order_raw.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # -------------- ORDERS TRUSTED --------------

# COMMAND ----------

# DBTITLE 1,carrega as tabelas raw, renomeia as colunas e separa só o último status do pedido
#EXISTEM PEDIDOS COM MAIS DE UM TIMESTAMP DIFERENTE NA COLUNA CREATED_AT, O QUE ESTAVA GERANDO LINHAS DUPLICADAS
data_pedido = spark.read.parquet('/mnt/datalake_test_data_architect/raw_zone/order_raw.parquet')\
              .groupBy('order_id').agg(min(col('order_created_at').cast(TimestampType())).alias('data_minima'))

order_raw = spark.read.parquet('/mnt/datalake_test_data_architect/raw_zone/order_raw.parquet')\
                      .withColumn('data_minima', col('order_created_at').cast(TimestampType()))\
                      .join(data_pedido, ['order_id', 'data_minima'], how = 'inner')\
                      .select('order_id', 'order_created_at', col('order_scheduled').alias('is_order_scheduled'), 'order_scheduled_date', 'order_total_amount', 'merchant_id', 'merchant_latitude', 'merchant_longitude', 'merchant_timezone', 'customer_id', col('cpf').alias('customer_cpf'), 'delivery_address_city', 'delivery_address_country', 'delivery_address_district', 'delivery_address_external_id', 'delivery_address_latitude', 'delivery_address_longitude', 'delivery_address_state', 'delivery_address_zip_code', col('origin_platform').alias('order_origin_platform'))\
                      .withColumn('order_scheduled_timestamp', col('order_scheduled_date').cast(TimestampType()))\
                      .withColumn('order_created_at_timestamp', col('order_created_at').cast(TimestampType()))\
                      .drop('order_scheduled_date', 'order_created_at')

merchant_raw = spark.read.parquet('/mnt/datalake_test_data_architect/raw_zone/merchant_raw.parquet')\
                    .select(col('id').alias('merchant_id'), col('created_at').alias('merchant_account_created_at_timestamp'), 'enabled', 'price_range', 'average_ticket', 'takeout_time', 'delivery_time', 'minimum_order_value', 'merchant_zip_code', 'merchant_city', 'merchant_state', 'merchant_country')\
                    .withColumn('merchant_account_created_at_timestamp', col('merchant_account_created_at_timestamp').cast(TimestampType()))\
                    .withColumnRenamed('enabled', 'is_merchant_enabled')\
                    .withColumnRenamed('price_range', 'merchant_price_range')\
                    .withColumnRenamed('average_ticket', 'merchant_average_ticket')\
                    .withColumnRenamed('takeout_time', 'merchant_takeout_time')\
                    .withColumnRenamed('delivery_time', 'merchant_delivery_time')\
                    .withColumnRenamed('minimum_order_value', 'merchant_minimum_order_value')

customer_raw = spark.read.parquet('/mnt/datalake_test_data_architect/raw_zone/customer_raw.parquet')\
                    .select('customer_id', col('language').alias('customer_language'), col('created_at').alias('customer_account_creation_timestamp'), col('active').alias('is_customer_active'), 'customer_name', 'customer_phone_area', 'customer_phone_number')

#EXISTIA MAIS DE 1 LINHA PARA CADA COMBINAÇÃO DE PEDIDO E STATUS
status_raw = spark.read.parquet('/mnt/datalake_test_data_architect/raw_zone/status_raw.parquet')\
                  .select('created_at', 'order_id', 'status_id', 'value')\
                  .distinct()

last_status = status_raw\
              .select('order_id', col('created_at').cast(TimestampType()))\
              .groupBy('order_id').agg(max(col('created_at')).alias('created_at'))
              

status_final = status_raw.join(last_status, ['order_id', 'created_at'], how = 'inner')\
               .withColumnRenamed('created_at', 'status_last_change_timestamp')\
               .withColumn('order_status_last_change_timestamp', col('status_last_change_timestamp').cast(TimestampType()))\
               .withColumnRenamed('value', 'order_last_status')\
               .withColumnRenamed('status_id', 'order_status_last_change_id')

# COMMAND ----------

# DBTITLE 1,faz os joins, anonimiza dados sensíveis e ordena colunas
order_trusted = order_raw\
                .join(merchant_raw, ['merchant_id'], how = 'inner')\
                .join(customer_raw, ['customer_id'], how = 'inner')\
                .join(status_final, ['order_id'], how = 'inner')

order_trusted = order_trusted\
                .withColumn('customer_name_hash', sha2(col('customer_name'), 256))\
                .withColumn('customer_cpf_hash', sha2(col('customer_cpf'), 256))\
                .withColumn('customer_phone_number_hash', sha2(col('customer_phone_number').cast(StringType()), 256))\
                .drop('customer_name', 'customer_cpf', 'customer_phone_number')

cols_order = ['order_id',
              'order_created_at_timestamp',
              'order_total_amount',
              'is_order_scheduled',
              'order_scheduled_timestamp',
              'order_origin_platform',
              'order_last_status',
              'order_status_last_change_timestamp',
              'order_status_last_change_id',
              'customer_id',
              'customer_name_hash',
              'customer_cpf_hash',
              'customer_phone_area',
              'customer_phone_number_hash',
              'is_customer_active',
              'customer_account_creation_timestamp',
              'customer_language',
              'merchant_id',
              'merchant_latitude',
              'merchant_longitude',
              'merchant_timezone',
              'is_merchant_enabled',
              'merchant_price_range',
              'merchant_average_ticket',
              'merchant_takeout_time',
              'merchant_delivery_time',
              'merchant_minimum_order_value',
              'merchant_zip_code',
              'merchant_city',
              'merchant_state',
              'merchant_country',
              'merchant_account_created_at_timestamp',
              'delivery_address_city',
              'delivery_address_country',
              'delivery_address_district',
              'delivery_address_external_id',
              'delivery_address_latitude',
              'delivery_address_longitude',
              'delivery_address_state',
              'delivery_address_zip_code',
]

order_trusted_final = order_trusted.select(cols_order)\
                      .withColumn('order_date_local', from_utc_timestamp(col('order_created_at_timestamp'), col('merchant_timezone')).cast(DateType()).cast(StringType()))

# COMMAND ----------

# DBTITLE 1,escreve trusted_zone.order_trusted
dbutils.fs.rm('/mnt/datalake_test_data_architect/trusted_zone/order_trusted.parquet', True)
order_trusted_final.write.partitionBy('order_date_local').parquet('/mnt/datalake_test_data_architect/trusted_zone/order_trusted.parquet')

# COMMAND ----------

# DBTITLE 1,verifica 1 linha por pedido
# ESSE COMANDO DEVE RETORNAR RESULTADO VAZIO
order_trusted = spark.read.parquet('/mnt/datalake_test_data_architect/trusted_zone/order_trusted.parquet')\
                     .select('order_id')\
                     .groupBy('order_id').agg(count(col('order_id')).alias('linhas'))\
                     .where("linhas > 1")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # -------------- STATUS TRUSTED --------------

# COMMAND ----------

status_trusted = spark.read.parquet('/mnt/datalake_test_data_architect/raw_zone/status_raw.parquet')\
                  .select('created_at', 'order_id', 'status_id', 'value')\
                  .distinct()\
                  .withColumn('created_at', col('created_at').cast(TimestampType()))\
                  .groupBy('order_id')\
                  .pivot('value')\
                  .agg(min(col('created_at')))\
                  .withColumnRenamed('CANCELLED', 'cancelled_status_timestamp')\
                  .withColumnRenamed('CONCLUDED', 'concluded_status_timestamp')\
                  .withColumnRenamed('PLACED', 'placed_status_timestamp')\
                  .withColumnRenamed('REGISTERED', 'registered_status_timestamp')\
                  .select('order_id', 'placed_status_timestamp', 'registered_status_timestamp', 'concluded_status_timestamp', 'cancelled_status_timestamp')\
                  .withColumn('order_date_local', coalesce(col('concluded_status_timestamp'), col('cancelled_status_timestamp')))\
                  .withColumn('order_date_local', col('order_date_local').cast(DateType()).cast(StringType()))

# COMMAND ----------

# DBTITLE 1,escreve trusted_zone.status_trusted
dbutils.fs.rm('/mnt/datalake_test_data_architect/trusted_zone/status_trusted.parquet', True)
status_trusted.write.partitionBy('order_date_local').parquet('/mnt/datalake_test_data_architect/trusted_zone/status_trusted.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # -------------- TENTATIVA ITEMS TRUSTED --------------

# COMMAND ----------

#DEVE TER ALGO DE ERRADO COM ESSE SCHEMA, JÁ QUE QUANDO EU LEIO O ARQUIVO COM ELE, VEM TUDO VAZIO. NÃO CONSEGUI RESOLVER

order_raw_schema = StructType([
	StructField("cpf", StringType(), True),
	StructField("customer_id", StringType(), True),
	StructField("customer_name", StringType(), True),
	StructField("delivery_address_city", StringType(), True),
	StructField("delivery_address_country", StringType(), True),
	StructField("delivery_address_district", StringType(), True),
	StructField("delivery_address_external_id", StringType(), True),
	StructField("delivery_address_latitude", StringType(), True),
	StructField("delivery_address_longitude", StringType(), True),
	StructField("delivery_address_state", StringType(), True),
	StructField("delivery_address_zip_code", StringType(), True),
	StructField("items", ArrayType(StructType([
		StructField("name", StringType(), True),
		StructField("addition", StructType([
			StructField("value", StringType(), True),
			StructField("currency", StringType(), True)
		]), True),
		StructField("discount", StructType([
			StructField("value", StringType(), True),
			StructField("currency", StringType(), True)
		]), True),
		StructField("quantity", DecimalType(), True),
		StructField("sequence", IntegerType(), True),
		StructField("unit_price", StructType([
			StructField("value", StringType(), True),
			StructField("currency", StringType(), True)
		]), True),
		StructField("externalId", StringType(), True),
		StructField("totalValue", StructType([
			StructField("value", StringType(), True),
			StructField("currency", StringType(), True)
		]), True),
		StructField("customerNote", StringType(), True),
		StructField("garnishItems", ArrayType(StructType([
			StructField("name", StringType(), True),
			StructField("addition", StructType([
				StructField("value", StringType(), True),
				StructField("currency", StringType(), True)
			]), True),
			StructField("discount", StructType([
				StructField("value", StringType(), True),
				StructField("currency", StringType(), True)
			]), True),
			StructField("quantity", DecimalType(), True),
			StructField("sequence", IntegerType(), True),
			StructField("unitPrice", StructType([
				StructField("value", StringType(), True),
				StructField("currency", StringType(), True)
			]), True),
			StructField("categoryId", StringType(), True),
            StructField("externalId", StringType(), True),
			StructField("totalValue", StructType([
				StructField("value", StringType(), True),
				StructField("currency", StringType(), True)
			]), True),
			StructField("categoryName", StringType(), True),
			StructField("integrationId", StringType(), True)
		])), True),
        StructField("integrationId", StringType(), True),
        StructField("totalAddition", StructType([
            StructField("value", StringType(), True),
            StructField("currency", StringType(), True)
		]), True),
        StructField("totalDiscount", StructType([
            StructField("value", StringType(), True),
            StructField("currency", StringType(), True)
        ]), True),])), True),
	StructField("merchant_id", StringType(), True),
	StructField("merchant_latitude", StringType(), True),
	StructField("merchant_longitude", StringType(), True),
	StructField("merchant_timezone", StringType(), True),
	StructField("order_created_at", StringType(), True),
	StructField("order_id", StringType(), True),
	StructField("order_scheduled", BooleanType(), True),
	StructField("order_scheduled_date", StringType(), True),
	StructField("order_total_amount", DoubleType(), True),
	StructField("origin_platform", StringType(), True)])

# COMMAND ----------

items_trusted = spark.read.json("dbfs:/mnt/transient_zone/order.json.gz", schema = order_raw_schema)

# COMMAND ----------

items_trusted.where("order_id is not null").count()
