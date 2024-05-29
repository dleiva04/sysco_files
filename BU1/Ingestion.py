# Databricks notebook source
env = dbutils.widgets.get("env")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

secret_scope = f"{env}_scope"

# Retrieve secrets dynamically based on the environment
kafka_bootstrap_servers = dbutils.secrets.get(scope=secret_scope, key="kafka.bootstrap.servers")
kafka_topic = dbutils.secrets.get(scope=secret_scope, key="kafka.topic")
kafka_security_protocol = dbutils.secrets.get(scope=secret_scope, key="kafka.security.protocol")
kafka_sasl_mechanism = dbutils.secrets.get(scope=secret_scope, key="kafka.sasl.mechanism")
kafka_sasl_jaas_config = dbutils.secrets.get(scope=secret_scope, key="kafka.sasl.jaas.config")

# COMMAND ----------

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic)
      .option("kafka.security.protocol", kafka_security_protocol)
      .option("kafka.sasl.mechanism", kafka_sasl_mechanism)
      .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config)
      .option("startingOffsets", "latest")
      .load())

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show()

table_name = f"{env}_kafka_table"

df.writeStream \
  .format("delta") \
  .option("checkpointLocation", f"/tmp/{table_name}/checkpoint") \
  .outputMode("append") \
  .table(f"{catalog}.{table_name}")
