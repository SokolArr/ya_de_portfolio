from pyspark.sql.types import StructType, StructField, DoubleType, StringType

spark_jars_packages = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.postgresql:postgresql:42.4.0",
])

kafka_read_settings = {
    'kafka.bootstrap.servers': 'XXX',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config':'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"XXX\" password=\"XXX\";',
    "subscribe": "topic_in"
}

kafka_write_settings = {
    'kafka.bootstrap.servers': 'XXX',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"XXX\" password=\"XXX\";',
    'topic': 'topic_out',
    'truncate': False
}

postgresql_read_settings = {
    'url': "XXX",
    'driver': "org.postgresql.Driver",
    'user': "XXX",
    'password': "XXX",
    'dbtable': "public.subscribers_restaurants"
}

postgresql_write_settings = {
    'url': 'jdbc:postgresql://localhost:5432/de',
    'driver': 'org.postgresql.Driver',
    'schema': 'public',
    'dbtable': 'subscribers_feedback',
    'user': 'jovyan',
    'password': 'jovyan',
    'autoCommit': 'true'
}

restaurant_schema = StructType([
    StructField("restaurant_id", StringType()),
    StructField("adv_campaign_id", StringType()),
    StructField("adv_campaign_content", StringType()),
    StructField("adv_campaign_owner", StringType()),
    StructField("adv_campaign_owner_contact", StringType()),
    StructField("adv_campaign_datetime_start", DoubleType()),
    StructField("adv_campaign_datetime_end", DoubleType()),
    StructField("datetime_created", DoubleType())
])