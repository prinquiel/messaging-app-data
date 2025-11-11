import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct, hour


def main():
    spark = (
        SparkSession.builder.appName("messaging-insights")
        .getOrCreate()
    )

    db_host = os.getenv("ANALYTICS_DB_HOST", "analyticsdb")
    db_port = os.getenv("ANALYTICS_DB_PORT", "5432")
    db_name = os.getenv("ANALYTICS_DB_NAME", "analyticsdb")
    db_user = os.getenv("ANALYTICS_DB_USER", "analyticsuser")
    db_pass = os.getenv("ANALYTICS_DB_PASSWORD", "analyticspassword")

    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    props = {
        "user": db_user,
        "password": db_pass,
        "driver": "org.postgresql.Driver",
    }

    # Read analytics tables
    user_stats = spark.read.jdbc(jdbc_url, "user_statistics", properties=props)
    chat_stats = spark.read.jdbc(jdbc_url, "chat_statistics", properties=props)
    daily_stats = spark.read.jdbc(jdbc_url, "daily_message_stats", properties=props)
    hourly_stats = spark.read.jdbc(jdbc_url, "hourly_message_stats", properties=props)
    type_summary = spark.read.jdbc(jdbc_url, "message_type_summary", properties=props)

    # Example aggregations (Spark side, though tables already aggregated)
    top_users = (
        user_stats.select("username", "total_messages_sent")
        .orderBy(col("total_messages_sent").desc())
        .limit(20)
    )

    busy_chats = (
        chat_stats.select("chat_name", "total_messages", "unique_senders")
        .orderBy(col("total_messages").desc())
        .limit(20)
    )

    # Persist results back into Postgres as materialized tables for dashboards
    top_users.write.mode("overwrite").jdbc(jdbc_url, "spark_top_users", properties=props)
    busy_chats.write.mode("overwrite").jdbc(jdbc_url, "spark_busy_chats", properties=props)
    type_summary.write.mode("overwrite").jdbc(jdbc_url, "spark_message_type_mix", properties=props)
    hourly_stats.write.mode("overwrite").jdbc(jdbc_url, "spark_hourly_distribution", properties=props)
    daily_stats.write.mode("overwrite").jdbc(jdbc_url, "spark_daily_activity", properties=props)

    spark.stop()


if __name__ == "__main__":
    main()


