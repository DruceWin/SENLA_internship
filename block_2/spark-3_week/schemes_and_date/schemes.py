from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, DoubleType, MapType


schema_articles = StructType(fields=[
    StructField("article_id", StringType()),
    StructField("product_code", IntegerType()),
    StructField("prod_name", StringType()),
    StructField("product_type_no", IntegerType()),
    StructField("product_type_name", StringType()),
    StructField("product_group_name", StringType()),
    StructField("graphical_appearance_no", IntegerType()),
    StructField("graphical_appearance_name", StringType()),
    StructField("colour_group_code", IntegerType()),
    StructField("colour_group_name", StringType()),
    StructField("perceived_colour_value_id", IntegerType()),
    StructField("perceived_colour_value_name", StringType()),
    StructField("perceived_colour_master_id", IntegerType()),
    StructField("perceived_colour_master_name", StringType()),
    StructField("department_no", IntegerType()),
    StructField("department_name", StringType()),
    StructField("index_code", StringType()),
    StructField("index_name", StringType()),
    StructField("index_group_no", IntegerType()),
    StructField("index_group_name", StringType()),
    StructField("section_no", IntegerType()),
    StructField("section_name", StringType()),
    StructField("garment_group_no", IntegerType()),
    StructField("garment_group_name", StringType()),
    StructField("text", StringType())
])

schema_customers = StructType(fields=[
    StructField("customer_id", StringType()),
    StructField("FN", FloatType()),
    StructField("Active", FloatType()),
    StructField("club_member_status", StringType()),
    StructField("fashion_news_frequency", StringType()),
    StructField("age", IntegerType()),
    StructField("postal_code", StringType())
])

schema_transactions_train = StructType(fields=[
    StructField("t_dat", DateType()),
    StructField("customer_id", StringType()),
    StructField("article_id", StringType()),
    StructField("price", DoubleType()),
    StructField("sales_channel_id", StringType())
])

schema_transactions_train_currency = StructType(fields=[
    StructField("id", IntegerType()),
    StructField("t_dat", DateType()),
    StructField("customer_id", StringType()),
    StructField("article_id", StringType()),
    StructField("price", DoubleType()),
    StructField("sales_channel_id", StringType()),
    StructField("currency", StringType()),
    StructField("current_exchange_rate", StringType())
    # StructField("current_exchange_rate", MapType(StringType(), IntegerType()))
])