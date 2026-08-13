# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
print("Hello Databricks!")

# COMMAND ----------

spark

# COMMAND ----------

df_customers = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/workspace/default/ecommerce_raw/customers.csv')

# COMMAND ----------

df_customers.display()

# COMMAND ----------

df_customers.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

df_customers.select(col('name'),col('city'),col('state')).show()

# COMMAND ----------

df_customers.filter(col('state')=='Delhi').show()

# COMMAND ----------

df_customers.filter(col('state')=='Delhi').select(col('name'),col('city')).show()

# COMMAND ----------

df_customers.withColumn('new_column',when(col('state')=='Delhi','Delhi Customer').otherwise('Other Customer')).display()

# COMMAND ----------

df_categories = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/workspace/default/ecommerce_raw/categories.csv')

# COMMAND ----------

df_sellers = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/workspace/default/ecommerce_raw/sellers.csv')

# COMMAND ----------

df_products = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/workspace/default/ecommerce_raw/products.csv')
df_orders = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/workspace/default/ecommerce_raw/orders.csv')


# COMMAND ----------

df_order_items = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/workspace/default/ecommerce_raw/order_items.csv')

# COMMAND ----------

df_payments = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/workspace/default/ecommerce_raw/payments.csv')

# COMMAND ----------

print(df_categories.count())
print(df_sellers.count())
print(df_products.count())
print(df_orders.count())
print(df_order_items.count())
print(df_payments.count())

# COMMAND ----------

df_customers.write.format('delta').mode('overwrite').saveAsTable('bronze_customers')

# COMMAND ----------

spark.sql("""
          select * from bronze_customers""").show()

# COMMAND ----------

spark.table('bronze_customers').count()

# COMMAND ----------

spark.table('bronze_customers').groupby('customer_id').count().filter(col('count')> 1).display()

# COMMAND ----------

bronze_tables = {
    'sellers': df_sellers,
    'orders': df_orders,
    'order_items': df_order_items,
    'payments': df_payments,
    'products': df_products,
    'categories': df_categories,
    'customers': df_customers
}

for table_name, dataframe in bronze_tables.items():
    dataframe.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'bronze_{table_name}')
spark.sql("""
          show tables""").show()

# COMMAND ----------

display(spark.table('bronze_customers'))

# COMMAND ----------

df_customers.select(
    sum(when(col("customer_id").isNull(),1).otherwise(0)).alias('null_customer_id'),
    sum(when(col("name").isNull(),1).otherwise(0)).alias('null_name'),
    sum(when(col("email").isNull(),1).otherwise(0)).alias('null_email'),
    sum(when(col("city").isNull(),1).otherwise(0)).alias('null_city'),
    sum(when(col("state").isNull(),1).otherwise(0)).alias('null_state'),
    sum(when(col("registration_date").isNull(),1).otherwise(0)).alias('null_registration_date')
).display()

# COMMAND ----------

df_customers.printSchema()

# COMMAND ----------

df_silver_customers = df_customers.withColumn("email_domain",split(col('email'),'@')[1]).withColumn("registration_year",year(col('registration_date')))
df_silver_customers.show()

# COMMAND ----------

df_silver_customers.write.format('delta').mode('overwrite').saveAsTable('silver_customers')
spark.sql("""
          select * from silver_customers""").show()

# COMMAND ----------

df_orders.printSchema()

# COMMAND ----------

df_orders.select('order_status').distinct().show()

# COMMAND ----------

df_orders.groupBy('order_status').count().show()

# COMMAND ----------

df_silver_orders = df_orders.withColumn("is_completed",when(col("order_status")=='Delivered',1).otherwise(0))


# COMMAND ----------

df_silver_orders.select(col('order_id'),col('order_status'),col('is_completed')).show()

# COMMAND ----------

df_silver_orders.count()

# COMMAND ----------

df_silver_orders = df_silver_orders.withColumn('order_year',year(col('order_date'))).withColumn('order_month',month(col('order_date')))
df_silver_orders.select(col('order_id'),col('order_date'),col('order_year'),col('order_month'),col('order_status'),col('is_completed')).show()

df_silver_orders.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('silver_orders')

# COMMAND ----------

df_silver_orders.count()

# COMMAND ----------

display(spark.table('silver_orders'))

# COMMAND ----------

spark.table('silver_orders').count()

# COMMAND ----------

df_order_items.printSchema()

# COMMAND ----------

df_silver_order_items = df_order_items.withColumn('line_total',col('quantity')*col('price_at_purchase'))
df_silver_order_items.show()

# COMMAND ----------

df_silver_order_items = df_order_items.withColumn('line_total',col('quantity')*col('price_at_purchase'))
df_silver_order_items.select(
    sum(when(col('quantity').isNull(), 1).otherwise(0)).alias('null_quantity'),
    sum(when(col('price_at_purchase').isNull(), 1).otherwise(0)).alias('null_price')
)
df_silver_order_items.filter(
    (col('quantity')<=0)| (col('price_at_purchase')<=0)
).show()

# COMMAND ----------

df_silver_order_items.write.format('delta').mode('overwrite').saveAsTable('silver_order_items')

# COMMAND ----------

display(spark.table('silver_order_items'))
spark.table('silver_order_items').count()

# COMMAND ----------

df_products.printSchema()

# COMMAND ----------

df_products.select(
    sum(when(col('product_id').isNull(),1).otherwise(0)).alias('null_product_id'),
    sum(when(col('product_name').isNull(),1).otherwise(0)).alias('null_product_name'),
    sum(when(col('category_id').isNull(),1).otherwise(0)).alias('null_category_id'),
    sum(when(col('seller_id').isNull(),1).otherwise(0)).alias('null_seller_id'),
    sum(when(col('price').isNull(),1).otherwise(0)).alias('null_price'),
    sum(when(col('stock_quantity').isNull(),1).otherwise(0)).alias('null_stock_quantity')
).display()

# COMMAND ----------

df_products.filter((col('price')<=0)|(col('stock_quantity')<0)).show()

# COMMAND ----------

df_silver_products = df_products.withColumn('stock_status',when(col('stock_quantity')>0,'in_stock').otherwise('out_of_stock'))
df_silver_products.write.format('delta').mode('overwrite').saveAsTable('silver_products')
display(spark.table('silver_products'))
spark.table('silver_products').count()

# COMMAND ----------

df_order_product = df_silver_order_items.join(spark.table('silver_products'),['product_id']).select(col('order_id'),col('product_id'),col('product_name'),col('quantity'),col('price_at_purchase'),col("line_total"))
df_order_product.display()

# COMMAND ----------

df_silver_order_items.count()

# COMMAND ----------

df_order_product.count()

# COMMAND ----------

df_silver_order_items.join(
    spark.table('silver_products'),
    ['product_id'],
    'left_anti').show()


# COMMAND ----------

df_categories.printSchema()

# COMMAND ----------

display(df_categories)

# COMMAND ----------

df_categories.groupby('category_id').count().filter(col('count')>1).show()

# COMMAND ----------

df_categories.select(
    sum(when(col('category_id').isNull(),1).otherwise(0)).alias('null_category_id'),
    sum(when(col('category_name').isNull(),1).otherwise(0)).alias('null_category_name')).display()

# COMMAND ----------

df_categories.write.format('delta').mode('overwrite').saveAsTable('silver_categories')

# COMMAND ----------

display(spark.table('silver_categories'))

# COMMAND ----------

df_order_product_category = df_silver_order_items.join(spark.table('silver_products'),['product_id'],'inner')

# COMMAND ----------

df_order_product_category = df_order_product_category.select(
    'order_id',
    'product_id',
    'product_name',
    'category_id',
    'quantity',
    'price_at_purchase',
    'line_total',
)

# COMMAND ----------

spark.table('silver_categories')

# COMMAND ----------

df_order_product_category = df_order_product_category.join(
    spark.table("silver_categories"),
    ["category_id"],
    "inner"
)

# COMMAND ----------

df_order_product_category = df_order_product_category.select(
    'order_id',
    'product_id',
    'product_name',
    'quantity',
    'price_at_purchase',
    'line_total',
    'category_name',
)
df_order_product_category.show()

# COMMAND ----------

df_silver_orders.show()

# COMMAND ----------

df_order_product_customer = df_order_product_category.join(
    spark.table("silver_orders"),
    ["order_id"],
    "inner"
)

# COMMAND ----------

df_order_product_customer.select(
    "order_id",
    "customer_id",
    "product_id",
    "product_name",
    "category_name",
    "quantity",
    "line_total"
).show()

# COMMAND ----------

df_final_detail = df_order_product_customer.join(
    spark.table("silver_customers"),
    ["customer_id"],
    "inner"
)

# COMMAND ----------

df_final_detail.select(
    'order_id',
    'customer_id',
    'name',
    'city',
    'state',
    'product_id',
    'product_name',
    'category_name',
    'quantity',
    'line_total'
)

# COMMAND ----------

df_final_detail.show()

# COMMAND ----------

display(spark.table("silver_payments"))

# COMMAND ----------

# DBTITLE 1,Create silver_payments table
# Create silver_payments table from bronze data
df_payments = df_payments.withColumn('payment_id', df_payments['payment_id'].cast('string'))
df_payments.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable('silver_payments')


# COMMAND ----------

spark.table("silver_payments").printSchema()

# COMMAND ----------

df_payments.select('payment_method').distinct().show()

# COMMAND ----------

df_payments.select("payment_status").distinct().show()

# COMMAND ----------

df_payments.select("amount").show(20, False)

# COMMAND ----------

df_payments.select("payment_date").show(20, False)

# COMMAND ----------

df_payments.select('payment_method').distinct().show()

# COMMAND ----------

df_silver_payments = df_payments.withColumn('order_id', col('order_id').cast('int')) \
    .withColumn('amount', col('amount').cast('int')) \
    .withColumn('payment_date', col('payment_date').cast('date'))

# COMMAND ----------

df_silver_payments.printSchema()

# COMMAND ----------

df_silver_payments.select(
    sum(when(col('order_id').isNull(),1).otherwise(0)).alias("null_order_id"),
    sum(when(col('amount').isNull(),1).otherwise(0)).alias('null_amount'),
    sum(when(col('payment_date').isNull(),1).otherwise(0)).alias('null_payment_date')
).show()

# COMMAND ----------

df_silver_payments.filter(col('amount')<0).show()

# COMMAND ----------

df_silver_payments.write.format('delta').mode('overwrite').saveAsTable('silver_payments')

# COMMAND ----------

df_silver_payments.show()

# COMMAND ----------

df_sellers.printSchema()

# COMMAND ----------

df_sellers.select(
    sum(when(col('seller_id').isNull(),1).otherwise(0)).alias("null_seller_id"),
    sum(when(col('seller_name').isNull(),1).otherwise(0)).alias('null_seller_name'),
    sum(when(col('city').isNull(),1).otherwise(0)).alias('null_city'),
    sum(when(col('rating').isNull(),1).otherwise(0)).alias('null_rating'),
    sum(when(col('joining_date').isNull(),1).otherwise(0)).alias('null_joining_date')
).show()


# COMMAND ----------

df_sellers.filter(~col('rating').between(1, 5)).show()

# COMMAND ----------

df_sellers.write.format('delta').mode('overwrite').saveAsTable('silver_sellers')

# COMMAND ----------

spark.table('silver_sellers').count()

# COMMAND ----------

spark.table('silver_order_items').display()

# COMMAND ----------

df_order_items_products = df_order_items.join(
    spark.table('silver_products'),
    ['product_id'],
    'inner'
)


# COMMAND ----------

df_order_items_products_categories = df_order_items_products.join(
    spark.table('silver_categories'),
    ['category_id'],
    'inner'
)


# COMMAND ----------

df_order_items_products_categories.withColumn('line_total', col('quantity') * col('price_at_purchase')).groupBy('category_name').agg(sum('line_total').alias('total_revenue')).show()

# COMMAND ----------

spark.table('silver_sellers').display()

# COMMAND ----------

df_silver_customers_orders = df_silver_customers.join(
    spark.table('silver_orders'),
    ['customer_id'],
    'inner'
)
df_silver_customer_orders_order_items = df_silver_customers_orders.join(
    spark.table('silver_order_items'),
    ['order_id'],
    'inner'
)

# COMMAND ----------

df_silver_customer_orders_order_items.withColumn('line_total', col('quantity') * col('price_at_purchase')).groupBy('customer_id','name').agg(sum('line_total').alias('total_revenue')).orderBy('customer_id').show()

# COMMAND ----------

df_silver_orders_order_items = df_silver_orders.join(
    spark.table('silver_order_items'),
    ['order_id'],
    'inner'
)
df_silver_orders_order_items = df_silver_orders_order_items.withColumn('line_total', col('quantity') * col('price_at_purchase')).withColumn('order_month', month('order_date')).withColumn('order_year', year('order_date'))
df_silver_orders_order_items.groupBy('order_year','order_month').agg(sum('line_total').alias('total_revenue')).orderBy('order_year','order_month').show()

# COMMAND ----------

df_silver_products_order_items = df_silver_products.join(
    spark.table('silver_order_items'),
    ['product_id'],
    'inner'
)
df_silver_products_order_items.select('product_id','product_name','line_total').groupBy('product_id','product_name').agg(sum('line_total').alias('total_revenue')).orderBy('total_revenue', ascending=False).show()

# COMMAND ----------

silver_order_items_products = spark.table('silver_order_items').join(
    spark.table('silver_products'),
    ['product_id'],
    'inner'
)
silver_order_items_products_sellers = silver_order_items_products.join(
    spark.table('silver_sellers'),
    ['seller_id'],
    'inner')
silver_order_items_products_sellers.select('seller_id','seller_name','line_total').groupBy('seller_id','seller_name').agg(sum('line_total').alias('total_revenue')).orderBy('total_revenue', ascending=False).show()

# COMMAND ----------

df_silver_payments.select('payment_id','payment_method','amount','payment_status').where(col('payment_status') == 'Success').groupBy('payment_method').agg(sum('amount').alias('total_amount')).orderBy('total_amount', ascending=False).show()

# COMMAND ----------

df_silver_orders.select(
    sum(when(col('is_completed') == 1, 1).otherwise(0)).alias('delivered_orders'),
    sum(when(col('order_status') == 'Cancelled', 1).otherwise(0)).alias('cancelled_orders'),
    sum(when(col('order_status') == 'Pending', 1).otherwise(0)).alias('pending_orders'),
    sum(when(col('order_status') == 'Out for Delivery', 1).otherwise(0)).alias('out_for_delivery_orders'),
    sum(when(col('order_status') == "Preparing", 1).otherwise(0)).alias("preparing_orders")
).show()

# COMMAND ----------

df_silver_orders.select('customer_id','customer_id').groupBy('customer_id').count().orderBy('count', ascending=False).show()

# COMMAND ----------

df_orders_order_items = df_orders.join(
    spark.table('silver_order_items'),
    ['order_id'],
    'inner'
)
df_orders_order_items.select('order_id','line_total').groupby('order_id').agg(sum('line_total').alias('revenue')).agg(avg('revenue').alias('avg_revenue')).show()

# COMMAND ----------

df_silver_orders_order_items = df_silver_orders.join(
    spark.table('silver_order_items'),
    ['order_id'],
    'inner'
)
df_silver_orders_order_items.select('order_status','line_total').where(col('is_completed')==1).agg(sum('line_total').alias('revenue')).show()


# COMMAND ----------

total_orders = df_silver_orders.count()
cancelled_orders = df_silver_orders.filter(col('order_status') == 'Cancelled').count()
cancellation_rate = (cancelled_orders / total_orders) * 100
print(f'The cancellation rate is {cancellation_rate}%')


# COMMAND ----------

from pyspark.sql.window import *

df_silver_customers_orders = spark.table('silver_customers').join(
    spark.table('silver_orders'),
    ['customer_id'],
    'inner'
)
df_silver_customers_orders_items = df_silver_customers_orders.join(
    spark.table('silver_order_items'),
    ['order_id'],
    'inner'
)

df_silver_customers_orders_items.select('customer_id','name','line_total').groupBy('customer_id','name').agg(sum('line_total').alias('total_revenue')).withColumn('rank', rank().over(Window.orderBy(col('total_revenue').desc()))).orderBy('rank').show()


# COMMAND ----------

df_silver_products_order_items = df_silver_products.join(
    spark.table('silver_order_items'),
    ['product_id'],
    'inner'
)
df_silver_products_order_items.select('product_id','product_name','quantity').groupBy('product_id','product_name').agg(sum('quantity').alias('total_quantity')).orderBy('total_quantity', ascending=False).show()

# COMMAND ----------

df_revenue = df_orders_order_items.withColumn('year',year('order_date')).withColumn('month',month('order_date')).groupBy('year','month').agg(sum('line_total').alias('revenue'))
window_spec = Window.partitionBy('year').orderBy('month')
df_revenue_with_lag = df_revenue.withColumn('prev_month_revenue', lag('revenue', 1).over(window_spec)).orderBy('year', 'month')
growth_percentage = df_revenue_with_lag.withColumn('growth_percentage', (col('revenue') - col('prev_month_revenue')) / col('prev_month_revenue') * 100)
growth_percentage.show()


# COMMAND ----------

spark.table('silver_products').show()

# COMMAND ----------

df_silver_category_products_order_items =df_categories.join(
    spark.table('silver_products'),
    ['category_id'],
    'inner'
).join(
    spark.table('silver_order_items'),
    ['product_id'],
    'inner'
)
df_revenue_by_category = df_silver_category_products_order_items.select('category_name','product_name','line_total').groupBy('category_name','product_name').agg(sum('line_total').alias('revenue'))
window_spec = Window.partitionBy('category_name').orderBy(col('revenue').desc())
df_revenue_by_category.withColumn('rank', rank().over(window_spec)).show()


# COMMAND ----------

silver_categories_order_items = spark.table('silver_categories').join(
    spark.table('silver_products'),
    ['category_id'],
    'inner'
).join(
    spark.table('silver_order_items'),
    ['product_id'],
    'inner'
)

df_category_revenue = silver_categories_order_items.groupBy("category_name").agg(sum('line_total').alias('revenue')).orderBy(col('revenue').desc())
display(df_category_revenue)

df_category_revenue.write.format('delta').mode('overwrite').saveAsTable('gold_category_revenue')

# COMMAND ----------

spark.table('gold_category_revenue').show()

# COMMAND ----------

silver_customers_orders_order_items_products_categories = spark.table('silver_customers').join(
    spark.table('silver_orders'),
    ['customer_id'],
    'inner'
).join(
    spark.table('silver_order_items'),
    ['order_id'],
    'inner'
).join(
    spark.table('silver_products'),
    ['product_id'],
    'inner'
).join(
    spark.table('silver_categories'),
    ['category_id'],
    'inner'
)
display(
    silver_customers_orders_order_items_products_categories.select(
        'customer_id',
        'name',
        'order_id',
        'order_date',
        'order_status',
        'product_id',
        'product_name',
        'category_name',
        'quantity',
        'line_total'
    ))


# COMMAND ----------

df_final = silver_customers_orders_order_items_products_categories.select(
    'customer_id',
    'name',
    'order_id',
    'order_date',
    'order_status',
    'product_id',
    'product_name',
    'category_name',
    'quantity',
    'line_total'
)

print(df_final.count())

# COMMAND ----------

df_final.write.format('delta').mode('overwrite').saveAsTable('gold_customer_orders_details')

# COMMAND ----------

spark.table('gold_customer_orders_details').show()

# COMMAND ----------

latest_order_date_row = (
    spark.table('silver_orders')
    .agg({'order_date': 'max'})
    .collect()[0]
)
latest_order_date = latest_order_date_row[0]

print("Latest Silver order date:", latest_order_date)

# COMMAND ----------

spark.table('silver_orders').filter(col('order_date')== '2025-12-02').show()

# COMMAND ----------

print(spark.table('silver_orders').count())


# COMMAND ----------

spark.table('silver_orders').groupBy('order_id').count().filter(col('count')> 1).show()

# COMMAND ----------

max_order_id = (
    spark.table('silver_orders')
    .agg(max('order_id').alias('max_order_id'))
    .collect()[0]['max_order_id']
)
print("Maximum order ID:", max_order_id)
df_new_orders = (
    df_orders.limit(3)
    .withColumn(
        'order_id',
        monotonically_increasing_id() + max_order_id + 1
    )
    .withColumn(
        'order_date',
        date_add(lit(latest_order_date), 3)
    )
)

display(df_new_orders)

# COMMAND ----------

df_new_orders_complete = (
    df_new_orders
    .withColumn('is_completed', lit(1))
    .withColumn('order_year', year('order_date'))
    .withColumn('order_month', month('order_date'))
)

# COMMAND ----------

from delta.tables import DeltaTable

silver_orders_delta = DeltaTable.forName(
    spark,
    'silver_orders'
)

silver_orders_delta.alias('target').merge(
    df_new_orders_complete.alias('source'),
    'target.order_id = source.order_id'
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()

# COMMAND ----------

print('Final Silver row count:')
print(spark.table('silver_orders').count())

# COMMAND ----------

spark.table('silver_orders') \
    .filter(col('order_id') >= 5061) \
    .show()

# COMMAND ----------

print("BRONZE :")
print("Customers:", spark.table("bronze_customers").count())
print("Categories:", spark.table("bronze_categories").count())
print("Products:", spark.table("bronze_products").count())
print("Sellers:", spark.table("bronze_sellers").count())
print("Orders:", spark.table("bronze_orders").count())
print("Order Items:", spark.table("bronze_order_items").count())
print("Payments:", spark.table("bronze_payments").count())


print("\nSILVER :")
print("Customers:", spark.table("silver_customers").count())
print("Categories:", spark.table("silver_categories").count())
print("Products:", spark.table("silver_products").count())
print("Sellers:", spark.table("silver_sellers").count())
print("Orders:", spark.table("silver_orders").count())
print("Order Items:", spark.table("silver_order_items").count())
print("Payments:", spark.table("silver_payments").count())


print("\nGOLD :")
print("Category Revenue:", spark.table("gold_category_revenue").count())
print("Customer Order Details:", spark.table("gold_customer_orders_details").count())

# COMMAND ----------

print("NULL CHECK :")

for table_name in [
    "silver_customers",
    "silver_categories",
    "silver_products",
    "silver_sellers",
    "silver_orders",
    "silver_order_items",
    "silver_payments"
]:
    df = spark.table(table_name)

    null_count = df.select(
        *[
            sum(
                when(col(c).isNull(), 1).otherwise(0)
            ).alias(c)
            for c in df.columns
        ]
    )

    print(f"\n{table_name}")
    null_count.show()

# COMMAND ----------

spark.table("silver_order_items").groupBy("order_id", "product_id") \
.count().filter(col("count") > 1) \
.show()

# COMMAND ----------

