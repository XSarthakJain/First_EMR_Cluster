from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
input_file = 's3://myemrbucket13/inputfolder/product_data.csv'
output_file = 's3://myemrbucket13/outputfolder/'
df = spark.read.format('csv').option('header','true').option('inferSchema','true').option('path',input_file).load()
df1 = df.select(df['*']).filter(df['quantity'] > 10)
df1.write.format('json').option('path',output_file).option('header','true').mode('overwrite').save()
spark.stop()

