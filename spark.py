from pyspark.sql import SparkSession
from pyspark.sql import Row
import cmd, sys
    
origin = sys.argv[1]
dest = sys.argv[2]

spark = SparkSession \
        .builder \
        .config("spark.sql.warehouse.dir", "file:///C:/temp") \
        .appName("SparkSQL").getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(Origin=str(fields[0]), Dest=str(fields[1]), MktFare=int(
        fields[2]))


lines = spark.sparkContext.textFile("OriginDestFare.csv")
prices = lines.map(mapper)

schemaPrices = spark.createDataFrame(prices).cache()
schemaPrices.createOrReplaceTempView("prices")

abeToMia = spark.sql("SELECT * FROM prices WHERE Origin = '" + str(origin) + "' AND Dest = '" + str(dest) + "'")

for value in abeToMia.collect():
    print(value)

median = abeToMia.approxQuantile("MktFare", [0.5], 0.25)
print("The median fare from " + str(origin) + " to " + str(dest) + " is " + str(median) + " dollars.")

