from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
from py4j.protocol import Py4JError

# Configuration:
# Show Table: Set showTable to True to show table of values satisfying criteria.
showTable = False

# Rows: Adjust the maximum values viewed in the table.
rows = 1000

# Greenwald-Khanna Algorithm Precision:
# Range 0-1. Set to 0 by default.
# 0 calculates exact quantiles, but is computationally expensive.
# Values above 1 are treated as though they are 1
relativeError = 0

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

try:
    median = abeToMia.approxQuantile("MktFare", [0.5], relativeError)
except ValueError:
    raise Exception("Configuration Error: Please set the value of |precision| in medianFare.py to a numerical value of 0 or above")

print("The median fare from " + str(origin) + " to " + str(dest) + " is " + str(median) + " dollars.")

if showTable is not False:
    try:
        abeToMia.groupBy("MktFare").count().orderBy("MktFare").show(rows)
    except Py4JError:
        raise Exception("Configuration Error: Please set the value of |rows| "
                        "in medianFare.py to a whole number value above 0")


