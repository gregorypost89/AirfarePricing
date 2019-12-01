# Airfare Pricing

### Description:

Model using Spark to return price based on two airports:

### Overview:

The .csv file contains 3 columns in this order:
1. origin airport code (e.g. ABE)
2. destination airport code
3. Market Fare (in USD)

The module spark.py will return the median airfare for a provided origin
 airport code and provided destination airport code.
 
### Instructions:

You must have Apache Spark set up in your system.

Download OriginDestFare.csv and spark.py from this repository. Make sure both
 files are in the same directory.
 
Navigate to this directory in command prompt.  We will use the spark-submit
 method on our .py file **while also providing** our origin and destination
  airport codes
  
```
spark-submit spark.py (Origin) (Destination)
```

For example, if we wanted to find the median airfare between ABE and MIA, our
 prompt would look like this:
 
```
spark-submit spark.py ABE MIA
```

The program will return the median airfare for all flights that match the
 parameters.
 
If you would like to view the individual columns returned, uncomment lines 29
-30 in spark.py and the program will print these columns before providing the
 median value.  


