# Airfare Pricing

### Description:

This model was built to calculate the future price of airline tickets based
on historical data.  Travel agencies and their customers are the primary
beneficiaries of this information.  For example, consumers are more willing to
purchase tickets if they believe they are getting a good deal, where price
may be lower year-to-date, and Agencies can encourage customers to purchase
tickets if prices are estimated to go up in the near future.

### Overview:

The module medianFare.py uses Spark to return price based on the input of an
origin airport and destination airport from a .csv file with the appropriate
information to make these calculations.

The .csv file should contain 3 columns in this order:
1. origin airport code (e.g. ABE)
2. destination airport code (e.g MIA)
3. Market Fare (in USD)

A formatted .csv file **OriginDestFare.csv** is provided in this repository for
testing purposes, which consists of 2019 Q1 data. 

The program will calculate the median using the approxQuantile method in
Spark, which utilizes the Greenwald-Khanna Algorithm. This algorithm was
chosen based on two main factors: 

1. It does not need prior knowledge of the
length of the input, which works well for large data sets like this; 

2. Precise target precision.  We set target precision so that the
exact quantiles are computed, though this can be computationally expensive
especially with larger data sets.  
 
### Instructions:

You must have Apache Spark set up in your system.

Download medianFare.py from this repository. Make sure
this file and the .csv file  are in the same directory.
 
Navigate to this directory in command prompt.  We will use the spark-submit
method on our .py file **while also providing** our origin and destination
airport codes
  
```
spark-submit medianFare.py (Origin) (Destination)
```

For example, if we wanted to find the median airfare between ABE and MIA, our
 prompt would look like this:
 
```
spark-submit medianFare.py ABE MIA
```

The program will return the median airfare for all flights that match the
parameters.

```
The median fare from ABE to MIA is [178.0] dollars.
``` 

This program also contains some customization options in the Configuration
section of medianFare.py. For example, if working with a large data set and
a target precision value of 0 (which calculates exact quantiles) is too
expensive, this value can be adjusted accordingly to the user's preference.

### Data Sources:
Data obtained from Bureau of Transportation Statistics at https://www.transtats.bts.gov/

Data set is based off figures from the Airline Origin and Destination Survey (DB1B)
which represents a 10% sample of tickets from reporting carriers.

