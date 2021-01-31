# SF Crime statistics Project

## Project questions

### How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
By increasing maxOffsetsPerTrigger I observed higher throughput, but batch processing time also increased, so processingTime should be adjusted based on the time it takes to process a batch.

### What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?
Tweaking spark.driver.memory, spark.executor.memory didn't produce any significant results most likely due to small size of our dataset. But they are worth optimizing for larger datasets. Also, maxOffsetsPerTrigger set to 250000, gave me the best throughput. 


## Project Files
- kafka_server.py
- producer_server.py
- consumer_server.py
- data_stream.py


## Usage

### Producer
```
python kafka_server.py
```

### Consumer
```
python consumer_server.py
```

### Spark job
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py
```

