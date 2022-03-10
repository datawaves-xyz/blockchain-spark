# Blockchain Spark

Blockchain Spark lets you decode smart contract activity into DataFrame.


[![Auto test](https://github.com/tellery/blockchain-spark/actions/workflows/auto-test.yml/badge.svg?branch=master&event=push)](https://github.com/tellery/blockchain-spark/actions/workflows/auto-test.yml)

## Using this Repo

### Building

We use Maven for building Java UDFs used by blockchain-spark.

To compile, run tests, and build jars:

```shell
cd java
mvn package
```

Install blockchain-spark:

```shell
python3 setup.py install
```


### Running

To run a spark-shell with blockchain-spark and its dependencies on the classpath:

```shell
pyspark --jars target/blockchain=spark-$VERSION-SNAPSHOT-jar-with-dependencies.jar
```

### Running Tests

To run Python tests:

```shell
export SPARK_HOME=<location of local Spark installation>
python3 setup.py nosetests
```

## Usage


### Prerequisites


Before you start parsing events, you need to store logs and traces somewhere Apache Spark can access them
(usually an object store).


We recommend using [Ethereum ETL](https://github.com/blockchain-etl/ethereum-etl) to export them.


### Quickstart

Context initialization

```python
spark3 = Spark3(spark.sparkSession)
```

Get the decoded function calls and events of a certain contract as Dataframe

```python
contract = spark3.contract(address=..., abi=...)
function = contract.get_function_by_name("function_name")
event = contract.get_event_by_name("event_name")
```

Display the first 100 lines of function calls

```python
contract.get_function_by_name('atomicMatch_').filter('dt = "2022-01-01"').show(100)
```

Aggregate on events

```python
df = contract.get_event_by_name('OrdersMatched')
df.select("event_parameter.inputs.*", "dt").createOrReplaceTempView("opensea_order_matched")
spark.sql("""
select dt,count(1),sum(price) from
opensea_order_matched
where dt > "2022-01-01"
group by dt
order by dt
""").show()
```
