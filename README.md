
# Day 17 - Spark Window Functions

## Objective
Learn Spark window functions using Scala DataFrames.

## Dataset
15 employee records across 5 department IDs.

## Topics Covered
- row_number()
- rank()
- dense_rank()
- Window.partitionBy()
- Window.orderBy()
- Running salary totals
- lag() and lead()
- Salary differences within departments

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.9
- sbt 2.0.8
- Java 17

## Run
```bash
export JAVA_TOOL_OPTIONS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
sbt run
```
