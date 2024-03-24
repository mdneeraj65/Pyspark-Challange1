# Pyspark
## How to do pivot and unpivot in spark ?

Spark pivot() function is used to pivot/rotate the data from one DataFrame/Dataset column into multiple columns (transform row to column) and unpivot is used to transform it back (transform columns to rows).
Pivot() is an aggregation where one of the grouping columns values transposed into individual columns with distinct data.

Unpivot Spark DataFrame
Unpivot is a reverse operation, we can achieve by rotating column values into rows values. Spark SQL doesn’t have unpivot function hence will use the stack() function.