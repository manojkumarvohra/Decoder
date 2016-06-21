# Decoder
This UDF decodes the input expression and match it against a set of values, if matched corresponding output is returned else default value is returned

-----
Usage
-----
*EXPR_DECODE(<String> expression, val1, output1, val2, output2....valn, outputn, defaultOutput)*

---------
Examples
--------
hive> SELECT EXPR_DECODE('hive','pig',false, 'hive', true, 'none');

*true*

hive> SELECT EXPR_DECODE('pig','pig',false, 'hive', true, 'none');

*false*

hive> SELECT EXPR_DECODE('pegion','pig',false, 'hive', true, 'none');

*none*

------------
Installation
------------

- checkout the repository
- make the package
- add the jar (without dependencies) to hive
- create temporary/permanent function expr_decode as 'com.bigdata.hive.udf.impl.ExpressionDecodeUDF'
