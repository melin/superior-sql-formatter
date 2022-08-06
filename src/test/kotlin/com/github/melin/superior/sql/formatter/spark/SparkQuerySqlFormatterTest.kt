package com.github.melin.superior.sql.formatter.spark

import org.junit.Assert
import org.junit.Test

class SparkQuerySqlFormatterTest {

    @Test
    fun simpleSelectSqlTest() {
        val sql = """
            SELECT /*+ REPARTITION(100), COALESCE(500), REPARTITION_BY_RANGE(3, c) */ name, age FROM person 
            ORDER BY age DESC, name asc NULLS FIRST limit all;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT /*+ REPARTITION(100), COALESCE(500), REPARTITION_BY_RANGE(3, c) */
            |  name,
            |  age
            |FROM person
            |ORDER BY
            |  age DESC,
            |  name ASC NULLS FIRST
            |LIMIT ALL
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun simpleSelectSqlTest1() {
        val sql = "select distinct name, age from users as t limit 10;"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT DISTINCT
            |  name,
            |  age
            |FROM users AS t
            |LIMIT 10
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun simpleSelectSqlTest2() {
        val sql = "select date '2022-12-12' as test, CURRENT_DATE, name from demo where not name = 'ss' and id>100 sort by name, age"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  date '2022-12-12' AS test,
            |  current_date,
            |  name
            |FROM demo
            |WHERE
            |  NOT name = 'ss'
            |  AND id > 100
            |SORT BY
            |  name,
            |  age
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun firstLastSelectSqlTest() {
        val sql = """
            SELECT first(col), last(col IGNORE NULLS) FROM VALUES (10), (5), (20) AS tab(col);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  first(col),
            |  last(col IGNORE NULLS)
            |FROM VALUES
            |  (10),
            |  (5),
            |  (20) AS tab(col)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun simpleSelectSqlTest4() {
        val sql = """
            select array(1,2,3) as arr1, CURRENT_DATE, CURRENT_TIMESTAMP, CURRENT_USER,
            timestampadd(MICROSECOND, 5, TIMESTAMP'2022-02-28 00:00:00'),
            timestampadd(MONTH, -1, TIMESTAMP'2022-03-31 00:00:00'),
            timestampdiff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59'),
            timestampdiff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 12:00:00'),
            timestampdiff(YEAR, DATE'2021-01-01', DATE'1900-03-28'),
            cast(split("1,2,3", ",") as array<long>),
            cast('12' as bigint),
            CAST('11 23:4:0' AS INTERVAL DAY TO SECOND),
            position('bar', 'foobarbar'),
            position('bar', 'foobarbar', 5),
            POSITION('bar' IN 'foobarbar'),
            (1, 3.4, 'hello') as row1,
            exists(array(1, 2, 3), x -> x % 2 == 0),
            extract(MONTH FROM INTERVAL '2021-11' YEAR TO MONTH),
            extract(seconds FROM interval 5 hours 30 seconds 1 milliseconds 1 microseconds),
            extract(week FROM timestamp'2019-08-12 01:00:00.123456'),
            substring('Spark SQL' FROM -3),
            substring('Spark SQL' FROM 5 FOR 1),
            trim('    SparkSQL   '),
            trim(BOTH FROM '    SparkSQL   '),
            trim(BOTH 'SL' FROM 'SSparkSQLS'),
            trim('SL' FROM 'SSparkSQLS'),
            overlay('Spark SQL' PLACING '_' FROM 6),
            overlay('Spark SQL' PLACING 'CORE' FROM 7),
            overlay('Spark SQL' PLACING 'ANSI ' FROM 7 FOR 0),
            overlay(encode('Spark SQL', 'utf-8') PLACING encode('tructured', 'utf-8') FROM 2 FOR 4)
        """.trimIndent()

        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  array(1, 2, 3) AS arr1,
            |  current_date,
            |  current_timestamp,
            |  current_user,
            |  timestampadd(MICROSECOND, 5, TIMESTAMP '2022-02-28 00:00:00'),
            |  timestampadd(MONTH, -1, TIMESTAMP '2022-03-31 00:00:00'),
            |  timestampdiff(MONTH, TIMESTAMP '2021-02-28 12:00:00', TIMESTAMP '2021-03-28 11:59:59'),
            |  timestampdiff(MONTH, TIMESTAMP '2021-02-28 12:00:00', TIMESTAMP '2021-03-28 12:00:00'),
            |  timestampdiff(YEAR, DATE '2021-01-01', DATE '1900-03-28'),
            |  cast(split("1,2,3", ",") AS array<long>),
            |  cast('12' AS bigint),
            |  cast('11 23:4:0' AS INTERVAL DAY TO SECOND),
            |  position('bar', 'foobarbar'),
            |  position('bar', 'foobarbar', 5),
            |  position('bar' in 'foobarbar'),
            |  (1, 3.4, 'hello') AS row1,
            |  exists(array(1, 2, 3), x -> x % 2 == 0),
            |  extract(MONTH FROM INTERVAL '2021-11' YEAR TO MONTH),
            |  extract(SECONDS FROM INTERVAL 5 hours 30 seconds 1 milliseconds 1 microseconds),
            |  extract(WEEK FROM timestamp '2019-08-12 01:00:00.123456'),
            |  substring('Spark SQL' FROM -3),
            |  substring('Spark SQL' FROM 5 FOR 1),
            |  trim('    SparkSQL   '),
            |  trim(BOTH FROM '    SparkSQL   '),
            |  trim(BOTH 'SL' FROM 'SSparkSQLS'),
            |  trim('SL' FROM 'SSparkSQLS'),
            |  overlay('Spark SQL' PLACING '_' FROM 6),
            |  overlay('Spark SQL' PLACING 'CORE' FROM 7),
            |  overlay('Spark SQL' PLACING 'ANSI ' FROM 7 FOR 0),
            |  overlay(encode('Spark SQL', 'utf-8') PLACING encode('tructured', 'utf-8') FROM 2 FOR 4)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun simpleSelectDistrubuteBySqlTest() {
        val sql = "SELECT age, name FROM person DISTRIBUTE BY age;"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  age,
            |  name
            |FROM person
            |DISTRIBUTE BY age
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun simpleSelectClusterBySqlTest() {
        val sql = "SELECT age, name FROM person CLUSTER BY age;"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  age,
            |  name
            |FROM person
            |CLUSTER BY age
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun simpleSelectHavingSqlTest0() {
        val sql = "SELECT city, sum(quantity) AS sum FROM dealer GROUP BY city HAVING city = 'Fremont';"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  city,
            |  sum(quantity) AS sum
            |FROM dealer
            |GROUP BY city
            |HAVING
            |  city = 'Fremont'
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun simpleSelectHavingSqlTest1() {
        val sql = "SELECT city, sum(quantity) AS sum FROM dealer GROUP BY city HAVING max(quantity) > 15;"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  city,
            |  sum(quantity) AS sum
            |FROM dealer
            |GROUP BY city
            |HAVING
            |  max(quantity) > 15
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun simpleSelectGroupSqlTest1() {
        val sql = "SELECT id, sum(quantity) filter (WHERE car_model IN ('Honda Civic', 'Honda CRV')) AS `sum(quantity)` " +
                "FROM dealer GROUP BY id ORDER BY id;"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  id,
            |  sum(quantity) FILTER (
            |    WHERE
            |      car_model IN ('Honda Civic', 'Honda CRV')
            |  ) AS `sum(quantity)`
            |FROM dealer
            |GROUP BY id
            |ORDER BY id
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun simpleSelectGroupSqlTest2() {
        val sql = "SELECT city, car_model, sum(quantity) AS sum FROM dealer " +
                "GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ()) ORDER BY city"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  city,
            |  car_model,
            |  sum(quantity) AS sum
            |FROM dealer
            |GROUP BY GROUPING SETS (
            |    (city, car_model),
            |    (city),
            |    (car_model),
            |    ()
            |  )
            |ORDER BY city
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun simpleSelectGroupSqlTest3() {
        val sql = "SELECT city, car_model, sum(quantity) AS sum FROM dealer " +
                "GROUP BY city, car_model WITH ROLLUP ORDER BY city, car_model;"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  city,
            |  car_model,
            |  sum(quantity) AS sum
            |FROM dealer
            |GROUP BY
            |  city,
            |  car_model WITH ROLLUP
            |ORDER BY
            |  city,
            |  car_model
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun subQuerySqlTest() {
        val sql = "select d.name, d.test from (select * from demo) t"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  d.name,
            |  d.test
            |FROM (
            |    SELECT *
            |    FROM demo
            |  ) t
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun joinSelectSqlTest() {
        val sql = """
            SELECT * FROM demo1 as t1
            LEFT JOIN demo2 t2 on t1.col1 = t2.col2
            left join demo3 t3 on t1.sd = t3.col
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT *
            |FROM demo1 AS t1
            |  LEFT JOIN demo2 t2 ON t1.col1 = t2.col2
            |  LEFT JOIN demo3 t3 ON t1.sd = t3.col
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun inSubQuerySqlTest() {
        val sql = """
            SELECT * FROM sales.orders WHERE customer_id IN (SELECT customer_id FROM sales.customers WHERE city = 'San Jose')
            ORDER BY customer_id, order_date
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT *
            |FROM sales.orders
            |WHERE
            |  customer_id IN (
            |    SELECT customer_id
            |    FROM sales.customers
            |    WHERE
            |      city = 'San Jose'
            |  )
            |ORDER BY
            |  customer_id,
            |  order_date
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun existsSubQuerySqlTest() {
        val sql = """
            SELECT * FROM sales.orders o WHERE EXISTS (SELECT customer_id FROM sales.customers c WHERE o.customer_id = c.customer_id AND city = 'San Jose')
            ORDER BY o.customer_id, order_date
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT *
            |FROM sales.orders o
            |WHERE
            |  EXISTS (
            |    SELECT customer_id
            |    FROM sales.customers c
            |    WHERE
            |      o.customer_id = c.customer_id
            |      AND city = 'San Jose'
            |  )
            |ORDER BY
            |  o.customer_id,
            |  order_date
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun caseWhenQuerySqlTest() {
        val sql = """
            SELECT  user_id,item_id,behavior_type,
            case when substr(time,1,10)='2014-12-16' then 16 
            when substr(time,1,10)='2014-12-17' then 17
            when substr(time,1,10)='2014-12-18' then 18 else 0 end as day from user_test
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  user_id,
            |  item_id,
            |  behavior_type,
            |  CASE
            |    WHEN substr(time, 1, 10) = '2014-12-16' 16
            |    WHEN substr(time, 1, 10) = '2014-12-17' 17
            |    WHEN substr(time, 1, 10) = '2014-12-18' 18
            |  ELSE 0 END AS day
            |FROM user_test
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun caseWhenQuerySqlTest1() {
        val sql = """
            SELECT OrderID, Quantity,
            CASE 1=1 
                WHEN Quantity > 30 THEN 'The quantity is greater than 30'
                WHEN Quantity = 30 THEN 'The quantity is 30'
                ELSE 'The quantity is under 30'
            END AS QuantityText
            FROM OrderDetails;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  OrderID,
            |  Quantity,
            |  CASE 1 = 1
            |    WHEN Quantity > 30 'The quantity is greater than 30'
            |    WHEN Quantity = 30 'The quantity is 30'
            |  ELSE 'The quantity is under 30' END AS QuantityText
            |FROM OrderDetails
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun tvfQuerySqlTest1() {
        val sql = """
            SELECT * FROM range(6 + cos(3));
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT *
            |FROM range(6 + cos(3))
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun tvfQuerySqlTest2() {
        val sql = """
            SELECT inline(array(struct(1, 'a'), struct(2, 'b')))
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT inline(array(STRUCT(1, 'a'), STRUCT(2, 'b')))
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun inlineTableQuerySqlTest1() {
        val sql = """
            SELECT * FROM VALUES ("one", 1), ("two", 2), ("three", null) AS data(a, b);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT *
            |FROM VALUES
            |  ("one", 1),
            |  ("two", 2),
            |  ("three", null) AS data(a, b)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun cteQuerySqlTest1() {
        val sql = """
            with q1 as ( select key from q2 where key = '5'),
            q2 as ( select key from src where key = '5')
            select * from (select key from q1) a;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |WITH q1 AS (
            |  SELECT key
            |  FROM q2
            |  WHERE
            |    key = '5'
            |),
            |q2 AS (
            |  SELECT key
            |  FROM src
            |  WHERE
            |    key = '5'
            |)
            |SELECT *
            |FROM (
            |    SELECT key
            |    FROM q1
            |  ) a
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun cteQuerySqlTest2() {
        val sql = """
            WITH t(x, y) AS (SELECT 1, 2)
            SELECT * FROM t WHERE x = 1 AND y = 2;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |WITH t(x, y) AS (
            |  SELECT
            |    1,
            |    2
            |)
            |SELECT *
            |FROM t
            |WHERE
            |  x = 1
            |  AND y = 2
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun cteQuerySqlTest3() {
        val sql = """
            WITH t AS (
                WITH t2 AS (SELECT 1)
                SELECT * FROM t2
            )
            SELECT * FROM t;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |WITH t AS (
            |  WITH t2 AS (
            |    SELECT 1
            |  )
            |  SELECT *
            |  FROM t2
            |)
            |SELECT *
            |FROM t
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun cteQuerySqlTest4() {
        val sql = """
            SELECT max(c) FROM (
                WITH t(c) AS (SELECT 1)
                SELECT * FROM t
            );
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT max(c)
            |FROM (
            |    WITH t(c) AS (
            |      SELECT 1
            |    )
            |    SELECT *
            |    FROM t
            |  )
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun lateralViewSqlTest1() {
        val sql = """
            SELECT c_age, COUNT(1) FROM person
            LATERAL VIEW EXPLODE(ARRAY(30, 60)) t1 AS c_age
            LATERAL VIEW EXPLODE(ARRAY(40, 80)) t2 AS d_age
            GROUP BY c_age;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  c_age,
            |  COUNT(1)
            |FROM person
            |  LATERAL VIEW EXPLODE(
            |    ARRAY(30, 60)
            |  ) t1 AS c_age
            |  LATERAL VIEW EXPLODE(
            |    ARRAY(40, 80)
            |  ) t2 AS d_age
            |GROUP BY c_age
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun lateralViewSqlTest2() {
        val sql = """
            SELECT c_age, COUNT(1) FROM person
                LATERAL VIEW OUTER EXPLODE(ARRAY(30, 60)) AS c_age
                LATERAL VIEW OUTER EXPLODE(ARRAY(40, 80)) AS d_age 
            GROUP BY c_age;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  c_age,
            |  COUNT(1)
            |FROM person
            |  LATERAL VIEW OUTER EXPLODE(
            |    ARRAY(30, 60)
            |  ) AS c_age
            |  LATERAL VIEW OUTER EXPLODE(
            |    ARRAY(40, 80)
            |  ) AS d_age
            |GROUP BY c_age
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun pivotSqlTest1() {
        val sql = """
            SELECT * FROM person
            PIVOT (
                SUM(age) AS a, AVG(class) AS c
                FOR name IN ('John' AS john, 'Mike' AS mike)
            );
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT *
            |FROM person
            |  PIVOT (
            |    SUM(age) AS a,
            |    AVG(class) AS c
            |    FOR name IN ('John' AS john, 'Mike' AS mike)
            |  )
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun pivotSqlTest2() {
        val sql = """
            SELECT * FROM person
            PIVOT (
                SUM(age) AS a, AVG(class) AS c
                FOR (name, age) IN (('John', 30) AS c1, ('Mike', 40) AS c2)
            );
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT *
            |FROM person
            |  PIVOT (
            |    SUM(age) AS a,
            |    AVG(class) AS c
            |    FOR (name, age) IN (('John', 30) AS c1, ('Mike', 40) AS c2)
            |  )
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun samplingSqlTest1() {
        val sql = """
            SELECT * FROM test TABLESAMPLE (50 PERCENT);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT *
            |FROM test TABLESAMPLE (50 PERCENT)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun samplingSqlTest2() {
        val sql = """
            SELECT * FROM test TABLESAMPLE (50 ROWS);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT *
            |FROM test TABLESAMPLE (50 ROWS)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun samplingSqlTest3() {
        val sql = """
            SELECT * FROM test TABLESAMPLE (BUCKET 4 OUT OF 10);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT *
            |FROM test TABLESAMPLE (BUCKET 4 OUT OF 10)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun windowFunctionSqlTest1() {
        val sql = """
            SELECT name, dept, salary, RANK() OVER (salary) AS rank FROM employees;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  name,
            |  dept,
            |  salary,
            |  RANK() OVER (salary) AS rank
            |FROM employees
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun windowFunctionSqlTest2() {
        val sql = """
            SELECT name, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY salary) AS rank FROM employees;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  name,
            |  dept,
            |  salary,
            |  RANK() OVER (PARTITION BY dept ORDER BY salary) AS rank
            |FROM employees
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun windowFunctionSqlTest3() {
        val sql = """
            SELECT name, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN
                UNBOUNDED PRECEDING AND CURRENT ROW) AS dense_rank FROM employees;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  name,
            |  dept,
            |  salary,
            |  DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary
            |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            |  ) AS dense_rank
            |FROM employees
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun windowFunctionSqlTest4() {
        val sql = """
            SELECT name, salary,
            LAG(salary) OVER (PARTITION BY dept ORDER BY salary) AS lag,
            LEAD(salary, 1, 0) OVER (PARTITION BY dept ORDER BY salary) AS lead
            FROM employees;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  name,
            |  salary,
            |  LAG(salary) OVER (PARTITION BY dept ORDER BY salary) AS lag,
            |  LEAD(salary, 1, 0) OVER (PARTITION BY dept ORDER BY salary) AS lead
            |FROM employees
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun windowFunctionSqlTest5() {
        val sql = """
            SELECT id, v,
            LEAD(v, 0) IGNORE NULLS OVER w lead,
            LAG(v, 0) IGNORE NULLS OVER w lag,
            NTH_VALUE(v, 2) IGNORE NULLS OVER w nth_value,
            FIRST_VALUE(v) IGNORE NULLS OVER w first_value,
            LAST_VALUE(v) IGNORE NULLS OVER w last_value
            FROM test_ignore_null
            WINDOW w AS (ORDER BY id)
            ORDER BY id;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  id,
            |  v,
            |  LEAD(v, 0) IGNORE NULLS OVER w lead,
            |  LAG(v, 0) IGNORE NULLS OVER w lag,
            |  NTH_VALUE(v, 2) IGNORE NULLS OVER w nth_value,
            |  FIRST_VALUE(v) IGNORE NULLS OVER w first_value,
            |  LAST_VALUE(v) IGNORE NULLS OVER w last_value
            |FROM test_ignore_null
            |WINDOW w AS (ORDER BY id)
            |ORDER BY id
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun percentileFunctionSqlTest() {
        val sql = """
            SELECT percentile_cont(array(0.5, 0.4, 0.1)) WITHIN GROUP (ORDER BY col)
            FROM VALUES (0), (1), (2), (10) AS tab(col);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT percentile_cont(array(0.5, 0.4, 0.1)) WITHIN (GROUP ORDER BY col)
            |FROM VALUES
            |  (0),
            |  (1),
            |  (2),
            |  (10) AS tab(col)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun callProduceSqlTest() {
        val sql = """
            call show_metadata_table_files(table => 'test_hudi_demo', partition => "ds=20210811")
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |CALL show_metadata_table_files(
            |  table => 'test_hudi_demo',
            |  partition => "ds=20210811"
            |)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun datatunnelProduceSqlTest() {
        val sql = """
            datatunnel source("mysql") options(
            username="dataworks",
            password="dataworks2021",
            host='10.5.20.20',
            port=3306,
            databaseName='dataworks', tableName='dc_dtunnel_datasource', columns=["*"])
            sink("hive") options(databaseName="bigdata", tableName='hive_dtunnel_datasource', writeMode='overwrite', columns=["*"]);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |DATATUNNEL SOURCE("mysql") OPTIONS(
            |  username = "dataworks",
            |  password = "dataworks2021",
            |  host = '10.5.20.20',
            |  port = 3306,
            |  databaseName = 'dataworks',
            |  tableName = 'dc_dtunnel_datasource',
            |  columns = ["*"]
            |)
            |SINK("mysql") OPTIONS(
            |  databaseName = "bigdata",
            |  tableName = 'hive_dtunnel_datasource',
            |  writeMode = 'overwrite',
            |  columns = ["*"]
            |)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun datatunnelProduceSqlTest1() {
        val sql = """
            datatunnel 
            source('mysql') options(
                username='dataworks',
                password='dataworks2021',
                host='10.5.20.20',
                port=3306,
                resultTableName='tdl_dc_job',
                databaseName='dataworks', tableName='dc_job', columns=['*'])
            transform = 'select * from tdl_dc_job where type="spark_sql"'
            sink('log') options(numRows = 10)
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |DATATUNNEL SOURCE('mysql') OPTIONS(
            |  username = 'dataworks',
            |  password = 'dataworks2021',
            |  host = '10.5.20.20',
            |  port = 3306,
            |  resultTableName = 'tdl_dc_job',
            |  databaseName = 'dataworks',
            |  tableName = 'dc_job',
            |  columns = ['*']
            |)
            |TRANSFORM = 'select * from tdl_dc_job where type="spark_sql"'
            |SINK('mysql') OPTIONS(
            |  numRows = 10
            |)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun exportSqlTest1() {
        val sql = """
            export table raw_activity_flat PARTITION (year=2018, month=3, day=12) TO 'activity_20180312.csv' options(delimiter=';')
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |EXPORT TABLE raw_activity_flat PARTITION(year = 2018, month = 3, day = 12)
            |TO 'activity_20180312.csv' OPTIONS(
            |  delimiter = ';'
            |)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun exportSqlTest2() {
        val sql = """
            with
                tdl_raw_activity_qunaer as (select email, idnumber, wifi from raw_activity_flat where year=2018 and month=3 and partnerCode='qunaer' )
            export table tdl_raw_activity_qunaer TO 'activity_20180312.csv'
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |WITH tdl_raw_activity_qunaer AS (
            |  SELECT
            |    email,
            |    idnumber,
            |    wifi
            |  FROM raw_activity_flat
            |  WHERE
            |    year = 2018
            |    AND month = 3
            |    AND partnerCode = 'qunaer'
            |)
            |EXPORT TABLE tdl_raw_activity_qunaer TO 'activity_20180312.csv'
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun loadFileSqlTest1() {
        val sql = """
            load data '/user/dataworks/users/qianxiao/demo.csv' table tdl_spark_test options( delimiter=',',header='true');
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |LOAD DATA '/user/dataworks/users/qianxiao/demo.csv' tdl_spark_test OPTIONS(
            |  delimiter = ',',
            |  header = 'true'
            |)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }
}
