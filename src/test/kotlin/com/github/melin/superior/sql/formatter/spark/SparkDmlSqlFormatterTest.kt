package com.github.melin.superior.sql.formatter.spark

import org.junit.Assert
import org.junit.Test

class SparkDmlSqlFormatterTest {

    @Test
    fun simpleSelectSqlTest() {
        val sql = "SELECT name, age FROM person ORDER BY age DESC, name asc NULLS FIRST;"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  name,
            |  age
            |FROM
            |  person
            |ORDER BY
            |  age DESC,
            |  name ASC NULLS FIRST
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun simpleSelectSqlTest1() {
        val sql = "select distinct name, age from users as t;"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT DISTINCT
            |  name,
            |  age
            |FROM
            |  users AS t
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun simpleSelectSqlTest2() {
        val sql = "select date '2022-12-12' as test, name from demo where name = 'ss' and id>100"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  date '2022-12-12' AS test,
            |  name
            |FROM
            |  demo
            |WHERE
            |  name = 'ss'
            |  AND id > 100
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun simpleSelectGroupSqlTest1() {
        val sql = "SELECT id, sum(quantity) FILTER (WHERE car_model IN ('Honda Civic', 'Honda CRV')) AS `sum(quantity)` " +
                "FROM dealer GROUP BY id ORDER BY id;"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT
            |  id,
            |  sum(quantity) FILTER (
            |    WHERE
            |      car_model IN ('Honda Civic', 'Honda CRV')
            |  ) AS `sum(quantity)`
            |FROM
            |  dealer
            |GROUP BY
            |  id
            |ORDER BY
            |  id
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
            |FROM
            |  dealer
            |GROUP BY
            |  GROUPING SETS (
            |    (city, car_model),
            |    (city),
            |    (car_model),
            |    ()
            |  )
            |ORDER BY
            |  city
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
            |FROM
            |  dealer
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
            |FROM
            |  (
            |    SELECT *
            |    FROM
            |      demo
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
            |FROM
            |  demo1 AS t1
            |  LEFT JOIN demo2 t2 ON t1.col1 = t2.col2
            |  LEFT JOIN demo3 t3 ON t1.sd = t3.col
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }
}
