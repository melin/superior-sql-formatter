package com.github.melin.superior.sql.formatter.spark

import org.junit.Assert
import org.junit.Test

class SparkSqlFormatterTest {

    @Test
    fun simpleSelectSqlTest() {
        val sql = "select distinct name from users"
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SELECT DISTINCT name
            |FROM
            |  users
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }
}