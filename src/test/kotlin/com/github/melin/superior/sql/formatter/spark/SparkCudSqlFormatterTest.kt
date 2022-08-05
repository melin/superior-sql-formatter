package com.github.melin.superior.sql.formatter.spark

import org.junit.Assert
import org.junit.Test

class SparkCudSqlFormatterTest {
    @Test
    fun insertTableSqlTest() {
        val sql = """
            INSERT INTO students PARTITION (student_id = 444444) SELECT name, address FROM persons WHERE name = "Dora Williams";
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |INSERT INTO students PARTITION(student_id = 444444)
            |SELECT
            |  name,
            |  address
            |FROM persons
            |WHERE
            |  name = "Dora Williams"
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }
}