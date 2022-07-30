package com.github.melin.superior.sql.formatter.spark

import org.junit.Assert
import org.junit.Test

class SparkCudSqlFormatterTest {

    @Test
    fun simpleSelectSqlTest() {
        val sql = """
            CREATE TABLE student_copy USING CSV AS SELECT * FROM student;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |CREATE TABLE student_copy USING CSV AS
            |SELECT *
            |FROM
            |  student
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }
}