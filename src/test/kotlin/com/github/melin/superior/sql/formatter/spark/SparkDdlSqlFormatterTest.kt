package com.github.melin.superior.sql.formatter.spark

import org.junit.Assert
import org.junit.Test

class SparkDdlSqlFormatterTest {

    @Test
    fun ctasSqlTest() {
        val sql = """
            CREATE TABLE student_copy USING CSV AS SELECT * FROM student where age between 10 and 12 
            and address not like '%hangzou%';
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |CREATE TABLE student_copy USING CSV AS
            |SELECT *
            |FROM student
            |WHERE
            |  age BETWEEN 10 AND 12
            |  AND address NOT LIKE '%hangzou%'
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun createTableSqlTest() {
        val sql = """
            CREATE TABLE student (id INT, name STRING, age INT) USING CSV;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |CREATE TABLE student (
            |  id INT,
            |  name STRING,
            |  age INT
            |)
            |USING CSV
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }
}