package com.github.melin.superior.sql.formatter.spark

import org.junit.Assert
import org.junit.Test

class SparkCudSqlFormatterTest {
    @Test
    fun insertTableSqlTest() {
        val sql = """
            INSERT INTO table students PARTITION (student_id = 444444) SELECT name, address FROM persons WHERE name = "Dora Williams";
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |INSERT INTO TABLE students PARTITION(student_id = 444444)
            |SELECT
            |  name,
            |  address
            |FROM persons
            |WHERE
            |  name = "Dora Williams"
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun insertTableSqlTest1() {
        val sql = """
            INSERT INTO students VALUES ('Bob Brown', '456 Taylor St, Cupertino', 222222),
            ('Cathy Johnson', '789 Race Ave, Palo Alto', 333333);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |INSERT INTO students VALUES
            |  ('Bob Brown', '456 Taylor St, Cupertino', 222222),
            |  ('Cathy Johnson', '789 Race Ave, Palo Alto', 333333)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun insertTableSqlTest2() {
        val sql = """
            INSERT INTO students TABLE 
            visiting_students;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |INSERT INTO students TABLE visiting_students
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }
}