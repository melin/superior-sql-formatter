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
            |INSERT INTO students
            |VALUES
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

    @Test
    fun insertTableSqlTest3() {
        val sql = """
            INSERT INTO students
            FROM applicants SELECT name, address, id applicants WHERE qualified = true;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |INSERT INTO students 
            |FROM applicants
            |SELECT
            |  name,
            |  address,
            |  id applicants
            |WHERE
            |  qualified = true
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun insertTableSqlTest4() {
        val sql = """
            INSERT INTO students PARTITION (birthday = date'2019-01-02')
            VALUES ('Amy Smith', '123 Park Ave, San Jose');
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |INSERT INTO students PARTITION(birthday = date'2019-01-02')
            |VALUES
            |  ('Amy Smith', '123 Park Ave, San Jose')
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun insertTableSqlTest5() {
        val sql = """
            INSERT INTO students (address, name, student_id) VALUES
            ('Hangzhou, China', 'Kent Yao', 11215016);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |INSERT INTO students (address, name, student_id)
            |VALUES
            |  ('Hangzhou, China', 'Kent Yao', 11215016)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun insertTableSqlTest6() {
        val sql = """
            INSERT INTO students PARTITION (student_id = 11215017) (address, name)
            VALUES
            (
            'Hangzhou, China', 'Kent Yao Jr.'
            );
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |INSERT INTO students PARTITION(student_id = 11215017) (address, name)
            |VALUES
            |  ('Hangzhou, China', 'Kent Yao Jr.')
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun insertTableSqlTest7() {
        val sql = """
            INSERT OVERWRITE students PARTITION (student_id = 222222)
            SELECT name, address FROM persons WHERE name = "Dora Williams";
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |INSERT OVERWRITE students PARTITION(student_id = 222222)
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
    fun mutilInsertSqlTest8() {
        val sql = """
            FROM staged_employees se
            INSERT INTO TABLE us_employees
            SELECT * WHERE se.cnty = 'US'
            INSERT INTO TABLE ca_employees
            SELECT * WHERE se.cnty = 'CA'
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |FROM staged_employees se
            |INSERT INTO TABLE us_employees 
            |SELECT *
            |WHERE
            |  se.cnty = 'US'INSERT INTO TABLE ca_employees 
            |SELECT *
            |WHERE
            |  se.cnty = 'CA'
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }
}
