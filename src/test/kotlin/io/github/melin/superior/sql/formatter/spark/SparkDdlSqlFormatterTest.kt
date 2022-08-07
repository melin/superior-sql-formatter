package io.github.melin.superior.sql.formatter.spark

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

    @Test
    fun createHudiTableSqlTest() {
        val sql = """
            create table test_hudi_demo ( 
                id int, 
                name string, 
                price double,
                ds string)
            using hudi    
            primary key (id)
            partitioned by (ds)
            COMMENT 'this is a comment'
            TBLPROPERTIES ('foo'='bar')
            lifeCycle 300;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |CREATE TABLE test_hudi_demo (
            |  id int,
            |  name string,
            |  price double,
            |  ds string
            |)
            |USING hudi
            |PRIMARY KEY (id)
            |PARTITIONED BY (ds)
            |LIFECYCLE 300
            |TBLPROPERTIES (
            |  'foo' = 'bar'
            |)
            |COMMENT 'this is a comment'
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun setConfigTest() {
        val sql = """
            set spark.sql.test=dsdfs
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SET spark.sql.test = dsdfs
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun createDatabaseTest() {
        val sql = """
            CREATE DATABASE IF NOT EXISTS customer_db COMMENT 'This is customer database' LOCATION '/user'
            WITH DBPROPERTIES (ID=001, Name='John');
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |CREATE DATABASE IF NOT EXISTS customer_db
            |COMMENT 'This is customer database'
            |LOCATION '/user'
            |WITH DBPROPERTIES (
            |  ID = 001,
            |  Name = 'John'
            |)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun showDatabasesTest() {
        val sql = """
            SHOW databases LIKE 'pay*';
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SHOW DATABASES LIKE 'pay*'
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun alterDatabasePropsTest() {
        val sql = """
            ALTER database inventory SET DBPROPERTIES ('Edited-by' = 'John', 'Edit-date' = '01/01/2001');
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |ALTER DATABASE inventory SET DBPROPERTIES (
            |  'Edited-by' = 'John',
            |  'Edit-date' = '01/01/2001'
            |)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun alterDatabaseLocationTest() {
        val sql = """
            ALTER database inventory SET LOCATION 'file:/temp/spark-warehouse/new_inventory.db';
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |ALTER DATABASE inventory SET LOCATION 'file:/temp/spark-warehouse/new_inventory.db'
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun replaceTableSqlTest() {
        val sql = """
            replace table test_hudi_demo ( 
                id int, 
                name string, 
                price double,
                ds string)
            using hudi
            partitioned by (ds)
            COMMENT 'this is a comment'
            TBLPROPERTIES ('foo'='bar')
            lifeCycle 300;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |REPLACE TABLE test_hudi_demo (
            |  id int,
            |  name string,
            |  price double,
            |  ds string
            |)
            |USING hudi
            |PARTITIONED BY (ds)
            |LIFECYCLE 300
            |TBLPROPERTIES (
            |  'foo' = 'bar'
            |)
            |COMMENT 'this is a comment'
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }
}
