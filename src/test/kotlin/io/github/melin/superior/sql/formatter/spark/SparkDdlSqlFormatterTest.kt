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
                name string not null  default 'test' comment  'sd', 
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
            |CREATE TABLE test_hudi_demo (
            |  id int,
            |  name string NOT NULL DEFAULT 'test' COMMENT 'sd',
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
    fun showCreateTableTest() {
        val sql = """
            SHOW CREATE table test AS SERDE;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SHOW CREATE TABLE test AS SERDE
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun showViewsTest() {
        val sql = """
            show views LIKE 'sam|suj|temp*';
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SHOW VIEWS LIKE 'sam|suj|temp*'
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

    @Test
    fun analyzeTest() {
        val sql = """
            ANALYZE table students COMPUTE STATISTICS FOR COLUMNS name;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |ANALYZE TABLE students COMPUTE STATISTICS FOR COLUMNS name
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun analyzeTest1() {
        val sql = """
            ANALYZE tables IN school_db COMPUTE STATISTICS NOSCAN;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |ANALYZE TABLES IN school_db COMPUTE STATISTICS NOSCAN
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun addColumnsTest1() {
        val sql = """
            ALTER table StudentInfo ADD columns (LastName string, DOB timestamp);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |ALTER TABLE ADD COLUMNS(
            |  LastName string,
            |  DOB timestamp
            |)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun dropColumnsTest1() {
        val sql = """
            ALTER table StudentInfo DROP columns (LastName, DOB);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |ALTER TABLE StudentInfo DROP COLUMNS (LastName, DOB)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun addPartitionsTest1() {
        val sql = """
            ALTER TABLE StudentInfo ADD IF NOT EXISTS PARTITION (age=18) PARTITION (age=20);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |ALTER TABLE StudentInfo ADD IF NOT EXISTS
            |  PARTITION(age = 18)
            |  PARTITION(age = 20)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun addColumnTest1() {
        val sql = """
            ALTER TABLE StudentInfo ALTER COLUMN FirstName COMMENT "new comment";
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |ALTER TABLE StudentInfo ALTER COLUMN FirstName COMMENT "new comment"
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun addColumnTest2() {
        val sql = """
            ALTER TABLE StudentInfo REPLACE COLUMNS (name string, ID int COMMENT 'new comment');
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |ALTER TABLE StudentInfo REPLACE COLUMNS (
            |  name string,
            |  ID int COMMENT 'new comment'
            |)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun alterColumnTest3() {
        val sql = """
            ALTER TABLE employee CHANGE name ename String;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |ALTER TABLE employee CHANGE name ename String
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun alterColumnTest4() {
        val sql = """
            ALTER TABLE demo rename column privce TO price
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |ALTER TABLE demo RENAME COLUMN privce TO price
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun alterViewTest1() {
        val sql = """
            ALTER VIEW tempdb1.v2 AS SELECT * FROM tempdb1.v1;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |ALTER VIEW tempdb1.v2 AS
            |SELECT *
            |FROM tempdb1.v1
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun alterViewTest2() {
        val sql = """
            ALTER VIEW tempdb1.v2 SET TBLPROPERTIES ('created.by.user' = "John", 'created.date' = '01-01-2001' );
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |ALTER VIEW tempdb1.v2 SET TBLPROPERTIES(
            |  'created.by.user' = "John",
            |  'created.date' = '01-01-2001'
            |)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun showTableExtendedTest1() {
        val sql = """
            SHOW table EXTENDED LIKE 'employee';
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SHOW TABLE EXTENDED LIKE 'employee'
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun showTableExtendedTest2() {
        val sql = """
            SHOW table EXTENDED  IN default LIKE 'employee' PARTITION (grade=1);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SHOW TABLE EXTENDED IN default LIKE 'employee' PARTITION(grade = 1)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun showColumnsTest1() {
        val sql = """
            SHOW COLUMNS IN customer IN salesdb;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |SHOW COLUMNS IN customer IN salesdb
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun explainTest1() {
        val sql = """
            EXPLAIN select k, sum(v) from values (1, 2), (1, 3) t(k, v) group by k;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |EXPLAIN SELECT
            |  k,
            |  sum(v)
            |FROM VALUES
            |  (1, 2),
            |  (1, 3) AS t(k, v)
            |GROUP BY k
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun describeQuertTest1() {
        val sql = """
            DESCRIBE QUERY WITH all_names_cte
            AS (SELECT name from person) SELECT * FROM all_names_cte;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |DESCRIBE QUERY WITH all_names_cte AS (
            |  SELECT name
            |  FROM person
            |)
            |SELECT *
            |FROM all_names_cte
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun describeQuertTest2() {
        val sql = """
            DESC QUERY VALUES(100, 'John', 10000.20D) AS employee(id, name, salary);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |DESC QUERY VALUES
            |  (100, 'John', 10000.20D) AS employee(id, name, salary)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun createViewTest1() {
        val sql = """
            CREATE OR REPLACE VIEW experienced_employee
            (ID COMMENT 'Unique identification number', Name) 
            COMMENT 'View for experienced employees'
            AS SELECT id, name FROM all_employee
                WHERE working_years > 5;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |CREATE OR REPLACE VIEW experienced_employee(
            |  ID COMMENT 'Unique identification number',
            |  Name
            |)
            |COMMENT 'View for experienced employees'
            |AS SELECT
            |  id,
            |  name
            |FROM all_employee
            |WHERE
            |  working_years > 5
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun createViewTest2() {
        val sql = """
            CREATE GLOBAL TEMPORARY VIEW IF NOT EXISTS subscribed_movies 
            AS SELECT mo.member_id, mb.full_name, mo.movie_title
                FROM movies AS mo INNER JOIN members AS mb 
                ON mo.member_id = mb.id;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |CREATE GLOBAL TEMPORARY VIEW IF NOT EXISTS subscribed_movies
            |AS SELECT
            |  mo.member_id,
            |  mb.full_name,
            |  mo.movie_title
            |FROM movies AS mo
            |  INNER JOIN members AS mb ON mo.member_id = mb.id
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun commentTableTest1() {
        val sql = """
            COMMENT ON table my_table IS 'This is my table';
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |COMMENT ON TABLE my_table IS 'This is my table'
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun commentTableTest2() {
        val sql = """
            COMMENT ON TABLE my_table IS NULL
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |COMMENT ON TABLE my_table IS NULL
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun truncateTableTest2() {
        val sql = """
            TRUNCATE TABLE Student partition(age=10);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |TRUNCATE TABLE Student PARTITION(age = 10)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun msckTableTest2() {
        val sql = """
            MSCK REPAIR TABLE t1;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |MSCK REPAIR TABLE t1
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun dropTableTest() {
        val sql = """
            DROP TABLE IF EXISTS employeetable;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |DROP TABLE IF EXISTS employeetable
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun createIndexTest0() {
        val sql = """
            CREATE index i1 ON a.b.c USING BTREE (col1)
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |CREATE INDEX i1.BTREE ON a.b.c
            |USING BTREE(col1)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun createIndexTest1() {
        val sql = """
            CREATE index IF NOT EXISTS i1 ON TABLE a.b.c USING BTREE (
                col1 OPTIONS ('k1'='v1'), 
                col2 OPTIONS ('k2'='v2')
            ) OPTIONS ('k3'='v3', 'k4'='v4')
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |CREATE INDEX IF NOT EXISTS i1.BTREE ON TABLE a.b.c
            |USING BTREE(
            |  col1 OPTIONS ('k1' = 'v1'),
            |  col2 OPTIONS ('k2' = 'v2')
            |)
            |OPTIONS (
            |  'k3' = 'v3',
            |  'k4' = 'v4'
            |)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun createIndexTest2() {
        val sql = """
            CREATE index IF NOT EXISTS i1 ON a.b.c (
                col1 OPTIONS ('k1'='v1'), 
                col2 OPTIONS ('k2'='v2')
            ) 
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |CREATE INDEX IF NOT EXISTS i1 ON a.b.c
            |(
            |  col1 OPTIONS ('k1' = 'v1'),
            |  col2 OPTIONS ('k2' = 'v2')
            |)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun dropIndexTest() {
        val sql = """
            DROP index IF EXISTS test_index ON TABLE bigdata.demo
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |DROP INDEX IF EXISTS test_index ON TABLE bigdata.demo
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }
}
