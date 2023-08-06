package io.github.melin.superior.sql.formatter.spark

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
    fun mergTableFileTest() {
        val sql = """
            merge table bigdata.test_user11_dt PARTITION (ds=20211204) options(dd='ss', ssd=12)
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |MERGE INTO bigdata.test_user11_dt PARTITION(ds = 20211204)
            |OPTIONS(
            |  dd = 'ss',
            |  ssd = 12
            |)
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

    @Test
    fun deleteSqlTest1() {
        val sql = """
            delete from hudi_mor_tbl where id % 2 = 0;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |DELETE FROM hudi_mor_tbl
            |WHERE
            |  id % 2 = 0
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun updateSqlTest1() {
        val sql = """
            update hudi_mor_tbl set price = price * 2, ts = 1111 where id = 1;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |UPDATE hudi_mor_tbl SET
            |  price = price * 2,
            |  ts = 1111
            |WHERE
            |  id = 1
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun mergeUpdateSqlTest1() {
        val sql = """
            merge into hudi_mor_tbl as target
            using merge_source as source
            on target.id = source.id
            when matched then update set *
            when not matched then insert *;
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |MERGE INTO hudi_mor_tbl AS target 
            |USING merge_source AS source
            |ON target.id = source.id
            |WHEN MATCHED THEN
            |  UPDATE SET *
            |WHEN NOT MATCHED THEN
            |  INSERT *
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }

    @Test
    fun mergeUpdateSqlTest2() {
        val sql = """
            merge into hudi_cow_pt_tbl as target
            using (
              select id, name, '1000' as ts, flag, dt, hh from merge_source2
            ) source
            on target.id = source.id
            when matched and flag != 'delete' then
             update set id = source.id, name = source.name, ts = source.ts, dt = source.dt, hh = source.hh
            when matched and flag = 'delete' then delete
            when not matched then
             insert (id, name, ts, dt, hh) values(source.id, source.name, source.ts, source.dt, source.hh);
        """.trimIndent()
        val formatSql = SparkSqlFormatter.formatSql(sql)
        val expected = """
            |MERGE INTO hudi_cow_pt_tbl AS target 
            |USING (
            |  SELECT
            |    id,
            |    name,
            |    '1000' AS ts,
            |    flag,
            |    dt,
            |    hh
            |  FROM merge_source2
            |) source
            |ON target.id = source.id
            |WHEN MATCHED AND flag != 'delete' THEN
            |  UPDATE SET
            |    id = source.id,
            |    name = source.name,
            |    ts = source.ts,
            |    dt = source.dt,
            |    hh = source.hh
            |WHEN MATCHED AND flag = 'delete' THEN
            |  DELETE
            |WHEN NOT MATCHED THEN
            |  INSERT (idnametsdthh) VALUES (source.id, source.name, source.ts, source.dt, source.hh)
        """.trimMargin()
        Assert.assertEquals(expected, formatSql)
    }
}
