### superior-sql-formatter
Spark SQL 代码格式化，用于Superior数据中台 Studio SQL 格式化

```xml
<dependency>
    <artifactId>superior-sql-formatter</artifactId>
    <groupId>io.github.melin.superior.sql.formatter</groupId>
    <version>0.8.0</version>
</dependency>
```

### Deploy
> mvn clean deploy -Prelease

### Example:
```kotlin
val sql = "select distinct name, age from users as t limit 10;"
val formatSql = SparkSqlFormatter.formatSql(sql)
val expected = """
    |SELECT DISTINCT
    |  name,
    |  age
    |FROM users AS t
    |LIMIT 10
""".trimMargin()
```