### superior-sql-formatter
Spark SQL 代码格式化，用于Superior数据中台 Studio SQL 格式化

```xml
<dependency>
    <groupId>io.github.melin</groupId>
    <artifactId>superior-sql-formatter</artifactId>
    <version>0.9.3</version>
</dependency>
```

### Deploy
> export GPG_TTY=$(tty)
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

### 参考
1. https://www.sqlstyle.guide/zh/
