package io.github.melin.superior.sql.formatter.spark

import io.github.melin.superior.sql.formatter.antlr4.ParseErrorListener
import io.github.melin.superior.sql.formatter.antlr4.ParseException
import io.github.melin.superior.sql.formatter.antlr4.SparkSqlPostProcessor
import io.github.melin.superior.sql.formatter.antlr4.UpperCaseCharStream
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.atn.PredictionMode
import org.apache.commons.lang3.StringUtils

object SparkSqlFormatter {

    fun formatSql(sql: String): String? {
        val trimSql = StringUtils.trim(sql)

        val charStream = UpperCaseCharStream(
            CharStreams.fromString(trimSql)
        )
        val lexer = SparkSqlLexer(charStream)
        lexer.removeErrorListeners()
        lexer.addErrorListener(ParseErrorListener())

        val tokenStream = CommonTokenStream(lexer)
        val parser = SparkSqlParser(tokenStream)
        parser.addParseListener(SparkSqlPostProcessor())
        parser.removeErrorListeners()
        parser.addErrorListener(ParseErrorListener())
        parser.interpreter.predictionMode = PredictionMode.SLL

        val builder = StringBuilder()
        val sqlVisitor = FormatterVisitor(builder)
        try {
            // first, try parsing with potentially faster SLL mode
            sqlVisitor.visit(parser.singleStatement())
        } catch (e: ParseException) {
            if (StringUtils.isNotBlank(e.command)) {
                throw e;
            } else {
                throw e.withCommand(trimSql)
            }
        }

        return builder.toString();
    }
}