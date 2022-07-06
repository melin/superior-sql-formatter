package com.github.melin.superior.sql.formatter.spark

import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.SetQuantifierContext
import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.TableNameContext
import com.google.common.base.Strings
import com.google.common.collect.Iterables

class FormatterVisitor(val builder: StringBuilder) : SparkSqlParserBaseVisitor<Void>() {
    companion object {
        private const val INDENT = "   "
    }

    private var indent: Int = 0

    override fun visitSelectClause(ctx: SparkSqlParser.SelectClauseContext): Void? {
        append(indent, "SELECT")
        return super.visitSelectClause(ctx)
    }

    override fun visitSetQuantifier(ctx: SetQuantifierContext): Void? {
        if (ctx.DISTINCT() !== null) {
            builder.append(" DISTINCT")
        }
        return null
    }

    override fun visitFromClause(ctx: SparkSqlParser.FromClauseContext): Void? {
        append(indent, "FROM")
        builder.append('\n')
        append(indent, "  ")
        return super.visitFromClause(ctx)
    }

    override fun visitRelation(ctx: SparkSqlParser.RelationContext): Void? {
        var node = ctx.getChild(0)
        if (node is TableNameContext) {
            builder.append(node.multipartIdentifier().text)
            return null
        } else {
            return super.visitRelation(ctx)
        }
    }

    override fun visitNamedExpression(ctx: SparkSqlParser.NamedExpressionContext): Void? {
        if (ctx.childCount > 1) {

        } else {
            builder.append(' ')
            visit(Iterables.getOnlyElement(ctx.children))
        }
        builder.append('\n')

        return null
    }

    override fun visitColumnReference(ctx: SparkSqlParser.ColumnReferenceContext): Void? {
        builder.append(ctx.identifier().text)
        return null;
    }

    private fun append(indent: Int, value: String): java.lang.StringBuilder? {
        return builder.append(indentString(indent))
            .append(value)
    }

    private fun indentString(indent: Int): String? {
        return Strings.repeat(INDENT, indent)
    }
}