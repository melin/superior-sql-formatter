package com.github.melin.superior.sql.formatter.spark

import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.NamedExpressionContext
import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.SetQuantifierContext
import com.google.common.base.Strings
import com.google.common.collect.Iterables
import org.antlr.v4.runtime.tree.TerminalNodeImpl

class FormatterVisitor(val builder: StringBuilder) : SparkSqlParserBaseVisitor<Void>() {
    companion object {
        private const val INDENT = "  "
    }

    private var indent: Int = 0

    override fun visitSelectClause(ctx: SparkSqlParser.SelectClauseContext): Void? {
        append(indent, "SELECT")
        return super.visitSelectClause(ctx)
    }

    override fun visitSetQuantifier(ctx: SetQuantifierContext): Void? {
        if (ctx.DISTINCT() !== null) {
            append(indent, " DISTINCT")
        }
        return null
    }

    override fun visitFromClause(ctx: SparkSqlParser.FromClauseContext): Void? {
        append(indent, "FROM")
        append(indent, "\n", INDENT)
        return super.visitFromClause(ctx)
    }

    override fun visitJoinRelation(ctx: SparkSqlParser.JoinRelationContext): Void? {
        append(indent, "\n", INDENT)
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(child.text.uppercase()).append(" ")
            } else {
                visit(child)
            }
        }
        return null
    }

    override fun visitJoinType(ctx: SparkSqlParser.JoinTypeContext): Void? {
        ctx.children.forEach { child ->
            builder.append(child.text.uppercase()).append(" ")
        }
        return null
    }

    override fun visitJoinCriteria(ctx: SparkSqlParser.JoinCriteriaContext): Void? {
        builder.append(" ")
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(child.text.uppercase()).append(" ")
            } else {
                visit(child)
            }
        }
        return null
    }

    override fun visitComparisonOperator(ctx: SparkSqlParser.ComparisonOperatorContext): Void? {
        builder.append(" ").append(ctx.text).append(" ")
        return null
    }

    override fun visitPredicate(ctx: SparkSqlParser.PredicateContext?): Void {
        return super.visitPredicate(ctx)
    }

    override fun visitRelation(ctx: SparkSqlParser.RelationContext): Void? {
        return super.visitRelation(ctx)
    }

    override fun visitTableAlias(ctx: SparkSqlParser.TableAliasContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(" ").append(child.text.uppercase())
            } else {
                builder.append(" ").append(child.text)
            }
        }
        return null;
    }

    override fun visitNamedExpressionSeq(ctx: SparkSqlParser.NamedExpressionSeqContext): Void? {
        if (ctx.childCount > 1) {
            ctx.children.forEach { child ->
                if (child is NamedExpressionContext) {
                    append(indent, "\n", INDENT)
                    visit(child)
                } else if (child is TerminalNodeImpl) {
                    builder.append(child.text)
                } else {
                    builder.append("")
                }
            }
        } else {
            builder.append(' ')
            visit(Iterables.getOnlyElement(ctx.children))
        }
        builder.append('\n')

        return null;
    }

    override fun visitDereference(ctx: SparkSqlParser.DereferenceContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(child.text)
            } else {
                visit(child)
            }
        }

        return null;
    }

    override fun visitIdentifier(ctx: SparkSqlParser.IdentifierContext): Void? {
        builder.append(ctx.text)
        return null;
    }

    override fun visitColumnReference(ctx: SparkSqlParser.ColumnReferenceContext): Void? {
        builder.append(ctx.identifier().text)
        return null;
    }

    override fun visitStar(ctx: SparkSqlParser.StarContext): Void? {
        builder.append(ctx.text)
        return null;
    }

    private fun append(indent: Int, value: String): java.lang.StringBuilder? {
        return builder.append(indentString(indent))
            .append(value)
    }

    private fun append(indent: Int, value1: String, value2: String): java.lang.StringBuilder? {
        return builder.append(indentString(indent))
            .append(value1).append(value2)
    }

    private fun indentString(indent: Int): String? {
        return Strings.repeat(INDENT, indent)
    }
}