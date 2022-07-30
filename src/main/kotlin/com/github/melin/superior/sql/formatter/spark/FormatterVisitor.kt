package com.github.melin.superior.sql.formatter.spark

import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.ExpressionContext
import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.GroupingAnalyticsContext
import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.NamedExpressionContext
import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.NumericLiteralContext
import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.PredicateContext
import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.QuotedIdentifierAlternativeContext
import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.SetQuantifierContext
import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.StringLiteralContext
import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.TableNameContext
import com.google.common.base.Strings
import com.google.common.collect.Iterables
import org.antlr.v4.runtime.tree.TerminalNodeImpl
import org.apache.commons.lang3.StringUtils

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
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                append(indent, "FROM", "\n")
            } else {
                return visit(child)
            }
        }

        return null
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

    override fun visitPredicate(ctx: SparkSqlParser.PredicateContext): Void? {
        return super.visitPredicate(ctx)
    }

    override fun visitWhereClause(ctx: SparkSqlParser.WhereClauseContext): Void? {
        builder.append("\n")
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                append(indent, "WHERE", "\n")
            } else {
                indent++
                visit(child)
                indent--
            }
        }

        return null;
    }

    // booleaExpression
    override fun visitLogicalBinary(ctx: SparkSqlParser.LogicalBinaryContext): Void? {
        var first = true
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append("\n")
                append(indent, child.text.uppercase(), " ")
            } else {
                if (first) {
                    append(indent)
                    first = false
                }
                visit(child)
            }
        }

        return null;
    }

    override fun visitPredicated(ctx: SparkSqlParser.PredicatedContext): Void? {
        if (ctx.predicate() != null) {
            append(indent, INDENT)
            visit(ctx.getChild(0))
            builder.append(" ")
            (ctx.getChild(1) as PredicateContext).children.forEach { child ->
                if (child is TerminalNodeImpl) {
                    val text = child.text
                    builder.append(child.text)
                    if (!"(".equals(text) && !")".equals(text)) {
                        builder.append(" ")
                    }
                } else {
                    visit(child)
                }
            }
        } else {
            ctx.children.forEach { child ->
                if (child is TerminalNodeImpl) {
                    builder.append(child.text)
                } else {
                    visit(child)
                }
            }
        }
        return null;
    }

    override fun visitValueExpressionDefault(ctx: SparkSqlParser.ValueExpressionDefaultContext): Void? {
        return super.visitValueExpressionDefault(ctx)
    }

    override fun visitRelation(ctx: SparkSqlParser.RelationContext): Void? {
        ctx.children.forEach { child ->
            if (child is TableNameContext) {
                append(indent, INDENT)
            }

            visit(child)
        }
        return null
    }

    override fun visitAliasedQuery(ctx: SparkSqlParser.AliasedQueryContext): Void? {
        builder.append(INDENT).append("(").append("\n")

        indent += 2
        visit(ctx.getChild(1)) // sub query sql
        indent -= 2

        append(indent, "\n", INDENT)
        builder.append(")")

        visit(ctx.getChild(3)) // alias name

        return null
    }

    override fun visitTableAlias(ctx: SparkSqlParser.TableAliasContext): Void? {
        ctx.children?.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(" ").append(child.text.uppercase())
            } else {
                builder.append(" ").append(child.text)
            }
        }
        return null;
    }

    override fun visitQueryOrganization(ctx: SparkSqlParser.QueryOrganizationContext): Void? {
        if (ctx.children == null) {
            return null
        }

        builder.append("\n")
        append(indent)

        var first = true
        ctx.children?.forEach { child ->
            if (child is TerminalNodeImpl) {
                val text = child.text.uppercase()
                if (!",".equals(text)) { // 处理orderby 多个字段
                    if (first) {
                        first = false
                    } else {
                        builder.append(" ")
                    }
                    builder.append(text)
                } else {
                    builder.append(text)
                }
            } else {
                visit(child)
            }
        }

        return null;
    }

    override fun visitSortItem(ctx: SparkSqlParser.SortItemContext): Void? {
        builder.append("\n")
        append(indent, INDENT)

        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(" ").append(child.text.uppercase())
            } else {
                visit(child)
            }
        }

        return null;
    }

    override fun visitAggregationClause(ctx: SparkSqlParser.AggregationClauseContext): Void? {
        builder.append("\n")
        append(indent)

        var first = true
        ctx.children?.forEach { child ->
            if (child is TerminalNodeImpl) {
                val text = child.text.uppercase()
                if (!",".equals(text)) { // 处理orderby 多个字段
                    if (first) {
                        first = false
                    } else {
                        builder.append(" ")
                    }
                    builder.append(text)
                } else {
                    builder.append(text)
                }
            } else if (child is ExpressionContext) {
                builder.append("\n")
                append(indent, INDENT)
                visit(child)
            } else {
                visit(child)
            }
        }

        return null
    }

    override fun visitGroupByClause(ctx: SparkSqlParser.GroupByClauseContext): Void? {
        builder.append("\n")
        append(indent, INDENT)

        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(" ").append(child.text.uppercase())
            } else if (child is GroupingAnalyticsContext) {
                var first = true;
                child.children.forEach {ch ->
                    if (ch is TerminalNodeImpl) {
                        val text = ch.text.uppercase()
                        if ("(".equals(text)) {
                            builder.append(" ").append("(").append("\n")
                        } else if (")".equals(text)) {
                            builder.append("\n")
                            append(indent, INDENT,")")
                        } else if (",".equals(text)) {
                            builder.append(",").append("\n")
                        } else {
                            if (first) {
                                first = false
                            } else {
                                builder.append(" ")
                            }
                            builder.append(text)
                        }
                    } else {
                        append(indent, INDENT)
                        visit(ch)
                    }
                }
            } else {
                visit(child)
            }
        }

        return null
    }

    override fun visitGroupingSet(ctx: SparkSqlParser.GroupingSetContext): Void? {
        var first = true;
        ctx.children.forEach {child ->
            if (child is TerminalNodeImpl) {
                val text = child.text.uppercase()
                if ("(".equals(text)) {
                    builder.append("  ").append("(")
                } else if (")".equals(text)) {
                    builder.append(")")
                } else if (",".equals(text)) {
                    builder.append(", ")
                } else {
                    if (first) {
                        first = false
                    } else {
                        builder.append(" ")
                    }
                    builder.append(text)
                }
            } else {
                visit(child)
            }
        }

        return null
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

    override fun visitFunctionCall(ctx: SparkSqlParser.FunctionCallContext): Void? {
        run outside@{
            ctx.children.forEach { child ->
                if (child is TerminalNodeImpl) {
                    val text = child.text;
                    if (",".equals(text)) {
                        builder.append(text)
                        builder.append(" ")
                    } else if (StringUtils.endsWithIgnoreCase("filter", text)) {
                        return@outside
                    } else {
                        builder.append(text)
                    }
                } else {
                    visit(child)
                }
            }
        }

        if (ctx.where != null) {
            builder.append(" FILTER (").append("\n")
            indent ++
            append(indent, INDENT, "WHERE")
            indent ++
            builder.append("\n")
            visit(ctx.where)
            indent -= 2
            builder.append("\n")
            append(indent, INDENT, ")")
        }

        return null
    }

    override fun visitNamedExpression(ctx: NamedExpressionContext): Void? {
        var first = true
        ctx.children.forEach { child ->
            if (first) {
                first = false
            } else {
                builder.append(" ")
            }

            if (child is TerminalNodeImpl) {
                builder.append(child.text.uppercase())
            } else {
                visit(child)
            }
        }

        return null
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

    override fun visitTypeConstructor(ctx: SparkSqlParser.TypeConstructorContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(" ").append(child.text)
            } else {
                visit(child)
            }
        }

        return null;
    }

    override fun visitIdentifier(ctx: SparkSqlParser.IdentifierContext): Void? {
        val child = ctx.getChild(0)
        if (child is QuotedIdentifierAlternativeContext) {
            builder.append("`").append(ctx.text).append("`")
        } else {
            builder.append(ctx.text)
        }
        return null;
    }

    override fun visitConstantDefault(ctx: SparkSqlParser.ConstantDefaultContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(child.text)
            } else if (child is StringLiteralContext) {
                builder.append(child.text)
            } else if (child is NumericLiteralContext) {
                builder.append(child.text)
            } else {
                visit(child)
            }
        }

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

    private fun append(indent: Int): java.lang.StringBuilder? {
        return builder.append(indentString(indent))
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