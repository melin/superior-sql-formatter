package com.github.melin.superior.sql.formatter.spark

import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.*
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

    override fun visitQuery(ctx: SparkSqlParser.QueryContext): Void? {
        if (!(ctx.parent is StatementDefaultContext || ctx.parent is CreateTableContext)) {
            builder.append("\n")
            indent++
        }

        super.visitQuery(ctx)

        if (!(ctx.parent is StatementDefaultContext || ctx.parent is CreateTableContext)) {
            builder.append("\n")
            indent--
            append(indent)
        }

        return null
    }

    override fun visitCreateTable(ctx: SparkSqlParser.CreateTableContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                val text = child.text.uppercase()
                if ("AS".equals(text)) {
                    builder.append(text).append("\n")
                } else {
                    builder.append(text).append(" ")
                }
            } else {
                visit(child)
            }
        }

        return null
    }

    override fun visitCreateTableHeader(ctx: SparkSqlParser.CreateTableHeaderContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(child.text.uppercase()).append(" ")
            } else {
                visit(child)
                builder.append(" ")
            }
        }

        return null
    }

    override fun visitTableProvider(ctx: SparkSqlParser.TableProviderContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(child.text.uppercase()).append(" ")
            } else {
                visit(child)
                builder.append(" ")
            }
        }

        return null
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
                visit(child)
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

    override fun visitWhereClause(ctx: SparkSqlParser.WhereClauseContext): Void? {
        builder.append("\n")
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                append(indent, "WHERE", "\n")
            } else {
                indent++
                append(indent)
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
                    first = false
                    visit(child)
                } else {
                    val originIndex = indent
                    indent = 0
                    visit(child)
                    indent = originIndex
                }
            }
        }

        return null;
    }

    override fun visitComparison(ctx: SparkSqlParser.ComparisonContext): Void? {
        var first = true
        ctx.children.forEach { child ->
            if (first) {
                first = false
            }
            visit(child)
        }
        return null
    }

    override fun visitLogicalNot(ctx: SparkSqlParser.LogicalNotContext): Void? {
        builder.append("NOT ")
        visit(ctx.getChild(1)) // sub query sq

        return null
    }

    override fun visitExists(ctx: SparkSqlParser.ExistsContext): Void? {
        builder.append("EXISTS (")
        visit(ctx.getChild(2)) // sub query sq
        builder.append(")")

        return null
    }

    override fun visitPredicate(ctx: SparkSqlParser.PredicateContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                val text = child.text.uppercase()
                if (!")".equals(text) && !",".equals(text)) {
                    builder.append(" ")
                }
                builder.append(text)
                if (",".equals(text)) { // in 多个值，逗号后有空格
                    builder.append(" ")
                }
            } else {
                val kind = ctx.kind.text.uppercase()
                if (!"IN".equals(kind)) {
                    builder.append(" ")
                }

                visit(child)
            }
        }

        return null;
    }

    override fun visitPredicated(ctx: SparkSqlParser.PredicatedContext): Void? {
        if (ctx.predicate() != null) {
            visit(ctx.getChild(0))
            visit(ctx.getChild(1))
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

    override fun visitMultipartIdentifier(ctx: SparkSqlParser.MultipartIdentifierContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(child.text)
            } else {
                visit(child)
            }
        }
        return null
    }

    override fun visitAliasedQuery(ctx: SparkSqlParser.AliasedQueryContext): Void? {
        builder.append(INDENT).append("(")

        indent += 1
        visit(ctx.getChild(1)) // sub query sql
        indent -= 1

        append(indent)
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

        var hasLimit = false
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                if ("LIMIT".equals(child.text.uppercase())) {
                    hasLimit = true
                }
            }
        }

        if (ctx.order.size > 0) {
            builder.append("\n")
            append(indent)
            builder.append("ORDER BY")
            ctx.order.forEachIndexed { index, sortItemContext ->
                visit(sortItemContext)
                if (index != (ctx.order.size - 1)) {
                    builder.append(",")
                }
            }
        }

        if (ctx.sort.size > 0) {
            builder.append("\n")
            append(indent)
            builder.append("SORT BY")
            ctx.sort.forEachIndexed { index, sortItemContext ->
                visit(sortItemContext)
                if (index != (ctx.sort.size - 1)) {
                    builder.append(",")
                }
            }
        }

        if (ctx.distributeBy.size > 0) {
            builder.append("\n")
            append(indent)
            builder.append("DISTRIBUTE BY")
            ctx.distributeBy.forEachIndexed { index, expressionContext ->
                builder.append("\n")
                append(indent, INDENT)
                visit(expressionContext)
                if (index != (ctx.distributeBy.size - 1)) {
                    builder.append(",")
                }
            }
        }

        if (ctx.clusterBy.size > 0) {
            builder.append("\n")
            append(indent)
            builder.append("CLUSTER BY")
            ctx.clusterBy.forEachIndexed { index, expressionContext ->
                builder.append("\n")
                append(indent, INDENT)
                visit(expressionContext)
                if (index != (ctx.clusterBy.size - 1)) {
                    builder.append(",")
                }
            }
        }

        if (ctx.limit == null && hasLimit) {
            builder.append("\n")
            append(indent)
            builder.append("LIMIT ALL")
        } else if (ctx.limit != null) {
            builder.append("\n")
            append(indent)
            builder.append("LIMIT ")
            visit(ctx.limit.getChild(0))
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

    override fun visitHavingClause(ctx: SparkSqlParser.HavingClauseContext): Void? {
        builder.append("\n")
        append(indent)
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(child.text.uppercase())
                builder.append("\n")
            } else {
                append(indent, INDENT)
                visit(child)
            }
        }

        return null
    }

    override fun visitAggregationClause(ctx: SparkSqlParser.AggregationClauseContext): Void? {
        builder.append("\n")
        append(indent)

        var first = true
        ctx.children?.forEach { child ->
            if (child is TerminalNodeImpl) {
                val text = child.text.uppercase()
                if (!",".equals(text)) {
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

    override fun visitSearchedCase(ctx: SparkSqlParser.SearchedCaseContext): Void? {
        builder.append("CASE")
        indent++
        super.visitSearchedCase(ctx)
        indent--

        builder.append("\n")
        append(indent, INDENT)
        builder.append("ELSE ")
        visit(ctx.elseExpression)
        builder.append(" END")

        return null
    }

    override fun visitSimpleCase(ctx: SimpleCaseContext): Void? {
        builder.append("CASE ")
        visit(ctx.value)

        indent++
        ctx.whenClause().forEach { visit(it) }
        indent--

        builder.append("\n")
        append(indent, INDENT)
        builder.append("ELSE ")
        visit(ctx.elseExpression)
        builder.append(" END")

        return null
    }

    override fun visitWhenClause(ctx: WhenClauseContext): Void? {
        builder.append("\n")
        append(indent, INDENT)
        builder.append("WHEN ")
        visit(ctx.condition)
        builder.append(" ")
        visit(ctx.result)

        return null
    }

    override fun visitFunctionCall(ctx: SparkSqlParser.FunctionCallContext): Void? {
        run outside@{
            ctx.children.forEach { child ->
                if (child is TerminalNodeImpl) {
                    val text = child.text;
                    if (",".equals(text)) {
                        builder.append(text).append(" ")
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
            indent +=2
            builder.append("\n")
            append(indent)
            visit(ctx.where)
            indent -= 3
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

    private fun append(indent: Int): StringBuilder {
        return builder.append(indentString(indent))
    }

    private fun append(indent: Int, value: String): StringBuilder {
        return builder.append(indentString(indent))
            .append(value)
    }

    private fun append(indent: Int, value1: String, value2: String): StringBuilder {
        return builder.append(indentString(indent))
            .append(value1).append(value2)
    }

    private fun indentString(indent: Int): String {
        return Strings.repeat(INDENT, indent)
    }
}