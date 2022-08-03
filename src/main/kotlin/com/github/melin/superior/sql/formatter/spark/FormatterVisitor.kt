package com.github.melin.superior.sql.formatter.spark

import com.github.melin.superior.sql.formatter.spark.SparkSqlParser.*
import com.google.common.base.Strings
import com.google.common.collect.Iterables
import org.antlr.v4.runtime.tree.ParseTree
import org.antlr.v4.runtime.tree.TerminalNodeImpl
import org.apache.commons.lang3.StringUtils

class FormatterVisitor(val builder: StringBuilder) : SparkSqlParserBaseVisitor<Void>() {
    companion object {
        private const val INDENT = "  "
    }

    private var indent: Int = 0

    override fun visitSelectClause(ctx: SelectClauseContext): Void? {
        append(indent, "SELECT")

        ctx.hints.forEach { hint -> visit(hint) }

        if (ctx.setQuantifier() != null) {
            visit(ctx.setQuantifier())
        }
        visit(ctx.namedExpressionSeq())

        return null
    }

    override fun visitCtes(ctx: CtesContext): Void? {
        append(indent)
        builder.append("WITH ")

        var first = true
        ctx.namedQuery().forEach { child ->
            if (first) {
                first = false
            } else {
                builder.append(",\n")
                append(indent)
            }
            visit(child)
        }
        builder.append("\n")

        return null
    }

    override fun visitNamedQuery(ctx: NamedQueryContext): Void? {
        builder.append(ctx.name.text)

        if (ctx.columnAliases != null) {
            builder.append("(")
            var first = true
            ctx.columnAliases.identifierSeq().ident.forEach { col ->
                if (first) {
                    first = false
                } else {
                    builder.append(", ")
                }
                visit(col)
            }
            builder.append(")")
        }

        if (ctx.AS() != null) {
            builder.append(" AS ")
        }

        builder.append("(")
        visit(ctx.query())
        builder.append(")")
        return null
    }

    override fun visitHint(ctx: HintContext): Void? {
        builder.append(" /*+ ")

        var first = true
        ctx.hintStatements.forEach { hint ->
            if (first) {
                first = false
            } else {
                builder.append(", ")
            }
            visit(hint)
        }
        builder.append(" */")
        return null
    }

    override fun visitHintStatement(ctx: HintStatementContext): Void? {
        visit(ctx.hintName)

        builder.append("(")
        var first = true
        ctx.parameters.forEach { param ->
            if (first) {
                first = false
            } else {
                builder.append(", ")
            }
            visit(param)
        }
        builder.append(")")

        return null
    }

    override fun visitQuery(ctx: QueryContext): Void? {
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

    override fun visitCreateTable(ctx: CreateTableContext): Void? {
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

    override fun visitCreateTableHeader(ctx: CreateTableHeaderContext): Void? {
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

    override fun visitTableProvider(ctx: TableProviderContext): Void? {
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

    override fun visitFromClause(ctx: FromClauseContext): Void? {
        builder.append('\n')

        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                append(indent, "FROM")
            } else {
                visit(child)
            }
        }

        return null
    }

    override fun visitLateralView(ctx: LateralViewContext): Void? {
        builder.append("\n")
        append(indent, INDENT)

        builder.append("LATERAL VIEW ")
        if(ctx.OUTER() != null) {
            builder.append("OUTER ")
        }

        visit(ctx.qualifiedName())
        builder.append("(\n")
        indent += 2
        ctx.expression().forEach { child ->
            append(indent)
            visit(child)
        }
        indent -= 2

        builder.append("\n")
        append(indent, INDENT)
        builder.append(")")

        if (ctx.tblName != null) {
            builder.append(" ")
            visit(ctx.tblName)
            builder.append(" ")
        }

        if (ctx.AS() != null) {
            builder.append("AS ")
        }

        var first = true
        if (ctx.colName.size > 0) {
            ctx.colName.forEach { child ->
                if (first) {
                    first = false
                } else {
                    builder.append(", ")
                }

                visit(child)
            }
        }

        return null
    }

    override fun visitJoinRelation(ctx: JoinRelationContext): Void? {
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

    override fun visitJoinType(ctx: JoinTypeContext): Void? {
        ctx.children.forEach { child ->
            builder.append(child.text.uppercase()).append(" ")
        }
        return null
    }

    override fun visitJoinCriteria(ctx: JoinCriteriaContext): Void? {
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

    override fun visitComparisonOperator(ctx: ComparisonOperatorContext): Void? {
        builder.append(" ").append(ctx.text).append(" ")
        return null
    }

    override fun visitWhereClause(ctx: WhereClauseContext): Void? {
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
    override fun visitLogicalBinary(ctx: LogicalBinaryContext): Void? {
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

    override fun visitComparison(ctx: ComparisonContext): Void? {
        var first = true
        ctx.children.forEach { child ->
            if (first) {
                first = false
            }
            visit(child)
        }
        return null
    }

    override fun visitLogicalNot(ctx: LogicalNotContext): Void? {
        builder.append("NOT ")
        visit(ctx.getChild(1)) // sub query sq

        return null
    }

    override fun visitExists(ctx: ExistsContext): Void? {
        builder.append("EXISTS (")
        visit(ctx.getChild(2)) // sub query sq
        builder.append(")")

        return null
    }

    override fun visitPredicate(ctx: PredicateContext): Void? {
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

    override fun visitPredicated(ctx: PredicatedContext): Void? {
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

    override fun visitValueExpressionDefault(ctx: ValueExpressionDefaultContext): Void? {
        return super.visitValueExpressionDefault(ctx)
    }

    override fun visitRelation(ctx: RelationContext): Void? {
        var first = true
        ctx.children.forEach { child ->
            if (first) {
                first = false
                builder.append(" ")
            } else if (child is TableNameContext) {
                append(indent, INDENT)
            }

            visit(child)
        }
        return null
    }

    override fun visitTableValuedFunction(ctx: TableValuedFunctionContext): Void? {
        ctx.functionTable().children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(child.text)
            } else {
                visit(child)
            }
        }

        return null
    }

    override fun visitArithmeticBinary(ctx: ArithmeticBinaryContext): Void? {
        visit(ctx.left)
        builder.append(" ").append(ctx.operator.text).append(" ")
        visit(ctx.right)

        return null
    }

    override fun visitInlineTable(ctx: InlineTableContext): Void? {
        builder.append("VALUES").append("\n")
        append(indent, INDENT)

        var first = true
        ctx.expression().forEach { expr ->
            if (first) {
                first = false
            } else {
                builder.append(", ")
                builder.append("\n")
                append(indent, INDENT)
            }
            visit(expr)
        }

        if (ctx.tableAlias() != null) {
            builder.append(" AS ")
            builder.append(ctx.tableAlias().strictIdentifier().text).append("(")
            visit(ctx.tableAlias().identifierList().identifierSeq())
            builder.append(")")
        }

        return null
    }

    override fun visitIdentifierSeq(ctx: IdentifierSeqContext): Void? {
        ctx.children.forEach { expr ->
            if (expr is TerminalNodeImpl) {
                val text = expr.text
                builder.append(text)
                if (",".equals(text)) {
                    builder.append(" ")
                }
            } else {
                visit(expr)
            }
        }

        return null
    }

    override fun visitRowConstructor(ctx: RowConstructorContext): Void? {
        ctx.children.forEach { expr ->
            if (expr is TerminalNodeImpl) {
                val text = expr.text
                builder.append(text)
                if (",".equals(text)) {
                    builder.append(" ")
                }
            } else {
                visit(expr)
            }
        }

        return null
    }

    override fun visitMultipartIdentifier(ctx: MultipartIdentifierContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(child.text)
            } else {
                visit(child)
            }
        }
        return null
    }

    override fun visitAliasedQuery(ctx: AliasedQueryContext): Void? {
        builder.append("(")

        indent += 1
        visit(ctx.getChild(1)) // sub query sql
        indent -= 1

        append(indent)
        builder.append(")")

        visit(ctx.getChild(3)) // alias name

        return null
    }

    override fun visitTableAlias(ctx: TableAliasContext): Void? {
        ctx.children?.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(" ").append(child.text.uppercase())
            } else {
                builder.append(" ").append(child.text)
            }
        }
        return null;
    }

    override fun visitQueryOrganization(ctx: QueryOrganizationContext): Void? {
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
                if (ctx.order.size == 1) {
                    builder.append(" ")
                } else {
                    builder.append("\n")
                    append(indent, INDENT)
                }
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
                if (ctx.sort.size == 1) {
                    builder.append(" ")
                } else {
                    builder.append("\n")
                    append(indent, INDENT)
                }
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
                if (ctx.distributeBy.size == 1) {
                    builder.append(" ")
                } else {
                    builder.append("\n")
                    append(indent, INDENT)
                }

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
                if (ctx.clusterBy.size == 1) {
                    builder.append(" ")
                } else {
                    builder.append("\n")
                    append(indent, INDENT)
                }
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

    override fun visitSortItem(ctx: SortItemContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(" ").append(child.text.uppercase())
            } else {
                visit(child)
            }
        }

        return null;
    }

    override fun visitHavingClause(ctx: HavingClauseContext): Void? {
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

    override fun visitAggregationClause(ctx: AggregationClauseContext): Void? {
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

    override fun visitGroupByClause(ctx: GroupByClauseContext): Void? {
        builder.append(" ")

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

    override fun visitGroupingSet(ctx: GroupingSetContext): Void? {
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

    override fun visitNamedExpressionSeq(ctx: NamedExpressionSeqContext): Void? {
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

        return null;
    }

    override fun visitSearchedCase(ctx: SearchedCaseContext): Void? {
        builder.append("CASE")
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

    override fun visitFunctionCall(ctx: FunctionCallContext): Void? {
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

    override fun visitDereference(ctx: DereferenceContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(child.text)
            } else {
                visit(child)
            }
        }

        return null;
    }

    override fun visitTypeConstructor(ctx: TypeConstructorContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(" ").append(child.text)
            } else {
                visit(child)
            }
        }

        return null;
    }

    override fun visitIdentifier(ctx: IdentifierContext): Void? {
        val child = ctx.getChild(0)
        if (child is QuotedIdentifierAlternativeContext) {
            builder.append("`").append(ctx.text).append("`")
        } else {
            builder.append(ctx.text)
        }
        return null;
    }

    override fun visitConstantDefault(ctx: ConstantDefaultContext): Void? {
        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                builder.append(child.text)
            } else if (child is StringLiteralContext) {
                builder.append(child.text)
            } else if (child is NumericLiteralContext) {
                builder.append(child.text)
            } else if (child is NullLiteralContext) {
                builder.append(child.text)
            } else if (child is BooleanLiteralContext) {
                builder.append(child.text)
            } else {
                visit(child)
            }
        }

        return null;
    }

    override fun visitColumnReference(ctx: ColumnReferenceContext): Void? {
        builder.append(ctx.identifier().text)
        return null;
    }

    override fun visitStar(ctx: StarContext): Void? {
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