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

    override fun visitPivotClause(ctx: PivotClauseContext): Void? {
        builder.append("\n")
        append(indent, INDENT)

        builder.append("PIVOT (")

        indent++
        visit(ctx.aggregates)
        builder.append("\n")
        append(indent, INDENT)
        indent--

        builder.append("FOR ")

        ctx.pivotColumn().children.forEach {child ->
            if (child is TerminalNodeImpl) {
                val text = child.text
                builder.append(text)
                if (",".equals(text)) {
                    builder.append(" ")
                }
            } else {
                visit(child)
            }
        }

        builder.append(" IN ")
        builder.append("(")
        var first = true
        ctx.pivotValues.forEach{child ->
            if (first) {
                first = false
            } else {
                builder.append(", ")
            }
            visit(child)
        }
        builder.append(")")

        builder.append("\n")
        append(indent, INDENT)
        builder.append(")")
        return null
    }

    override fun visitPivotValue(ctx: PivotValueContext): Void? {
        visit(ctx.expression())

        if (ctx.AS() != null) {
            builder.append(" AS")
        }

        if (ctx.identifier() != null) {
            builder.append(" ")
            visit(ctx.identifier())
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

    override fun visitArithmeticUnary(ctx: ArithmeticUnaryContext): Void? {
        builder.append(ctx.operator.text)
        visit(ctx.valueExpression())
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

    override fun visitStruct(ctx: StructContext): Void? {
        builder.append("STRUCT(")
        var first = true
        ctx.argument .forEach { child ->
            if (first) {
                first = false
            } else {
                builder.append(", ")
            }

            visit(child)
        }
        builder.append(")")

        return null
    }

    override fun visitCurrentLike(ctx: CurrentLikeContext): Void? {
        builder.append(ctx.name.text.lowercase())
        return null
    }

    override fun visitTimestampadd(ctx: TimestampaddContext): Void? {
        builder.append(ctx.name.text.lowercase())
        builder.append("(")
        builder.append(ctx.unit.text.uppercase()).append(", ")
        visit(ctx.unitsAmount)
        builder.append(", ")
        visit(ctx.timestamp)
        builder.append(")")
        return null
    }

    override fun visitTimestampdiff(ctx: TimestampdiffContext): Void? {
        builder.append(ctx.name.text.lowercase())
        builder.append("(")
        builder.append(ctx.unit.text.uppercase()).append(", ")
        visit(ctx.startTimestamp)
        builder.append(", ")
        visit(ctx.endTimestamp)
        builder.append(")")

        return null
    }

    override fun visitCast(ctx: CastContext): Void? {
        builder.append(ctx.name.text.lowercase()).append("(")
        visit(ctx.expression())
        builder.append(" AS ")
        visit(ctx.dataType())
        builder.append(")")
        return null
    }

    override fun visitFirst(ctx: FirstContext): Void? {
        builder.append("first(")
        visit(ctx.expression())
        if (ctx.IGNORE() != null) {
            builder.append(" IGNORE NULLS")
        }
        builder.append(")")
        return null
    }

    override fun visitLast(ctx: LastContext): Void? {
        builder.append("last(")
        visit(ctx.expression())
        if (ctx.IGNORE() != null) {
            builder.append(" IGNORE NULLS")
        }
        builder.append(")")
        return null
    }

    override fun visitPosition(ctx: PositionContext): Void? {
        builder.append("position(")
        visit(ctx.substr)
        builder.append(" in ")
        visit(ctx.str)
        builder.append(")")
        return null
    }

    override fun visitLambda(ctx: LambdaContext): Void? {
        //identifier ARROW expression                                                              #lambda
        //    | LEFT_PAREN identifier (COMMA identifier)+ RIGHT_PAREN ARROW expression

        if (ctx.LEFT_PAREN() != null) {
            builder.append("(")
        }

        var first = true
        ctx.identifier().forEach { child ->
            if (first) {
                first = false
            } else {
                builder.append(", ")
            }

            visit(child)
        }

        if (ctx.RIGHT_PAREN() != null) {
            builder.append(")")
        }

        builder.append(" -> ")
        visit(ctx.expression())
        return null
    }

    override fun visitArithmeticBinary(ctx: ArithmeticBinaryContext): Void? {
        visit(ctx.left)
        builder.append(" ").append(ctx.operator.text).append(" ")
        visit(ctx.right)

        return null
    }

    override fun visitComplexDataType(ctx: ComplexDataTypeContext): Void? {
        builder.append(ctx.complex.text).append("<")
        if (ctx.dataType().size == 1) {
            visit(ctx.dataType(0))
        } else if (ctx.dataType().size == 2) {
            visit(ctx.dataType(0))
            builder.append(", ")
            visit(ctx.dataType(1))
        } else {
            if (ctx.NEQ() != null) {
                builder.append("<>")
            } else if (ctx.complexColTypeList() != null) {
                visit(ctx.complexColTypeList())
            }
        }
        builder.append(">")
        return null
    }

    override fun visitYearMonthIntervalDataType(ctx: YearMonthIntervalDataTypeContext): Void? {
        builder.append("INTERVAL ").append(ctx.from.text)
        if (ctx.TO() != null) {
            builder.append(" TO ").append(ctx.to.text)
        }

        return null
    }

    override fun visitDayTimeIntervalDataType(ctx: DayTimeIntervalDataTypeContext): Void? {
        builder.append("INTERVAL ").append(ctx.from.text)
        if (ctx.TO() != null) {
            builder.append(" TO ").append(ctx.to.text)
        }
        return null
    }

    override fun visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): Void? {
        visit(ctx.identifier())
        if (ctx.LEFT_PAREN() != null) {
            builder.append("(")
            var first = true
            ctx.INTEGER_VALUE().forEach { child ->
                if (first) {
                    first = false
                } else {
                    builder.append(", ")
                }
                visit(child)
            }
            builder.append(")")
        }

        return null
    }

    override fun visitComplexColTypeList(ctx: ComplexColTypeListContext): Void? {
        var first = true
        ctx.complexColType().forEach { child ->
            if (first) {
                first = false
            } else {
                builder.append(", ")
            }
            visit(child)
        }

        return null
    }

    override fun visitComplexColType(ctx: ComplexColTypeContext): Void? {
        // : identifier COLON? dataType (NOT NULL)? commentSpec?
        visit(ctx.identifier())
        if (ctx.COLON() != null) {
            builder.append(":")
        }
        visit(ctx.dataType())

        if (ctx.NOT() != null) {
            builder.append(" NOT NULL")
        }

        visit(ctx.commentSpec())

        return null
    }

    override fun visitCommentSpec(ctx: CommentSpecContext): Void? {
        builder.append("COMMENT")
        visit(ctx.STRING())
        return null
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

    override fun visitParenthesizedExpression(ctx: ParenthesizedExpressionContext): Void? {
        builder.append("(")
        visit(ctx.expression())
        builder.append(")")
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

    override fun visitSample(ctx: SampleContext): Void? {
        builder.append(" TABLESAMPLE (")

        if (ctx.sampleMethod() != null) {
            visit(ctx.sampleMethod())
        }

        builder.append(")")
        if (ctx.REPEATABLE() != null) {
            builder.append(" REPEATABLE(")
            builder.append(ctx.seed.text)
            builder.append(")")
        }

        return null
    }

    override fun visitSampleByBucket(ctx: SampleByBucketContext): Void? {
        builder.append("BUCKET ").append(ctx.numerator.text)
            .append(" OUT OF ").append(ctx.denominator.text)

        if (ctx.ON() != null) {
            builder.append(" ")
            if (ctx.LEFT_PAREN() != null) {
                visit(ctx.qualifiedName())
                builder.append("()")
            } else {
                visit(ctx.identifier())
            }
        }
        return null
    }

    override fun visitSampleByPercentile(ctx: SampleByPercentileContext): Void? {
        if (ctx.negativeSign != null) {
            builder.append(" -")
        }
        builder.append(ctx.percentage.text).append(" PERCENT")
        return null
    }

    override fun visitSampleByRows(ctx: SampleByRowsContext): Void? {
        visit(ctx.expression())
        builder.append(" ROWS")
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
                    builder.append("\n")
                    append(indent, INDENT)
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
                    } else if (StringUtils.endsWithIgnoreCase("filter", text) ||
                        StringUtils.endsWithIgnoreCase("over", text) ||
                        StringUtils.endsWithIgnoreCase("IGNORE", text) ||
                        StringUtils.endsWithIgnoreCase("RESPECT", text) ||
                        StringUtils.endsWithIgnoreCase("NULLS", text)) {
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

        if (ctx.nullsOption != null) {
            builder.append(" ").append(ctx.nullsOption.text).append(" NULLS")
        }

        if (ctx.OVER() != null) {
            builder.append(" OVER ")
            visit(ctx.windowSpec())
        }

        return null
    }

    override fun visitWindowClause(ctx: WindowClauseContext): Void? {
        builder.append("\n")
        append(indent)
        builder.append("WINDOW ")

        var first = true
        ctx.namedWindow().forEach { child ->
            if (first) {
                first = false
            } else {
                builder.append("\n")
                append(indent, INDENT)
            }

            visit(child.name)
            builder.append(" AS ")
            visit(child.windowSpec())
        }

        return null
    }

    override fun visitWindowRef(ctx: WindowRefContext): Void? {
        if (ctx.LEFT_PAREN() == null) {
            visit(ctx.name)
        } else {
            builder.append("(")
            visit(ctx.name)
            builder.append(")")
        }

        return null
    }

    override fun visitWindowDef(ctx: WindowDefContext): Void? {
        builder.append("(")

        if (ctx.CLUSTER() != null) {
            builder.append(" CLUSTER BY")
        } else {
            if (ctx.PARTITION() != null) {
                builder.append("PARTITION BY ")
            }
            if (ctx.DISTRIBUTE() != null) {
                builder.append("DISTRIBUTE BY ")
            }
            if (ctx.PARTITION() != null || ctx.DISTRIBUTE() != null) {
                var first = true
                ctx.partition.forEach { part ->
                    if (first) {
                        first = false
                    } else {
                        builder.append(", ")
                    }
                    visit(part)
                }

                if (ctx.ORDER() != null || ctx.SORT() != null) {
                    builder.append(" ")
                }
            }

            if (ctx.ORDER() != null) {
                builder.append("ORDER BY ")
            }
            if (ctx.SORT() != null) {
                builder.append("SORT BY ")
            }

            if (ctx.ORDER() != null || ctx.SORT() != null) {
                var first = true
                ctx.sortItem().forEach { item ->
                    if (first) {
                        first = false
                    } else {
                        builder.append(", ")
                    }
                    visit(item)
                }
            }
        }

        if (ctx.windowFrame() != null) {
            val frame = ctx.windowFrame()
            builder.append("\n")
            append(indent, INDENT, INDENT)
            builder.append(frame.frameType.text)

            if (frame.BETWEEN() != null) {
                builder.append(" BETWEEN")
            }
            visit(frame.start)
            if (frame.AND() != null) {
                builder.append(" AND")
                visit(frame.end)
            }

            builder.append("\n")
            append(indent, INDENT, ")")
        } else {
            builder.append(")")
        }

        return null
    }

    override fun visitFrameBound(ctx: FrameBoundContext): Void? {
        builder.append(" ")
        if (ctx.UNBOUNDED() != null) {
            builder.append("UNBOUNDED ").append(ctx.boundType.text)
        } else if (ctx.boundType != null) {
            builder.append("CURRENT ROW")
        } else {
            visit(ctx.expression())
            builder.append(" ").append(ctx.boundType.text)
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