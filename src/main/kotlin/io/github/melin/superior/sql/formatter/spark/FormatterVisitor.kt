package io.github.melin.superior.sql.formatter.spark

import io.github.melin.superior.sql.formatter.spark.SparkSqlParser.*
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
        if (ctx.parent is FromStatementBodyContext) {
            builder.append("\n")
        }
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
            joinChild(ctx.columnAliases.identifierSeq().ident)
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
        joinChild(ctx.hintStatements, " /*+ ", " */")
        return null
    }

    override fun visitHintStatement(ctx: HintStatementContext): Void? {
        visit(ctx.hintName)
        joinChild(ctx.parameters)

        return null
    }

    override fun visitQuery(ctx: QueryContext): Void? {
        if (!(ctx.parent is StatementDefaultContext
                    || ctx.parent is CreateTableContext
                    || ctx.parent is SingleInsertQueryContext
                    || ctx.parent is DescribeQueryContext
                    || ctx.parent is AlterViewQueryContext)) {
            builder.append("\n")
            indent++
        }

        super.visitQuery(ctx)

        if (!(ctx.parent is StatementDefaultContext
                    || ctx.parent is CreateTableContext
                    || ctx.parent is SingleInsertQueryContext
                    || ctx.parent is DescribeQueryContext
                    || ctx.parent is AlterViewQueryContext)) {
            builder.append("\n")
            indent--
            append(indent)
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
        if (!(ctx.parent is MultiInsertQueryContext)) {
            builder.append('\n')
        }

        ctx.children.forEach { child ->
            if (child is TerminalNodeImpl) {
                append(indent, "FROM")
            } else {
                visit(child)
            }
        }

        if (ctx.parent is MultiInsertQueryContext) {
            builder.append('\n')
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

        joinChild(ctx.colName, "", "")
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
        joinChild(ctx.pivotValues)

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
        iteratorChild(ctx.children, true, " ", "")
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
        iteratorChild(ctx.children, true, " ", "")
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
        joinChild(ctx.children, "", "", "")
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
            iteratorChild(ctx.children, false, "", "")
        }
        return null;
    }

    override fun visitStruct(ctx: StructContext): Void? {
        joinChild(ctx.argument, "STRUCT(", ")")
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
        if (ctx.LEFT_PAREN() != null) {
            builder.append("(")
        }

        joinChild(ctx.identifier(), "", "")

        if (ctx.RIGHT_PAREN() != null) {
            builder.append(")")
        }

        builder.append(" -> ")
        visit(ctx.expression())
        return null
    }

    override fun visitExtract(ctx: ExtractContext): Void? {
        builder.append("extract(")
            .append(ctx.field.text.uppercase())
            .append(" FROM ")
        visit(ctx.source)
        builder.append(")")
        return null
    }

    override fun visitSubstring(ctx: SubstringContext): Void? {
        if (ctx.SUBSTRING() != null) {
            builder.append("substring")
        } else if (ctx.SUBSTR() != null) {
            builder.append("substr")
        }
        builder.append("(")
        visit(ctx.str)

        if (ctx.FROM() != null) {
            builder.append(" FROM ")
            visit(ctx.pos)
        } else if (ctx.COMMA().size > 0) {
            builder.append(", ")
            visit(ctx.pos)
        }

        if (ctx.FOR() != null) {
            builder.append(" FOR ")
            visit(ctx.len)
        } else if (ctx.COMMA().size > 1) {
            builder.append(", ")
            visit(ctx.len)
        }

        builder.append(")")
        return null
    }

    override fun visitTrim(ctx: TrimContext): Void? {
        builder.append("trim(")
        if (ctx.trimOption != null) {
            builder.append(ctx.trimOption.text.uppercase()).append(" ")
        }

        if (ctx.trimStr != null) {
            visit(ctx.trimStr)
            builder.append(" ")
        }

        builder.append("FROM ")
        visit(ctx.srcStr)
        builder.append(")")
        return null
    }

    override fun visitOverlay(ctx: OverlayContext): Void? {
        builder.append("overlay(")
        visit(ctx.input)
        builder.append(" PLACING ")
        visit(ctx.replace)
        builder.append(" FROM ")
        visit(ctx.position)
        if (ctx.FOR() != null) {
            builder.append(" FOR ")
            visit(ctx.length)
        }
        builder.append(")")
        return null
    }

    override fun visitPercentile(ctx: PercentileContext): Void? {
        builder.append(ctx.name.text).append("(")
        visit(ctx.percentage)
        builder.append(") WITHIN (GROUP ORDER BY ")
        visit(ctx.sortItem())
        builder.append(")")

        if (ctx.OVER() != null) {
            builder.append(" OVER ")
            visit(ctx.windowSpec())
        }

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
            joinChild(ctx.INTEGER_VALUE())
        }

        return null
    }

    override fun visitComplexColTypeList(ctx: ComplexColTypeListContext): Void? {
        joinChild(ctx.complexColType(), "", "")
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
        builder.append("COMMENT ").append(ctx.STRING().text)
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
        iteratorChild(ctx.functionTable().children, true, "", "")
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
                builder.append(",")
                builder.append("\n")
                append(indent, INDENT)
            }
            visit(expr)
        }

        if (ctx.tableAlias().children != null) {
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
        iteratorChild(ctx.children, true, "", "")
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
                joinChild(ctx.partition, "", "")

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
                joinChild(ctx.sortItem(), "", "")
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
        iteratorChild(ctx.children, true, "", "")
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
            } else if (child is IntervalLiteralContext) {
                visit(child.interval())
            } else {
                visit(child)
            }
        }

        return null;
    }

    override fun visitInterval(ctx: IntervalContext): Void? {
        builder.append("INTERVAL ")
        if (ctx.errorCapturingMultiUnitsInterval() != null) {
            visit(ctx.errorCapturingMultiUnitsInterval())
        }
        if (ctx.errorCapturingUnitToUnitInterval() != null) {
            visit(ctx.errorCapturingUnitToUnitInterval())
        }
        return null;
    }

    override fun visitMultiUnitsInterval(ctx: MultiUnitsIntervalContext): Void? {
        var first = true
        ctx.children.forEach { child ->
            if (first) {
                first = false
            } else {
                builder.append(" ")
            }
            builder.append(child.text)
        }
        return null
    }

    override fun visitUnitToUnitInterval(ctx: UnitToUnitIntervalContext): Void? {
        visit(ctx.intervalValue())
        builder.append(" ").append(ctx.from.text.uppercase())
        builder.append(" TO ").append(ctx.to.text.uppercase())
        return null
    }

    override fun visitIntervalValue(ctx: IntervalValueContext): Void? {
        if (ctx.PLUS() != null) {
            builder.append("+")
        }
        if (ctx.MINUS() != null) {
            builder.append("-")
        }

        if (ctx.INTEGER_VALUE() != null) {
            builder.append(ctx.INTEGER_VALUE().text)
        }
        if (ctx.DECIMAL_VALUE() != null) {
            builder.append(ctx.DECIMAL_VALUE().text)
        }
        if (ctx.STRING() != null) {
            builder.append(ctx.STRING().text)
        }

        return null
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

    //-----------------custom sql-------------------------

    override fun visitCall(ctx: CallContext): Void? {
        builder.append("CALL ")
        visit(ctx.multipartIdentifier())

        joinChild(ctx.callArgument(), "(\n" + INDENT, "\n)", ",\n" + INDENT)
        return null
    }

    override fun visitDtunnelExpr(ctx: DtunnelExprContext): Void? {
        builder.append("DATATUNNEL SOURCE(").append(ctx.srcName.text).append(") OPTIONS")
        joinChild(ctx.readOpts.dtProperty(), "(\n" + INDENT, "\n)", ",\n" + INDENT)
        builder.append("\n")
        if (ctx.TRANSFORM() != null) {
            builder.append("TRANSFORM = ").append(ctx.transfromSql.text).append("\n");
        }
        builder.append("SINK(").append(ctx.srcName.text).append(") OPTIONS")
        joinChild(ctx.writeOpts.dtProperty(), "(\n" + INDENT, "\n)", ",\n" + INDENT)
        return null
    }

    override fun visitDtProperty(ctx: DtPropertyContext): Void? {
        visit(ctx.key)
        builder.append(" = ")
        visit(ctx.value)
        return null
    }

    override fun visitDtPropertyKey(ctx: DtPropertyKeyContext): Void? {
        iteratorChild(ctx.children, false, " ", "")
        return null
    }

    override fun visitDtPropertyValue(ctx: DtPropertyValueContext): Void? {
        if (ctx.LEFT_BRACKET() != null) {
            joinChild(ctx.dtPropertyValue(), "[", "]")
        } else {
            builder.append(ctx.text)
        }
        return null
    }

    override fun visitNamedArgument(ctx: NamedArgumentContext): Void? {
        visit(ctx.identifier())
        builder.append(" => ")
        visit(ctx.expression())
        return null
    }

    override fun visitExportTable(ctx: ExportTableContext): Void? {
        if (ctx.ctes() != null) {
            visit(ctx.ctes())
        }
        builder.append("EXPORT TABLE ")
        visit(ctx.multipartIdentifier())
        if (ctx.partitionSpec() != null) {
            builder.append(" ")
            visit(ctx.partitionSpec())
            builder.append("\nTO ").append(ctx.name.text)
        } else {
            builder.append(" TO ").append(ctx.name.text)
        }

        if (ctx.OPTIONS() != null) {
            builder.append(" OPTIONS")
            visit(ctx.propertyList())
        }
        return null
    }

    override fun visitPropertyList(ctx: PropertyListContext): Void? {
        joinChild(ctx.property(), "(\n" + INDENT, "\n)", ",\n" + INDENT)
        return null
    }

    override fun visitLoadTempTable(ctx: LoadTempTableContext): Void? {
        builder.append("LOAD DATA ").append(ctx.path.text).append(" ")
        visit(ctx.multipartIdentifier())
        if (ctx.OPTIONS() != null) {
            builder.append(" OPTIONS")
            visit(ctx.propertyList())
        }
        return null
    }

    override fun visitProperty(ctx: PropertyContext): Void? {
        visit(ctx.key)
        builder.append(" = ").append(ctx.value.text)
        return null
    }

    override fun visitPropertyKey(ctx: PropertyKeyContext): Void? {
        if (ctx.STRING() != null) {
            builder.append(ctx.STRING().text)
        } else {
            joinChild(ctx.identifier(), "", "", ".")
        }
        return null
    }

    //---------------------insert sql----------------------
    override fun visitSingleInsertQuery(ctx: SingleInsertQueryContext): Void? {
        visit(ctx.insertInto())
        val raw = ctx.query().text
        if (!StringUtils.startsWithIgnoreCase(raw, "table")
            &&!StringUtils.startsWithIgnoreCase(raw, "from")) {
            builder.setLength(builder.length - 1); // 去掉最后空格
            builder.append("\n");
        }
        visit(ctx.query())
        return null
    }

    override fun visitInsertIntoTable(ctx: InsertIntoTableContext): Void? {
        iteratorChild(ctx.children)
        return null
    }

    override fun visitInsertOverwriteTable(ctx: InsertOverwriteTableContext): Void? {
        iteratorChild(ctx.children)
        return null
    }

    override fun visitPartitionSpec(ctx: PartitionSpecContext): Void? {
        builder.append("PARTITION")
        joinChild(ctx.partitionVal())
        return null
    }

    override fun visitPartitionVal(ctx: PartitionValContext): Void? {
        visit(ctx.identifier())
        if (ctx.EQ() != null) {
            builder.append(" = ")
            builder.append(ctx.constant().text)
        }
        return null
    }

    override fun visitTable(ctx: TableContext): Void? {
        builder.append("TABLE ")
        visit(ctx.multipartIdentifier())
        return null
    }

    override fun visitIdentifierList(ctx: IdentifierListContext): Void? {
        builder.append("(")
        visit(ctx.identifierSeq())
        builder.append(")")
        return null
    }

    override fun visitDeleteFromTable(ctx: DeleteFromTableContext): Void? {
        builder.append("DELETE FROM ")
        visit(ctx.multipartIdentifier())
        if (ctx.tableAlias().children != null) {
            visit(ctx.tableAlias())
        }
        if (ctx.whereClause() != null) {
            visit(ctx.whereClause())
        }
        return null
    }

    override fun visitUpdateTable(ctx: UpdateTableContext): Void? {
        builder.append("UPDATE ")
        visit(ctx.multipartIdentifier())
        builder.append(" ")
        if (ctx.tableAlias().children != null) {
            visit(ctx.tableAlias())
            builder.append(" ")
        }
        builder.append("SET\n")
        visit(ctx.setClause())
        if (ctx.whereClause() != null) {
            visit(ctx.whereClause())
        }

        return null
    }

    override fun visitAssignmentList(ctx: AssignmentListContext): Void? {
        var first = true
        ctx.assignment().forEach {child ->
            if (first) {
                append(indent, INDENT)
                first = false
            } else {
                builder.append(",\n")
                append(indent, INDENT)
            }
            visit(child)
        }
        return null
    }

    override fun visitAssignment(ctx: AssignmentContext): Void? {
        visit(ctx.key)
        builder.append(" = ")
        visit(ctx.value)
        return null
    }

    override fun visitMergeIntoTable(ctx: MergeIntoTableContext): Void? {
        builder.append("MERGE INTO ")
        visit(ctx.target)
        if (ctx.targetAlias.children != null) {
            visit(ctx.targetAlias)
            builder.append(" ")
        }

        builder.append("\nUSING ")
        if (ctx.source != null) {
            visit(ctx.source)
        } else {
            builder.append("(")
            visit(ctx.sourceQuery)
            builder.append(")")
        }

        if (ctx.sourceAlias.children != null) {
            visit(ctx.sourceAlias)
        }

        builder.append("\nON ")
        visit(ctx.mergeCondition)
        joinChild(ctx.matchedClause(), "", "", "")
        joinChild(ctx.notMatchedClause(), "", "", "")
        return null
    }

    override fun visitMatchedClause(ctx: MatchedClauseContext): Void? {
        builder.append("\nWHEN MATCHED ")
        if (ctx.matchedCond != null) {
            builder.append("AND ")
            visit(ctx.matchedCond)
            builder.append(" ")
        }
        builder.append("THEN\n")
        indent++
        visit(ctx.matchedAction())
        indent--
        return null
    }

    override fun visitNotMatchedClause(ctx: NotMatchedClauseContext): Void? {
        builder.append("\nWHEN NOT MATCHED ")
        if (ctx.notMatchedCond != null) {
            builder.append("AND ")
            visit(ctx.notMatchedCond)
            builder.append(" ")
        }
        builder.append("THEN\n")
        indent++
        visit(ctx.notMatchedAction())
        indent--
        return null
    }

    override fun visitMatchedAction(ctx: MatchedActionContext): Void? {
        if (ctx.DELETE() != null) {
            append(indent, "DELETE")
        } else if (ctx.ASTERISK() != null) {
            append(indent, "UPDATE SET *")
        } else {
            append(indent, "UPDATE SET\n")
            visit(ctx.assignmentList())
        }
        return null
    }

    override fun visitNotMatchedAction(ctx: NotMatchedActionContext): Void? {
        if (ctx.ASTERISK() != null) {
            append(indent, "INSERT *")
        } else {
            append(indent, "INSERT (")
            visit(ctx.multipartIdentifierList())
            builder.append(") VALUES ")
            joinChild(ctx.expression())
        }
        return null
    }

    //---------------------DDL Syntax----------------------

    override fun visitCreateTable(ctx: CreateTableContext): Void? {
        visit(ctx.createTableHeader())
        if (ctx.LEFT_PAREN() != null) {
            builder.append("(\n")
            visit(ctx.colTypeList())
        }

        if (ctx.RIGHT_PAREN() != null) {
            builder.append("\n)")
            if (ctx.tableProvider() != null) {
                builder.append("\n")
                visit(ctx.tableProvider())
            }
        } else {
            if (ctx.tableProvider() != null) {
                visit(ctx.tableProvider())
            }
        }

        visit(ctx.createTableClauses())

        if (ctx.AS() != null) {
            builder.append(" AS\n")
        }
        if (ctx.query() != null) {
            visit(ctx.query())
        }

        return null
    }

    override fun visitCreateTableHeader(ctx: CreateTableHeaderContext): Void? {
        iteratorChild(ctx.children)
        return null
    }

    override fun visitTableProvider(ctx: TableProviderContext): Void? {
        builder.append("USING ")
        visit(ctx.multipartIdentifier())
        return null
    }

    override fun visitColTypeList(ctx: ColTypeListContext): Void? {
        joinChild(ctx.colType(), INDENT, "", ",\n" + INDENT)
        return null
    }

    override fun visitColType(ctx: ColTypeContext): Void? {
        joinChild(ctx.children, "", "", " ")
        return null
    }

    override fun visitCreateTableClauses(ctx: CreateTableClausesContext): Void? {
        if (ctx.primaryKeyExpr().size > 0) {
            visit(ctx.primaryKeyExpr().get(0))
        }
        if (ctx.PARTITIONED().size > 0) {
            builder.append("\nPARTITIONED BY ")
            joinChild(ctx.partitionFieldList())
        }
        if (ctx.locationSpec().size > 0) {
            builder.append("\nLOCATION ").append(ctx.locationSpec().get(0).STRING().text)
        }
        if (ctx.LIFECYCLE().size > 0) {
            builder.append("\nLIFECYCLE ").append(ctx.lifecycle.text)
        }
        if (ctx.OPTIONS().size > 0) {
            builder.append("\nOPTIONS")
            joinChild(ctx.options.property(), "(\n" + INDENT, "\n)", ",\n" + INDENT)
        }
        if (ctx.TBLPROPERTIES().size > 0) {
            builder.append("\nTBLPROPERTIES ")
            joinChild(ctx.tableProps.property(), "(\n" + INDENT, "\n)", ",\n" + INDENT)
        }
        if (ctx.commentSpec().size > 0) {
            builder.append("\n")
            visit(ctx.commentSpec().get(0))
        }

        return null
    }

    override fun visitPrimaryKeyExpr(ctx: PrimaryKeyExprContext): Void? {
        builder.append("\nPRIMARY KEY ")
        joinChild(ctx.primaryColumnNames().errorCapturingIdentifier())
        if (ctx.WITH() != null) {
            builder.append(" WITH ").append(ctx.hudiType.text)
        }
        return null
    }

    override fun visitUse(ctx: UseContext): Void? {
        builder.append("USE ")
        visit(ctx.multipartIdentifier())
        return null
    }

    override fun visitUseNamespace(ctx: UseNamespaceContext): Void? {
        builder.append("USE ").append(ctx.namespace().text.uppercase()).append(" ")
        visit(ctx.multipartIdentifier())
        return null
    }

    override fun visitSetCatalog(ctx: SetCatalogContext): Void? {
        builder.append("SET CATALOG ")
        if (ctx.STRING() != null) {
            builder.append(ctx.STRING().text)
        } else {
            visit(ctx.identifier())
        }
        return null
    }

    override fun visitSetConfiguration(ctx: SetConfigurationContext): Void? {
        ctx.children.forEach { child ->
            val text = child.text
            if ("SET".equals(text.uppercase())) {
                builder.append("SET ")
            } else if ("=".equals(text)) {
                builder.append(" = ")
            } else {
                builder.append(text)
            }
        }
        return null
    }

    override fun visitSetQuotedConfiguration(ctx: SetQuotedConfigurationContext): Void? {
        builder.append(ctx.configKey().text).append(" = ").append(ctx.configValue().text)
        return null
    }

    override fun visitCreateNamespace(ctx: CreateNamespaceContext): Void? {
        builder.append("CREATE ").append(ctx.namespace().text.uppercase()).append(" ")
        if (ctx.IF() != null) {
            builder.append("IF NOT EXISTS ")
        }
        visit(ctx.multipartIdentifier())
        if (ctx.commentSpec().size > 0) {
            builder.append("\n")
            visit(ctx.commentSpec().get(0))
        }
        if (ctx.locationSpec().size > 0) {
            builder.append("\nLOCATION ").append(ctx.locationSpec().get(0).STRING().text)
        }
        if (ctx.WITH().size > 0) {
            builder.append("\nWITH ")
            if (ctx.DBPROPERTIES().size > 0) {
                builder.append("DBPROPERTIES ")
            } else if (ctx.PROPERTIES().size > 0) {
                builder.append("PROPERTIES ")
            }
            visit(ctx.propertyList().get(0))
        }
        return null
    }

    override fun visitSetNamespaceProperties(ctx: SetNamespacePropertiesContext): Void? {
        builder.append("ALTER ").append(ctx.namespace().text.uppercase()).append(" ")
        visit(ctx.multipartIdentifier())
        builder.append(" SET ")
        if (ctx.DBPROPERTIES() != null) {
            builder.append("DBPROPERTIES ")
        } else {
            builder.append("PROPERTIES ")
        }
        visit(ctx.propertyList())
        return null
    }

    override fun visitSetNamespaceLocation(ctx: SetNamespaceLocationContext): Void? {
        builder.append("ALTER ").append(ctx.namespace().text.uppercase()).append(" ")
        visit(ctx.multipartIdentifier())
        builder.append(" SET ")
        builder.append("LOCATION ").append(ctx.locationSpec().STRING().text)
        return null
    }

    override fun visitDropNamespace(ctx: DropNamespaceContext): Void? {
        builder.append("DROP ").append(ctx.namespace().text.uppercase()).append(" ")
        if (ctx.IF() != null) {
            builder.append("IF EXISTS ")
        }
        visit(ctx.multipartIdentifier())

        if (ctx.RESTRICT() != null) {
            builder.append(" RESTRICT")
        } else if (ctx.CASCADE() != null) {
            builder.append(" CASCADE")
        }
        return null
    }

    override fun visitShowNamespaces(ctx: ShowNamespacesContext): Void? {
        builder.append("SHOW ").append(ctx.namespaces().text.uppercase()).append(" ")
        if (ctx.multipartIdentifier() != null) {
            if (ctx.FROM() != null) {
                builder.append("FROM ")
            } else {
                builder.append("IN ")
            }
            visit(ctx.multipartIdentifier())
            builder.append(" ")
        }

        if (ctx.pattern != null) {
            if (ctx.LIKE() != null) {
                builder.append("LIKE ")
            }
            builder.append(ctx.pattern.text)
        }

        return null
    }

    override fun visitReplaceTable(ctx: ReplaceTableContext): Void? {
        visit(ctx.replaceTableHeader())
        if (ctx.LEFT_PAREN() != null) {
            builder.append(" (\n")
            visit(ctx.colTypeList())
            builder.append("\n)")
        }
        if (ctx.tableProvider() != null) {
            builder.append("\n")
            visit(ctx.tableProvider())
        }
        visit(ctx.createTableClauses())
        if (ctx.AS() != null) {
            builder.append(" AS\n")
        }
        if (ctx.query() != null) {
            visit(ctx.query())
        }
        return null
    }

    override fun visitReplaceTableHeader(ctx: ReplaceTableHeaderContext): Void? {
        if (ctx.CREATE() != null) {
            builder.append("CREATE OR ")
        }
        builder.append("REPLACE TABLE ")
        visit(ctx.multipartIdentifier())
        return null
    }

    override fun visitAnalyze(ctx: AnalyzeContext): Void? {
        builder.append("ANALYZE TABLE ")
        visit(ctx.multipartIdentifier())
        builder.append(" ")
        if (ctx.partitionSpec() != null) {
            visit(ctx.partitionSpec())
            builder.append(" ")
        }
        builder.append("COMPUTE STATISTICS ")
        if (ctx.identifier() != null) {
            visit(ctx.identifier())
        } else if (ctx.identifierSeq() != null) {
            builder.append("FOR COLUMNS ")
            visit(ctx.identifierSeq())
        } else if (ctx.ALL() != null) {
            builder.append("FOR ALL COLUMNS")
        }
        return null
    }

    override fun visitAnalyzeTables(ctx: AnalyzeTablesContext): Void? {
        builder.append("ANALYZE TABLES ")
        if (ctx.multipartIdentifier() != null) {
            if (ctx.FROM() != null) {
                builder.append("FROM ")
            } else {
                builder.append("IN ")
            }
            visit(ctx.multipartIdentifier())
            builder.append(" ")
        }
        builder.append("COMPUTE STATISTICS ")
        if (ctx.identifier() != null) {
            visit(ctx.identifier())
        }

        return null
    }

    override fun visitAddTableColumns(ctx: AddTableColumnsContext): Void? {
        builder.append("ALTER TABLE ADD ")
        if (ctx.COLUMNS() != null) {
            builder.append("COLUMNS")
        } else {
            builder.append("COLUMN")
        }
        if (ctx.LEFT_PAREN() != null) {
            builder.append("(\n")
        } else {
            builder.append("\n")
        }
        visit(ctx.columns)
        if (ctx.RIGHT_PAREN() != null) {
            builder.append("\n)")
        }
        return null
    }

    override fun visitQualifiedColTypeWithPositionList(ctx: QualifiedColTypeWithPositionListContext): Void? {
        joinChild(ctx.qualifiedColTypeWithPosition(), INDENT, "", ",\n" + INDENT)
        return null
    }

    override fun visitQualifiedColTypeWithPosition(ctx: QualifiedColTypeWithPositionContext): Void? {
        visit(ctx.name)
        builder.append(" ")
        visit(ctx.dataType())
        if (ctx.NOT() != null) {
            builder.append(" ")
            builder.append("NOT NULL")
        }
        if (ctx.commentSpec() != null) {
            builder.append(" ")
            visit(ctx.commentSpec())
        }
        if (ctx.colPosition() != null) {
            builder.append(" ")
            visit(ctx.colPosition())
        }

        return null
    }

    override fun visitColPosition(ctx: ColPositionContext): Void? {
        if (ctx.FIRST() != null) {
            builder.append("FIRST")
        } else {
            builder.append("AFTER ")
            visit(ctx.afterCol)
        }

        return null
    }

    override fun visitDropTableColumns(ctx: DropTableColumnsContext): Void? {
        builder.append("ALTER TABLE ")
        visit(ctx.multipartIdentifier())
        builder.append(" DROP ")
        if (ctx.COLUMNS() != null) {
            builder.append("COLUMNS ")
        } else {
            builder.append("COLUMN ")
        }
        if (ctx.EXISTS() != null) {
            builder.append("IF EXISTS ")
        }

        if (ctx.LEFT_PAREN() != null) {
            builder.append("(")
        }
        joinChild(ctx.columns.multipartIdentifier(), "", "", ", ")
        if (ctx.RIGHT_PAREN() != null) {
            builder.append(")")
        }
        return null
    }

    override fun visitRenameTableColumn(ctx: RenameTableColumnContext): Void? {
        builder.append("ALTER TABLE ")
        visit(ctx.table)
        builder.append(" RENAME COLUMN ")
        visit(ctx.from)
        builder.append(" TO ")
        visit(ctx.to)
        return null
    }

    override fun visitRenameTable(ctx: RenameTableContext): Void? {
        builder.append("ALTER ")
        if (ctx.TABLE() != null) {
            builder.append("TABLE ")
        } else {
            builder.append("VIEW ")
        }
        visit(ctx.from)
        builder.append(" RENAME TO ")
        visit(ctx.to)
        return null
    }

    override fun visitTouchTable(ctx: TouchTableContext): Void? {
        builder.append("ALTER TABLE ")
        visit(ctx.table)
        builder.append(" TOUCH ")
        if (ctx.partitionSpec() != null) {
            visit(ctx.partitionSpec())
        }
        return null
    }

    override fun visitSetTableProperties(ctx: SetTablePropertiesContext): Void? {
        builder.append("ALTER ")
        if (ctx.TABLE() != null) {
            builder.append("TABLE ")
        } else {
            builder.append("VIEW ")
        }
        visit(ctx.multipartIdentifier())
        builder.append(" SET TBLPROPERTIES")
        visit(ctx.propertyList())
        return null
    }

    override fun visitUnsetTableProperties(ctx: UnsetTablePropertiesContext): Void? {
        builder.append("ALTER ")
        if (ctx.TABLE() != null) {
            builder.append("TABLE ")
        } else {
            builder.append("VIEW ")
        }
        visit(ctx.multipartIdentifier())
        builder.append(" UNSET TBLPROPERTIES")
        if (ctx.EXISTS() != null) {
            builder.append(" IF EXISTS ")
        }
        visit(ctx.propertyList())
        return null
    }

    override fun visitAlterTableAlterColumn(ctx: AlterTableAlterColumnContext): Void? {
        builder.append("ALTER TABLE ")
        visit(ctx.table)
        if (ctx.ALTER() != null) {
            builder.append(" ALTER ")
        } else {
            builder.append(" CHANGE ")
        }
        if (ctx.COLUMN() != null) {
            builder.append("COLUMN ")
        }
        visit(ctx.column)

        if (ctx.alterColumnAction() != null) {
            builder.append(" ")
            visit(ctx.alterColumnAction())
        }
        return null
    }

    override fun visitAlterColumnAction(ctx: AlterColumnActionContext): Void? {
        if (ctx.TYPE() != null) {
            builder.append("TYPE")
            visit(ctx.dataType())
        } else if (ctx.commentSpec() != null) {
            visit(ctx.commentSpec())
        } else if (ctx.colPosition() != null) {
            visit(ctx.colPosition())
        } else {
            builder.append(ctx.setOrDrop.text).append(" NOT NULL")
        }

        return null
    }

    override fun visitHiveChangeColumn(ctx: HiveChangeColumnContext): Void? {
        builder.append("ALTER TABLE ")
        visit(ctx.table)
        if (ctx.partitionSpec() != null) {
            builder.append(" ")
            visit(ctx.partitionSpec())
        }
        builder.append(" CHANGE ")

        if (ctx.COLUMN() != null) {
            builder.append("COLUMN ")
        }
        visit(ctx.colName)
        builder.append(" ")
        visit(ctx.colType())
        if (ctx.colPosition() != null) {
            builder.append(" ")
            visit(ctx.colPosition())
        }
        return null
    }

    override fun visitHiveReplaceColumns(ctx: HiveReplaceColumnsContext): Void? {
        builder.append("ALTER TABLE ")
        visit(ctx.table)
        if (ctx.partitionSpec() != null) {
            builder.append(" ")
            visit(ctx.partitionSpec())
        }
        builder.append(" REPLACE COLUMNS (\n")
        visit(ctx.columns)
        builder.append("\n)")
        return null
    }

    override fun visitAddTablePartition(ctx: AddTablePartitionContext): Void? {
        builder.append("ALTER ")
        if (ctx.TABLE() != null) {
            builder.append("TABLE ")
        } else {
            builder.append("VIEW ")
        }
        visit(ctx.multipartIdentifier())
        builder.append(" ADD")
        if (ctx.EXISTS() != null) {
            builder.append(" IF NOT EXISTS")
        }

        joinChild(ctx.partitionSpecLocation(), "\n" + INDENT, "", "\n" + INDENT)
        return null
    }

    override fun visitPartitionSpecLocation(ctx: PartitionSpecLocationContext): Void? {
        visit(ctx.partitionSpec())
        if (ctx.locationSpec() != null) {
            builder.append(" ")
            visit(ctx.locationSpec())
        }
        return null
    }

    override fun visitRenameTablePartition(ctx: RenameTablePartitionContext): Void? {
        builder.append("ALTER TABLE ")
        visit(ctx.multipartIdentifier())
        builder.append(" ")
        visit(ctx.from)
        builder.append(" RENAME TO ")
        visit(ctx.to)
        return null
    }

    override fun visitDropTablePartitions(ctx: DropTablePartitionsContext): Void? {
        builder.append("ALTER ")
        if (ctx.TABLE() != null) {
            builder.append("TABLE ")
        } else {
            builder.append("VIEW ")
        }
        visit(ctx.multipartIdentifier())
        builder.append(" DROP")
        if (ctx.EXISTS() != null) {
            builder.append(" IF EXISTS")
        }
        joinChild(ctx.partitionSpec(), "\n" + INDENT, "", "\n" + INDENT)

        if (ctx.PURGE() != null) {
            builder.append(" PURGE")
        }
        return null
    }

    override fun visitSetTableLocation(ctx: SetTableLocationContext): Void? {
        builder.append("ALTER TABLE ")
        visit(ctx.multipartIdentifier())
        builder.append(" ")
        if (ctx.partitionSpec() != null) {
            visit(ctx.partitionSpec())
            builder.append(" ")
        }
        builder.append("SET ")
        visit(ctx.locationSpec())
        return null
    }

    override fun visitRecoverPartitions(ctx: RecoverPartitionsContext): Void? {
        builder.append("ALTER TABLE ")
        visit(ctx.multipartIdentifier())
        builder.append(" RECOVER PARTITIONS")
        return null
    }

    override fun visitDropTable(ctx: DropTableContext): Void? {
        joinChildren(ctx.children)
        return null
    }

    override fun visitDropView(ctx: DropViewContext): Void? {
        joinChildren(ctx.children)
        return null
    }

    override fun visitCreateView(ctx: CreateViewContext): Void? {
        builder.append("CREATE ")
        if (ctx.REPLACE() != null) {
            builder.append("OR REPLACE ")
        }
        if (ctx.GLOBAL() != null) {
            builder.append("GLOBAL ")
        }
        if (ctx.TEMPORARY() != null) {
            builder.append("TEMPORARY ")
        }
        builder.append("VIEW ")
        if (ctx.EXISTS() != null) {
            builder.append("IF NOT EXISTS ")
        }
        visit(ctx.multipartIdentifier())
        if (ctx.identifierCommentList() != null) {
            joinChild(ctx.identifierCommentList().identifierComment(), "(\n" + INDENT, "\n)", ",\n" + INDENT)
        }
        if (ctx.commentSpec().size > 0) {
            builder.append("\n")
            visit(ctx.commentSpec().get(0))
        }
        if (ctx.PARTITIONED().size > 0) {
            builder.append("\nPARTITIONED ON ")
            visit(ctx.identifierList().get(0))
        }
        if (ctx.TBLPROPERTIES().size > 0) {
            builder.append("\nTBLPROPERTIES ")
            visit(ctx.propertyList().get(0))
        }
        builder.append("\nAS")
        visit(ctx.query())
        builder.setLength(builder.length - 1); // 去掉最后空行
        return null
    }

    override fun visitIdentifierComment(ctx: IdentifierCommentContext): Void? {
        visit(ctx.identifier())
        if (ctx.commentSpec() != null) {
            builder.append(" ")
            visit(ctx.commentSpec())
        }
        return null
    }

    override fun visitCreateTempViewUsing(ctx: CreateTempViewUsingContext): Void? {
        builder.append("CREATE ")
        if (ctx.REPLACE() != null) {
            builder.append("OR REPLACE ")
        }
        if (ctx.GLOBAL() != null) {
            builder.append("GLOBAL ")
        }

        builder.append("TEMPORARY VIEW ")
        visit(ctx.tableIdentifier())
        if (ctx.LEFT_PAREN() != null) {
            builder.append(" (\n")
            visit(ctx.colTypeList())
            builder.append("\n)")
        }
        builder.append("\n")
        visit(ctx.tableProvider())
        if (ctx.OPTIONS() != null) {
            builder.append("\nOPTIONS")
            visit(ctx.propertyList())
        }

        return null
    }

    override fun visitAlterViewQuery(ctx: AlterViewQueryContext): Void? {
        builder.append("ALTER VIEW ")
        visit(ctx.multipartIdentifier())
        if (ctx.AS() != null) {
            builder.append(" AS\n")
        } else {
            builder.append("\n")
        }
        visit(ctx.query())
        return null
    }

    override fun visitCreateFunction(ctx: CreateFunctionContext): Void? {
        builder.append("CREATE ")
        if (ctx.REPLACE() != null) {
            builder.append("OR REPLACE ")
        }
        if (ctx.TEMPORARY() != null) {
            builder.append("TEMPORARY ")
        }
        builder.append("FUNCTION ")
        if (ctx.EXISTS() != null) {
            builder.append("IF NOT EXISTS ")
        }
        visit(ctx.multipartIdentifier())
        builder.append(" AS ").append(ctx.className.text)
        if (ctx.USING() != null) {
            builder.append(" USING");
            joinChild(ctx.resource(), "\n" + INDENT, "", "\n" + INDENT)
        }

        return null;
    }

    override fun visitResource(ctx: ResourceContext): Void? {
        visit(ctx.identifier())
        builder.append(" ").append(ctx.STRING().text)
        return null;
    }

    override fun visitDropFunction(ctx: DropFunctionContext): Void? {
        builder.append("DROP ")
        if (ctx.TEMPORARY() != null) {
            builder.append("TEMPORARY ")
        }
        builder.append("FUNCTION ")
        if (ctx.EXISTS() != null) {
            builder.append("IF EXISTS ")
        }
        visit(ctx.multipartIdentifier())
        return null
    }

    override fun visitExplain(ctx: ExplainContext): Void? {
        builder.append("EXPLAIN ")
        if (ctx.LOGICAL() != null) {
            builder.append("EXPLAIN ")
        }
        if (ctx.FORMATTED() != null) {
            builder.append("FORMATTED ")
        }
        if (ctx.EXTENDED() != null) {
            builder.append("EXTENDED ")
        }
        if (ctx.CODEGEN() != null) {
            builder.append("CODEGEN ")
        }
        if (ctx.COST() != null) {
            builder.append("COST ")
        }
        visit(ctx.statement())
        return null
    }

    override fun visitShowTables(ctx: ShowTablesContext): Void? {
        builder.append("SHOW TABLES ")
        if (ctx.multipartIdentifier() != null) {
            if (ctx.FROM() != null) {
                builder.append("FROM ")
            } else {
                builder.append("IN ")
            }
            visit(ctx.multipartIdentifier())
        }

        if (ctx.LIKE() != null) {
            builder.append(" LIKE")
        }
        if (ctx.pattern != null) {
            builder.append(ctx.pattern.text)
        }
        return null
    }

    override fun visitShowTableExtended(ctx: ShowTableExtendedContext): Void? {
        builder.append("SHOW TABLE EXTENDED ")
        if (ctx.ns != null) {
            if (ctx.FROM() != null) {
                builder.append("FROM ")
            } else {
                builder.append("IN ")
            }
            visit(ctx.ns)
            builder.append(" ")
        }

        builder.append("LIKE ").append(ctx.pattern.text)
        if (ctx.partitionSpec() != null) {
            builder.append(" ")
            visit(ctx.partitionSpec())
        }
        return null
    }

    override fun visitShowTblProperties(ctx: ShowTblPropertiesContext): Void? {
        builder.append("SHOW TBLPROPERTIES ")
        visit(ctx.table)
        if (ctx.LEFT_PAREN() != null) {
            builder.append(" (")
            visit(ctx.key)
            builder.append(")")
        }
        return null
    }

    override fun visitShowColumns(ctx: ShowColumnsContext): Void? {
        joinChildren(ctx.children)
        return null
    }

    override fun visitShowViews(ctx: ShowViewsContext): Void? {
        builder.append("SHOW VIEWS ")
        if (ctx.multipartIdentifier() != null) {
            if (ctx.FROM() != null) {
                builder.append("FROM ")
            } else {
                builder.append("IN ")
            }
            visit(ctx.multipartIdentifier())
            builder.append(" ")
        }
        if (ctx.LIKE() != null) {
            builder.append("LIKE ")
        }
        if (ctx.pattern != null) {
            builder.append(ctx.pattern.text)
        }
        return null
    }

    override fun visitShowPartitions(ctx: ShowPartitionsContext): Void? {
        builder.append("SHOW PARTITIONS ")
        visit(ctx.multipartIdentifier())
        if (ctx.partitionSpec() != null) {
            builder.append(" ")
            visit(ctx.partitionSpec())
        }
        return null
    }

    override fun visitShowFunctions(ctx: ShowFunctionsContext): Void? {
        builder.append("SHOW ")
        if (ctx.identifier() != null) {
            visit(ctx.identifier())
            builder.append(" ")
        }
        builder.append("FUNCTIONS ")
        if (ctx.ns != null) {
            if (ctx.FROM() != null) {
                builder.append("FROM ")
            } else {
                builder.append("IN ")
            }
            visit(ctx.ns)
            builder.append(" ")
        }

        if (ctx.LIKE() != null) {
            builder.append("LIKE ")
        }
        if (ctx.legacy != null) {
            visit(ctx.legacy)
        }
        if (ctx.pattern != null) {
            builder.append(ctx.pattern.text)
        }

        return null
    }

    override fun visitShowCreateTable(ctx: ShowCreateTableContext): Void? {
        builder.append("SHOW CREATE TABLE ")
        visit(ctx.multipartIdentifier())
        if (ctx.AS() != null) {
            builder.append(" AS SERDE")
        }
        return null
    }

    override fun visitShowCurrentNamespace(ctx: ShowCurrentNamespaceContext): Void? {
        builder.append("SHOW CURRENT ").append(ctx.namespace().text)
        return null
    }

    override fun visitShowCatalogs(ctx: ShowCatalogsContext): Void? {
        builder.append("SHOW CATALOGS ")
        if (ctx.LIKE() != null) {
            builder.append("LIKE ")
        }

        if (ctx.pattern != null) {
            builder.append(ctx.pattern.text)
        }
        return null
    }

    override fun visitDescribeFunction(ctx: DescribeFunctionContext): Void? {
        if (ctx.DESC() != null) {
            builder.append("DESC ")
        } else {
            builder.append("DESCRIBE ")
        }
        if (ctx.EXTENDED() != null) {
            builder.append("EXTENDED ")
        }
        visit(ctx.describeFuncName())
        return null
    }

    override fun visitDescribeNamespace(ctx: DescribeNamespaceContext): Void? {
        if (ctx.DESC() != null) {
            builder.append("DESC ")
        } else {
            builder.append("DESCRIBE ")
        }
        if (ctx.EXTENDED() != null) {
            builder.append("EXTENDED ")
        }
        visit(ctx.multipartIdentifier())
        return null
    }

    override fun visitDescribeRelation(ctx: DescribeRelationContext): Void? {
        if (ctx.DESC() != null) {
            builder.append("DESC ")
        } else {
            builder.append("DESCRIBE ")
        }
        if (ctx.TABLE() != null) {
            builder.append("TABLE ")
        }
        if (ctx.option != null) {
            builder.append(ctx.option.text.uppercase()).append(" ")
        }
        visit(ctx.multipartIdentifier())
        if (ctx.partitionSpec() != null) {
            builder.append(" ")
            visit(ctx.partitionSpec())
        }
        if (ctx.describeColName() != null) {
            builder.append(" ")
            joinChild(ctx.describeColName().nameParts, "", "", ".")
        }
        return null
    }

    override fun visitDescribeQuery(ctx: DescribeQueryContext): Void? {
        if (ctx.DESC() != null) {
            builder.append("DESC ")
        } else {
            builder.append("DESCRIBE ")
        }
        if (ctx.QUERY() != null) {
            builder.append("QUERY ")
        }
        visit(ctx.query())
        return null
    }

    override fun visitCommentNamespace(ctx: CommentNamespaceContext): Void? {
        builder.append("COMMENT ON ").append(ctx.namespace().text).append(" ")
        visit(ctx.multipartIdentifier())
        builder.append(" IS ")
        if (ctx.NULL() != null) {
            builder.append("NULL")
        } else {
            builder.append(ctx.comment.text)
        }
        return null
    }

    override fun visitCommentTable(ctx: CommentTableContext): Void? {
        builder.append("COMMENT ON TABLE ")
        visit(ctx.multipartIdentifier())
        builder.append(" IS ")
        if (ctx.NULL() != null) {
            builder.append("NULL")
        } else {
            builder.append(ctx.comment.text)
        }
        return null
    }

    override fun visitTruncateTable(ctx: TruncateTableContext): Void? {
        joinChildren(ctx.children)
        return null
    }

    override fun visitRepairTable(ctx: RepairTableContext): Void? {
        joinChildren(ctx.children)
        return null
    }

    //-----------------------private method-------------------------------------

    private fun iteratorChild(children: List<ParseTree>, uppercase: Boolean = true,
                              terminalNodeAppend: String = " ", childAppend: String = " ") {
        children.forEach { child ->
            if (child is TerminalNodeImpl) {
                if (uppercase) {
                    builder.append(child.text.uppercase())
                } else {
                    builder.append(child.text)
                }

                builder.append(terminalNodeAppend)
            } else {
                visit(child)
                builder.append(childAppend)
            }
        }
    }

    private fun joinChildren(children: List<ParseTree>, uppercase: Boolean = true, delimiter: String = " ") {
        if (children.size == 0) {
            return
        }

        var first = true
        children.forEach { child ->
            if (first) {
                first = false
            } else {
                builder.append(delimiter)
            }

            if (child is TerminalNodeImpl) {
                if (uppercase) {
                    builder.append(child.text.uppercase())
                } else {
                    builder.append(child.text)
                }
            } else {
                visit(child)
            }
        }
    }

    private fun joinChild(children: List<ParseTree>, start: String = "(", end: String = ")", delimiter: String = ", "): Unit {
        if (children.size == 0) {
            return
        }

        builder.append(start)
        var first = true
        children.forEach { param ->
            if (first) {
                first = false
            } else {
                builder.append(delimiter)
            }
            visit(param)
        }
        builder.append(end)
    }
}