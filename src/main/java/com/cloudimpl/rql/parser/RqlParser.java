/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.parser;

import com.cloudimpl.rql.AllColumnNode;
import com.cloudimpl.rql.BinNode;
import com.cloudimpl.rql.ColumRefNode;
import com.cloudimpl.rql.ColumnNode;
import com.cloudimpl.rql.ConstNode;
import com.cloudimpl.rql.FieldCheckNode;
import com.cloudimpl.rql.GroupByNode;
import com.cloudimpl.rql.MaxFunction;
import com.cloudimpl.rql.MinFunction;
import com.cloudimpl.rql.OrderByItem;
import com.cloudimpl.rql.OrderByNode;
import com.cloudimpl.rql.RelNode;
import com.cloudimpl.rql.RqlBoolNode;
import com.cloudimpl.rql.RqlException;
import com.cloudimpl.rql.RqlNode;
import com.cloudimpl.rql.SelectNode;
import com.cloudimpl.rql.SumFunction;
import com.cloudimpl.rql.TimeUnitNode;
import com.cloudimpl.rql.VarNode;
import com.cloudimpl.rql.WindowNode;
import com.cloudimpl.rql.WindowTumblingNode;
import org.parboiled.BaseParser;
import org.parboiled.Parboiled;
import org.parboiled.Rule;
import org.parboiled.annotations.DontLabel;
import org.parboiled.annotations.MemoMismatches;
import org.parboiled.annotations.SuppressNode;
import org.parboiled.annotations.SuppressSubnodes;
import static org.parboiled.errors.ErrorUtils.printParseErrors;
import org.parboiled.parserunners.ReportingParseRunner;
import org.parboiled.support.ParsingResult;

/**
 *
 * @author nuwansa
 */
public class RqlParser extends BaseParser<RqlNode> {

    final Rule EQ = Terminal("=", Ch('='));
    final Rule GT = Terminal(">", AnyOf("=>"));
    final Rule NE = Terminal("<>");
    final Rule GTE = Terminal(">=");
    final Rule LT = Terminal("<", AnyOf("=<"));
    final Rule LTE = Terminal("<=");
    final Rule AND = StringIgnoreCaseWS("and").label("AND");
    final Rule OR = StringIgnoreCaseWS("or").label("OR");
    final Rule OB = Terminal("(");
    final Rule CB = Terminal(")");
    final Rule COMMA = Terminal(",");

    public Rule selectQuery() {
        return Sequence(select(), selectExpressionList(), fromClause(),
                 Optional(windowExpression()), whereClause(), Optional(groupByExpression()), Optional(orderBy()),Optional(limitExpression()), EOI
                ,push(pop(SelectNode.class).complete()));
    }

    Rule selectExpressionList() {
        return Sequence(selectExpression(), ZeroOrMore(Sequence(COMMA, selectExpression())));
    }

    Rule selectExpression() {
        return Sequence(FirstOf(
                selectAll(),
                Sequence(Identifier(false), Ch('.'), asterisk()),
                selectTermWithAlias()
        ), push(pop(1, SelectNode.class).addColumn(pop(ColumnNode.class))));
    }

    Rule selectTermWithAlias() {
        return Sequence(selectTerm(), Optional(Sequence(StringIgnoreCaseWS("as"), Identifier(true), push(pop(ColumnNode.class).setAlias(match().trim()))))
        );
    }

    Rule selectTerm() {
        return FirstOf(
                literal(),
                selectFunction(),
                selectColumnRef()          
        );
    }

    Rule selectFunction() {
        return FirstOf(
                maxFunction(),
                minFunction(),
                sumFunction()
        );
    }

    Rule selectColumnRef() {
        return Sequence(Identifier(true), push(new ColumRefNode(match())));
    }

    Rule maxFunction() {
        return Sequence(IgnoreCase("max"), OB, Identifier(true),push(new MaxFunction(match())), CB).label("max");
    }

    Rule minFunction() {
        return Sequence(IgnoreCase("min"), OB, Identifier(true),push(new MinFunction(match())), CB).label("min");
    }

    Rule sumFunction() {
        return Sequence(IgnoreCase("sum"), OB, Identifier(true),push(new SumFunction(match())), CB).label("sum");
    }

    Rule limitExpression() {
        return Sequence(StringIgnoreCaseWS("limit"), IntegerLiteral(), push(pop(SelectNode.class).setLimit(match())));
    }

    Rule windowExpression() {
        return Sequence(StringIgnoreCaseWS("WINDOW"), windowTumbling(), push(pop(1, SelectNode.class).setWindowNode(pop(WindowNode.class))));
    }

    Rule windowTumbling() {
        return Sequence(StringIgnoreCaseWS("TUMBLING"), OB, StringIgnoreCaseWS("size"),
                 IntegerLiteral(), push(new ConstNode(match())), Spacing(), windowTimeUnit(), CB,
                 push(new WindowTumblingNode(pop(1, ConstNode.class).getValue(), pop(TimeUnitNode.class).getTimeUnit()))
        );
    }

    Rule groupByExpression() {
        return Sequence(StringIgnoreCaseWS("group"), StringIgnoreCaseWS("by"), push(new GroupByNode()), groupByList(), push(pop(1, SelectNode.class)
                .setGroupBy(pop(GroupByNode.class))));
    }

    Rule groupByList() {
        return Sequence(Identifier(true), push(pop(GroupByNode.class).addField(match().trim())),
                 ZeroOrMore(Sequence(COMMA, Identifier(true), push(pop(GroupByNode.class).addField(match().trim())))));
    }

    Rule orderBy(){
        return Sequence(StringIgnoreCaseWS("order"),StringIgnoreCaseWS("by"),push(new OrderByNode()),orderByExpression(),push(pop(1, SelectNode.class)
                .setOrderBy(pop(OrderByNode.class))));
    }
    
    Rule orderByExpression() {
        return Sequence(orderByItem(), push(pop(1,OrderByNode.class).addField(pop(OrderByItem.class))),
                 ZeroOrMore(Sequence(COMMA, orderByItem(), push(pop(1,OrderByNode.class).addField(pop(OrderByItem.class))))));
    }
    
    Rule orderByItem(){
        return Sequence(Identifier(true),push(new OrderByItem(match(),"ASC")),Optional(FirstOf(StringIgnoreCaseWS("ASC"),StringIgnoreCaseWS("DESC")))
                ,push(pop(OrderByItem.class).setOrderBy(match())));
    }
    
    <T> T pop(Class<T> cls) {
        return cls.cast(super.pop());
    }

    <T> T pop(int index, Class<T> cls) {
        return cls.cast(super.pop(index));
    }

    Rule windowTimeUnit() {
        return Sequence(
                FirstOf(StringIgnoreCaseWS("seconds"), StringIgnoreCaseWS("second"),
                        StringIgnoreCaseWS("minutes"), StringIgnoreCaseWS("minute"),
                        StringIgnoreCaseWS("hours"), StringIgnoreCaseWS("hour"),
                        StringIgnoreCaseWS("days"), StringIgnoreCaseWS("day")
                ), push(new TimeUnitNode(match())));
    }

    public Rule fromClause() {
        return Sequence(from(), Identifier(true), push(((SelectNode) pop()).setTableName(match())));
    }

    public Rule whereClause() {
        return Optional(Sequence(where(), BooleanExpression(), push(pop(1, SelectNode.class).setExpression(pop(RqlBoolNode.class)))));
    }

    public Rule BooleanExpression() {
        return Sequence(BooleanTerm(), Optional(OR, BooleanTerm(), push(new BinNode(pop(1, RqlBoolNode.class), BinNode.Op.OR, pop(RqlBoolNode.class)))));
    }

    Rule BooleanFactor() {
        return FirstOf(BooleanFieldExp(), Parens());
    }

    Rule BooleanTerm() {
        return Sequence(BooleanFactor(), Optional(AND, BooleanFactor(), push(new BinNode(pop(1, RqlBoolNode.class), BinNode.Op.AND, pop(RqlBoolNode.class)))));
    }

    public Rule BooleanFieldExp() {
        return Sequence(Identifier(true), push(new VarNode(match())), FirstOf(
                Sequence(EQ, FieldValueExp(), push(new RelNode(pop(1, VarNode.class).getVar(), RelNode.Op.EQ, pop(ConstNode.class)))),
                Sequence(NE, FieldValueExp(), push(new RelNode(pop(1, VarNode.class).getVar(), RelNode.Op.NE, pop(ConstNode.class)))),
                Sequence(GT, FieldValueExp(), push(new RelNode(pop(1, VarNode.class).getVar(), RelNode.Op.GT, pop(ConstNode.class)))),
                Sequence(GTE, FieldValueExp(), push(new RelNode(pop(1, VarNode.class).getVar(), RelNode.Op.GTE, pop(ConstNode.class)))),
                Sequence(LT, FieldValueExp(), push(new RelNode(pop(1, VarNode.class).getVar(), RelNode.Op.LT, pop(ConstNode.class)))),
                Sequence(LTE, FieldValueExp(), push(new RelNode(pop(1, VarNode.class).getVar(), RelNode.Op.LTE, pop(ConstNode.class)))),
                isNull(),
                isNotNull()
        ));
    }

    public Rule FieldValueExp() {
        return Sequence(literal(), push(new ConstNode(match())));
    }

    public Rule isNull() {
        return Sequence(StringIgnoreCaseWS("is"), StringIgnoreCaseWS("null"), push(new FieldCheckNode(pop(VarNode.class).getVar(), false))).suppressSubnodes().label("is null");
    }

    public Rule isNotNull() {
        return Sequence(StringIgnoreCaseWS("is"), StringIgnoreCaseWS("not"), StringIgnoreCaseWS("null"),
                 push(new FieldCheckNode(pop(VarNode.class).getVar(), true))).suppressSubnodes().label("is not null");
    }

    public Rule Parens() {
        return Sequence(OB, BooleanExpression(), CB);
    }

    public Rule select() {
        return Sequence(StringIgnoreCaseWS("SELECT"), push(new SelectNode()));
    }

    public Rule where() {
        return StringIgnoreCaseWS("WHERE");
    }

    public Rule StringIgnoreCaseWS(String string) {
        return Sequence(IgnoreCase(string), WS());
    }

    public Rule WS() {
        return ZeroOrMore(FirstOf(COMMENT(), WS_NO_COMMENT()));
    }

    public Rule COMMENT() {
        return Sequence('#', ZeroOrMore(Sequence(TestNot(EOL()), ANY)), EOL());
    }

    public Rule EOL() {
        return AnyOf("\n\r");
    }

    public Rule WS_NO_COMMENT() {
        return FirstOf(Ch(' '), Ch('\t'), Ch('\f'), EOL());
    }

    public Rule asterisk() {
        return ChWS('*');
    }

    public Rule selectAll() {
        return Sequence(ChWS('*'), push(new AllColumnNode()));
    }

    public Rule ChWS(char c) {
        return Sequence(Ch(c), WS());
    }

    public Rule from() {
        return StringIgnoreCaseWS("FROM");
    }

    @SuppressSubnodes
    @MemoMismatches
    Rule Identifier(boolean space) {
        return Sequence(Letter(), ZeroOrMore(LetterOrDigit()), Spacing());
    }

    Rule Letter() {
        // switch to this "reduced" character space version for a ~10% parser performance speedup
        //return FirstOf(CharRange('a', 'z'), CharRange('A', 'Z'), '_', '$');
        return FirstOf(Sequence('\\', UnicodeEscape()), new JavaLetterMatcher());
    }

    Rule literal() {
        return Sequence(
                FirstOf(
                        FloatLiteral(),
                        IntegerLiteral(),
                        CharLiteral(),
                        StringLiteral(),
                        Sequence("true", TestNot(LetterOrDigit())),
                        Sequence("false", TestNot(LetterOrDigit())),
                        Sequence("null", TestNot(LetterOrDigit()))
                ),
                Spacing()
        );
    }

    Rule Spacing() {
        return ZeroOrMore(AnyOf(" \t\r\n\f").label("Whitespace"));
    }

    Rule StringLiteral() {
        return FirstOf(Sequence(
                '"',
                ZeroOrMore(
                        FirstOf(
                                Escape(),
                                Sequence(TestNot(AnyOf("\r\n\"\\")), ANY)
                        )
                ).suppressSubnodes(),
                '"'
        ), Sequence(
                '\'',
                ZeroOrMore(
                        FirstOf(
                                Escape(),
                                Sequence(TestNot(AnyOf("\r\n\'\\")), ANY)
                        )
                ).suppressSubnodes(),
                '\''
        ));
    }

    @MemoMismatches
    Rule LetterOrDigit() {
        // switch to this "reduced" character space version for a ~10% parser performance speedup
        //return FirstOf(CharRange('a', 'z'), CharRange('A', 'Z'), CharRange('0', '9'), '_', '$');
        return FirstOf(Sequence('\\', UnicodeEscape()), new JavaLetterOrDigitMatcher());
    }

    Rule CharLiteral() {
        return Sequence(
                '\'',
                FirstOf(Escape(), Sequence(TestNot(AnyOf("'\\")), ANY)).suppressSubnodes(),
                '\''
        );
    }

    Rule Escape() {
        return Sequence('\\', FirstOf(AnyOf("btnfr\"\'\\"), OctalEscape(), UnicodeEscape()));
    }

    Rule OctalEscape() {
        return FirstOf(
                Sequence(CharRange('0', '3'), CharRange('0', '7'), CharRange('0', '7')),
                Sequence(CharRange('0', '7'), CharRange('0', '7')),
                CharRange('0', '7')
        );
    }

    Rule UnicodeEscape() {
        return Sequence(OneOrMore('u'), HexDigit(), HexDigit(), HexDigit(), HexDigit());
    }

    Rule FloatLiteral() {
        return FirstOf(HexFloat(), DecimalFloat());
    }

    @SuppressSubnodes
    Rule IntegerLiteral() {
        return Sequence(FirstOf(HexNumeral(), OctalNumeral(), DecimalNumeral()), Optional(AnyOf("lL")));
    }

    @SuppressSubnodes
    Rule OctalNumeral() {
        return Sequence('0', OneOrMore(CharRange('0', '7')));
    }

    @SuppressSubnodes
    Rule DecimalNumeral() {
        return FirstOf('0', Sequence(CharRange('1', '9'), ZeroOrMore(Digit())));
    }

    @SuppressSubnodes
    Rule DecimalFloat() {
        return FirstOf(
                Sequence(OneOrMore(Digit()), '.', ZeroOrMore(Digit()), Optional(Exponent()), Optional(AnyOf("fFdD"))),
                Sequence('.', OneOrMore(Digit()), Optional(Exponent()), Optional(AnyOf("fFdD"))),
                Sequence(OneOrMore(Digit()), Exponent(), Optional(AnyOf("fFdD"))),
                Sequence(OneOrMore(Digit()), Optional(Exponent()), AnyOf("fFdD"))
        );
    }

    Rule Exponent() {
        return Sequence(AnyOf("eE"), Optional(AnyOf("+-")), OneOrMore(Digit()));
    }

    Rule BinaryExponent() {
        return Sequence(AnyOf("pP"), Optional(AnyOf("+-")), OneOrMore(Digit()));
    }

    Rule Digit() {
        return CharRange('0', '9');
    }

    @SuppressSubnodes
    Rule HexFloat() {
        return Sequence(HexSignificant(), BinaryExponent(), Optional(AnyOf("fFdD")));
    }

    Rule HexSignificant() {
        return FirstOf(
                Sequence(FirstOf("0x", "0X"), ZeroOrMore(HexDigit()), '.', OneOrMore(HexDigit())),
                Sequence(HexNumeral(), Optional('.'))
        );
    }

    @MemoMismatches
    Rule HexNumeral() {
        return Sequence('0', IgnoreCase('x'), OneOrMore(HexDigit()));
    }

    Rule HexDigit() {
        return FirstOf(CharRange('a', 'f'), CharRange('A', 'F'), CharRange('0', '9'));
    }

    @SuppressNode
    @DontLabel
    Rule Terminal(String string) {
        return Sequence(string, Spacing()).label('\'' + string + '\'');
    }

    @SuppressNode
    @DontLabel
    Rule Terminal(String string, Rule mustNotFollow) {
        return Sequence(string, TestNot(mustNotFollow), Spacing()).label('\'' + string + '\'');
    }

    public static SelectNode parse(String rql) {
        RqlParser parser = Parboiled.createParser(RqlParser.class);
        ParsingResult<?> result = new ReportingParseRunner(parser.selectQuery()).run(rql);

        if (result.hasErrors()) {
            throw new RqlException(printParseErrors(result));
        }
        SelectNode node = (SelectNode) result.resultValue;
        return node;
    }
}
