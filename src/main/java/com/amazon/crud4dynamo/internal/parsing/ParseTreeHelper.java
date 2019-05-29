package com.amazon.crud4dynamo.internal.parsing;

import com.amazon.crud4dynamo.ddbparser.ConditionExpressionLexer;
import com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser;
import com.amazon.crud4dynamo.ddbparser.ParserFactory;
import com.amazon.crud4dynamo.ddbparser.UpdateExpressionLexer;
import com.amazon.crud4dynamo.ddbparser.UpdateExpressionParser;
import com.google.common.base.Strings;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParseTreeHelper {
    public static Optional<ConditionExpressionParser.StartContext> getRootOfConditionExpr(final String expression) {
        return Optional.ofNullable(expression)
                .filter(s -> !Strings.isNullOrEmpty(s))
                .map(expr -> new ParserFactory<>(ConditionExpressionLexer.class, ConditionExpressionParser.class).create(expr))
                .map(ConditionExpressionParser::start);
    }

    public static Optional<UpdateExpressionParser.StartContext> getRootOfUpdateExpr(final String expression) {
        return Optional.ofNullable(expression)
                .filter(s -> !Strings.isNullOrEmpty(s))
                .map(expr -> new ParserFactory<>(UpdateExpressionLexer.class, UpdateExpressionParser.class).create(expr))
                .map(UpdateExpressionParser::start);
    }
}
