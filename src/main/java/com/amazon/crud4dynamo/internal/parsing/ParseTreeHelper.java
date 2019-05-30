/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

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
