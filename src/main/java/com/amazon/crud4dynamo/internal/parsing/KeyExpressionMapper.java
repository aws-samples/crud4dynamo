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

import com.amazon.crud4dynamo.ddbparser.KeyExpressionBaseVisitor;
import com.amazon.crud4dynamo.ddbparser.KeyExpressionLexer;
import com.amazon.crud4dynamo.ddbparser.KeyExpressionParser;
import com.amazon.crud4dynamo.ddbparser.KeyExpressionParser.EqualityExpressionContext;
import com.amazon.crud4dynamo.ddbparser.KeyExpressionParser.HashKeyExpressionContext;
import com.amazon.crud4dynamo.ddbparser.KeyExpressionParser.RangeKeyExpressionContext;
import com.amazon.crud4dynamo.ddbparser.KeyExpressionParser.StartContext;
import com.amazon.crud4dynamo.ddbparser.ParserFactory;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import java.util.Optional;
import lombok.Builder;
import lombok.Value;
import org.antlr.v4.runtime.tree.ParseTree;

public class KeyExpressionMapper {
  @Value
  @Builder
  public static class Context {
    private final String keyStringText;
    private final AttributeNameMapper nameMapper;
    private final AttributeValueMapper valueMapper;
  }

  private final String keyExpression;
  private final DynamoDBMapperTableModel tableModel;
  private final Optional<StartContext> contextRoot;

  public KeyExpressionMapper(
      final String keyExpression, final DynamoDBMapperTableModel tableModel) {
    this.keyExpression = keyExpression;
    this.tableModel = tableModel;
    contextRoot = getContextRoot();
  }

  public Context getHashKeyContext() {
    final AttributeNameMapper nameMapper = new AttributeNameMapper();
    final AttributeValueMapper valueMapper = new AttributeValueMapper();
    final String hashKeyText =
        contextRoot
            .map(
                root ->
                    root.accept(
                        new KeyExpressionBaseVisitor<String>() {
                          @Override
                          public String visitHashKeyExpression(final HashKeyExpressionContext ctx) {
                            final EqualityExpressionContext equalExpr = ctx.equalityExpression();
                            return doVisit(equalExpr, nameMapper, valueMapper);
                          }

                          @Override
                          protected String aggregateResult(
                              final String aggregate, final String nextResult) {
                            return aggregate != null ? aggregate : nextResult;
                          }
                        }))
            .orElseThrow(() -> new IllegalStateException("HashKey should not be empty"));
    return Context.builder()
        .keyStringText(hashKeyText)
        .nameMapper(nameMapper)
        .valueMapper(valueMapper)
        .build();
  }

  private String doVisit(
      final EqualityExpressionContext equalExpr,
      final AttributeNameMapper nameMapper,
      final AttributeValueMapper valueMapper) {
    equalExpr.accept(newNameMapperVisitor(nameMapper));
    equalExpr.accept(newValueMapperVisitor(valueMapper));
    return Optional.ofNullable(equalExpr.ATTRIBUTE_NAME())
        .flatMap(symbol -> Optional.of(symbol.getText()))
        .orElseGet(
            () ->
                Optional.ofNullable(equalExpr.EXPRESSION_ATTRIBUTE_NAME())
                    .map(ParseTree::getText)
                    .get());
  }

  private KeyExpressionBaseVisitor<Void> newValueMapperVisitor(
      final AttributeValueMapper valueMapper) {
    return new KeyExpressionBaseVisitor<Void>() {
      @Override
      public Void visitEqualityExpression(final EqualityExpressionContext ctx) {
        Optional.ofNullable(ctx.ATTRIBUTE_NAME())
            .map(ParseTree::getText)
            .ifPresent(
                attributeName ->
                    valueMapper.put(
                        ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                        tableModel.field(attributeName)::convert));
        return null;
      }
    };
  }

  private KeyExpressionBaseVisitor<Void> newNameMapperVisitor(
      final AttributeNameMapper nameMapper) {
    return new KeyExpressionBaseVisitor<Void>() {
      @Override
      public Void visitEqualityExpression(final EqualityExpressionContext ctx) {
        Optional.ofNullable(ctx.EXPRESSION_ATTRIBUTE_NAME())
            .map(ParseTree::getText)
            .ifPresent(
                placeholder ->
                    nameMapper.put(
                        placeholder, newLazyConverter(ctx.EXPRESSION_ATTRIBUTE_VALUE().getText())));
        return super.visitEqualityExpression(ctx);
      }
    };
  }

  public Optional<Context> getRangeKeyContext() {
    final AttributeNameMapper nameMapper = new AttributeNameMapper();
    final AttributeValueMapper valueMapper = new AttributeValueMapper();
    final Optional<String> keyText =
        contextRoot.map(
            root ->
                root.accept(
                    new KeyExpressionBaseVisitor<String>() {
                      @Override
                      public String visitRangeKeyExpression(final RangeKeyExpressionContext ctx) {
                        final EqualityExpressionContext equalExpr = ctx.equalityExpression();
                        return doVisit(equalExpr, nameMapper, valueMapper);
                      }

                      @Override
                      protected String aggregateResult(
                          final String aggregate, final String nextResult) {
                        return aggregate != null ? aggregate : nextResult;
                      }
                    }));
    return keyText.map(
        text ->
            Context.builder()
                .keyStringText(text)
                .nameMapper(nameMapper)
                .valueMapper(valueMapper)
                .build());
  }

  private Function<String, NameAwareConverter> newLazyConverter(final String attributeValue) {
    return name ->
        new NameAwareConverter() {
          @Override
          public String getName() {
            return attributeValue;
          }

          @Override
          public AttributeValue convert(final Object object) {
            return tableModel.field(name).convert(object);
          }
        };
  }

  private Optional<StartContext> getContextRoot() {
    return Optional.ofNullable(keyExpression)
        .filter(s -> !Strings.isNullOrEmpty(s))
        .map(
            expr ->
                new ParserFactory<>(KeyExpressionLexer.class, KeyExpressionParser.class)
                    .create(expr))
        .map(KeyExpressionParser::start);
  }
}
