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

import com.amazon.crud4dynamo.ddbparser.ConditionExpressionBaseVisitor;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * Condition Expression Parser
 *
 * <p>It visits the parsed tree and creates the mapping relationships between expression attribute name and attribute name, and between
 * expression attribute value and attribute value.
 */
public class ConditionExpressionParser implements ExpressionParser {
    private final String conditionExpression;
    private final DynamoDBMapperTableModel tableModel;
    private final Optional<com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.StartContext> contextRoot;
    @Getter private final AttributeNameMapper attributeNameMapper;
    @Getter private final AttributeValueMapper attributeValueMapper;
    @Getter private final Set<String> expressionAttributeNames;

    public ConditionExpressionParser(final String conditionExpression, final DynamoDBMapperTableModel tableModel) {
        this.conditionExpression = conditionExpression;
        this.tableModel = tableModel;
        contextRoot = ParseTreeHelper.getRootOfConditionExpr(conditionExpression);
        attributeNameMapper = newNameMapper();
        attributeValueMapper = newValueMapper();
        expressionAttributeNames = newExpressionAttributeNames();
    }

    private AttributeValueMapper newValueMapper() {
        final AttributeValueMapper mapper = new AttributeValueMapper();
        contextRoot.ifPresent(
                root ->
                        root.accept(
                                new ConditionExpressionBaseVisitor<Void>() {
                                    @Override
                                    public Void visitLeftComparisonExpression(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.LeftComparisonExpressionContext
                                                    ctx) {
                                        final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.PathContext path = ctx.path();
                                        if (isNestedPath(path)) {
                                            mapper.put(
                                                    ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                    new ArgumentTypeBasedConverter(path.getText())::convert);
                                        } else {
                                            getAttributeName(ctx.path())
                                                    .ifPresent(
                                                            attrName ->
                                                                    mapper.put(
                                                                            ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                                            tableModel.field(attrName)::convert));
                                        }
                                        return null;
                                    }

                                    @Override
                                    public Void visitRightComparisonExpression(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser
                                                            .RightComparisonExpressionContext
                                                    ctx) {
                                        final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.PathContext path = ctx.path();
                                        if (isNestedPath(path)) {
                                            mapper.put(
                                                    ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                    obj -> new ArgumentTypeBasedConverter(path.getText()).convert(obj));
                                        } else {
                                            getAttributeName(ctx.path())
                                                    .ifPresent(
                                                            attrName ->
                                                                    mapper.put(
                                                                            ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                                            tableModel.field(attrName)::convert));
                                        }
                                        return null;
                                    }

                                    @Override
                                    public Void visitBetweenExp(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.BetweenExpContext ctx) {
                                        final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.PathContext path = ctx.path();
                                        if (isNestedPath(path)) {
                                            ctx.EXPRESSION_ATTRIBUTE_VALUE()
                                                    .forEach(
                                                            value ->
                                                                    mapper.put(
                                                                            value.getText(),
                                                                            new ArgumentTypeBasedConverter(path.getText())::convert));
                                        } else {
                                            getAttributeName(ctx.path())
                                                    .ifPresent(
                                                            attrName ->
                                                                    ctx.EXPRESSION_ATTRIBUTE_VALUE()
                                                                            .forEach(
                                                                                    value ->
                                                                                            mapper.put(
                                                                                                    value.getText(),
                                                                                                    tableModel.field(attrName)::convert)));
                                        }
                                        return null;
                                    }

                                    @Override
                                    public Void visitInExp(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.InExpContext ctx) {
                                        final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.PathContext path = ctx.path();
                                        if (isNestedPath(path)) {
                                            ctx.EXPRESSION_ATTRIBUTE_VALUE()
                                                    .forEach(
                                                            value ->
                                                                    mapper.put(
                                                                            value.getText(),
                                                                            obj ->
                                                                                    new ArgumentTypeBasedConverter(path.getText())
                                                                                            .convert(obj)));
                                        } else {
                                            getAttributeName(ctx.path())
                                                    .ifPresent(
                                                            attrName ->
                                                                    ctx.EXPRESSION_ATTRIBUTE_VALUE()
                                                                            .forEach(
                                                                                    value ->
                                                                                            mapper.put(
                                                                                                    value.getText(),
                                                                                                    tableModel.field(attrName)::convert)));
                                        }
                                        return null;
                                    }

                                    @Override
                                    public Void visitAttrTypeFunExp(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.AttrTypeFunExpContext ctx) {
                                        mapper.put(
                                                ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                obj -> new AttributeValue().withS(obj.toString()));
                                        return null;
                                    }

                                    @Override
                                    public Void visitBeginsWithFunExp(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.BeginsWithFunExpContext ctx) {
                                        mapper.put(
                                                ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                obj -> new AttributeValue().withS(obj.toString()));
                                        return null;
                                    }

                                    @Override
                                    public Void visitContainsFunExp(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.ContainsFunExpContext ctx) {
                                        mapper.put(
                                                ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                obj -> new AttributeValue().withS(obj.toString()));
                                        return null;
                                    }

                                    private Optional<String> getAttributeName(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.PathContext path) {
                                        return Optional.ofNullable(path.ATTRIBUTE_NAME()).map(ParseTree::getText);
                                    }

                                    private boolean isNestedPath(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.PathContext path) {
                                        return Optional.ofNullable(path.nestedPath()).isPresent();
                                    }
                                }));
        return mapper;
    }

    private AttributeNameMapper newNameMapper() {
        final AttributeNameMapper mapper = new AttributeNameMapper();
        contextRoot.ifPresent(
                root ->
                        root.accept(
                                new ConditionExpressionBaseVisitor<Void>() {
                                    @Override
                                    public Void visitLeftComparisonExpression(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.LeftComparisonExpressionContext
                                                    ctx) {
                                        final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.PathContext path = ctx.path();
                                        getExpAttrName(path)
                                                .ifPresent(
                                                        nameHolder ->
                                                                mapper.put(
                                                                        nameHolder,
                                                                        NameAwareConverter.newLazyConverter(
                                                                                ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(), tableModel)));
                                        return null;
                                    }

                                    @Override
                                    public Void visitRightComparisonExpression(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser
                                                            .RightComparisonExpressionContext
                                                    ctx) {
                                        final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.PathContext path = ctx.path();
                                        getExpAttrName(path)
                                                .ifPresent(
                                                        nameHolder ->
                                                                mapper.put(
                                                                        nameHolder,
                                                                        NameAwareConverter.newLazyConverter(
                                                                                ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(), tableModel)));
                                        return null;
                                    }

                                    @Override
                                    public Void visitBetweenExp(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.BetweenExpContext ctx) {
                                        final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.PathContext path = ctx.path();
                                        getExpAttrName(path)
                                                .ifPresent(
                                                        nameHolder ->
                                                                ctx.EXPRESSION_ATTRIBUTE_VALUE()
                                                                        .stream()
                                                                        .map(ParseTree::getText)
                                                                        .map(name -> NameAwareConverter.newLazyConverter(name, tableModel))
                                                                        .forEach(converter -> mapper.put(nameHolder, converter)));
                                        return null;
                                    }

                                    @Override
                                    public Void visitInExp(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.InExpContext ctx) {
                                        final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.PathContext path = ctx.path();
                                        getExpAttrName(path)
                                                .ifPresent(
                                                        nameHolder ->
                                                                ctx.EXPRESSION_ATTRIBUTE_VALUE()
                                                                        .stream()
                                                                        .map(ParseTree::getText)
                                                                        .map(name -> NameAwareConverter.newLazyConverter(name, tableModel))
                                                                        .forEach(converter -> mapper.put(nameHolder, converter)));
                                        return null;
                                    }

                                    private Optional<String> getExpAttrName(
                                            final com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.PathContext path) {
                                        return Optional.ofNullable(path.EXPRESSION_ATTRIBUTE_NAME()).map(ParseTree::getText);
                                    }
                                }));
        return mapper;
    }

    private Set<String> newExpressionAttributeNames() {
        final Set<String> names = new HashSet<>();
        contextRoot.ifPresent(
                ctx ->
                        ctx.accept(
                                new ConditionExpressionBaseVisitor<Void>() {
                                    @Override
                                    public Void visitTerminal(final TerminalNode node) {
                                        if (node.getSymbol().getType()
                                                == com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.EXPRESSION_ATTRIBUTE_NAME) {
                                            names.add(node.getText());
                                        }
                                        return super.visitTerminal(node);
                                    }
                                }));
        return names;
    }
}
