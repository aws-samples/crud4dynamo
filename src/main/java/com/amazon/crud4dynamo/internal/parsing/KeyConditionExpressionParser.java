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

import com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionBaseVisitor;
import com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionLexer;
import com.amazon.crud4dynamo.ddbparser.ParserFactory;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.google.common.base.Strings;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * Key Condition Expression Parser
 *
 * <p>It visits the parsed tree and creates the mapping relationships between expression attribute name and attribute name, and between
 * expression attribute value and attribute value.
 */
public class KeyConditionExpressionParser implements ExpressionParser {
    private final String keyCondition;
    private final DynamoDBMapperTableModel tableModel;
    private final Optional<com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser.StartContext> contextRoot;
    @Getter private final AttributeNameMapper attributeNameMapper;
    @Getter private final AttributeValueMapper attributeValueMapper;
    @Getter private final Set<String> expressionAttributeNames;

    public KeyConditionExpressionParser(final String keyCondition, final DynamoDBMapperTableModel tableModel) {
        this.keyCondition = keyCondition;
        this.tableModel = tableModel;
        contextRoot = getContextRoot();
        attributeNameMapper = newNameMapper();
        attributeValueMapper = newValueMapper();
        expressionAttributeNames = newExpressionAttributeNames();
    }

    private AttributeValueMapper newValueMapper() {
        final AttributeValueMapper mapper = new AttributeValueMapper();
        contextRoot.ifPresent(
                root ->
                        root.accept(
                                new KeyConditionExpressionBaseVisitor<Void>() {
                                    @Override
                                    public Void visitPartitionKeyExpWithAttrName(
                                            final com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser
                                                            .PartitionKeyExpWithAttrNameContext
                                                    ctx) {
                                        mapper.put(
                                                ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                tableModel.field(ctx.ATTRIBUTE_NAME().getText())::convert);
                                        return null;
                                    }

                                    @Override
                                    public Void visitCompareExpWithAttrName(
                                            final com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser
                                                            .CompareExpWithAttrNameContext
                                                    ctx) {
                                        mapper.put(
                                                ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                tableModel.field(ctx.ATTRIBUTE_NAME().getText())::convert);
                                        return null;
                                    }

                                    @Override
                                    public Void visitBetweenExpWithAttrName(
                                            final com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser
                                                            .BetweenExpWithAttrNameContext
                                                    ctx) {
                                        final AttributeValueConverter convertFun =
                                                tableModel.field(ctx.ATTRIBUTE_NAME().getText())::convert;
                                        ctx.EXPRESSION_ATTRIBUTE_VALUE()
                                                .stream()
                                                .map(ParseTree::getText)
                                                .forEach(val -> mapper.put(val, convertFun));
                                        return null;
                                    }

                                    @Override
                                    public Void visitBeginsWithExpWithAttrName(
                                            final com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser
                                                            .BeginsWithExpWithAttrNameContext
                                                    ctx) {
                                        mapper.put(
                                                ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                tableModel.field(ctx.ATTRIBUTE_NAME().getText())::convert);
                                        return null;
                                    }
                                }));
        return mapper;
    }

    private AttributeNameMapper newNameMapper() {
        final AttributeNameMapper mapper = new AttributeNameMapper();
        contextRoot.ifPresent(
                root ->
                        root.accept(
                                new KeyConditionExpressionBaseVisitor<Void>() {
                                    @Override
                                    public Void visitPartitionKeyExpWithExpAttrName(
                                            final com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser
                                                            .PartitionKeyExpWithExpAttrNameContext
                                                    ctx) {
                                        mapper.put(
                                                ctx.EXPRESSION_ATTRIBUTE_NAME().getText(),
                                                NameAwareConverter.newLazyConverter(
                                                        ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(), tableModel));
                                        return null;
                                    }

                                    @Override
                                    public Void visitCompareExpWithExpAttrName(
                                            final com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser
                                                            .CompareExpWithExpAttrNameContext
                                                    ctx) {
                                        mapper.put(
                                                ctx.EXPRESSION_ATTRIBUTE_NAME().getText(),
                                                NameAwareConverter.newLazyConverter(
                                                        ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(), tableModel));
                                        return null;
                                    }

                                    @Override
                                    public Void visitBetweenExpWithExpAttrName(
                                            final com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser
                                                            .BetweenExpWithExpAttrNameContext
                                                    ctx) {
                                        ctx.EXPRESSION_ATTRIBUTE_VALUE()
                                                .stream()
                                                .map(attr -> NameAwareConverter.newLazyConverter(attr.getText(), tableModel))
                                                .forEach(converter -> mapper.put(ctx.EXPRESSION_ATTRIBUTE_NAME().getText(), converter));
                                        return null;
                                    }

                                    @Override
                                    public Void visitBeginsWithExpWithExpAttrName(
                                            final com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser
                                                            .BeginsWithExpWithExpAttrNameContext
                                                    ctx) {
                                        mapper.put(
                                                ctx.EXPRESSION_ATTRIBUTE_NAME().getText(),
                                                NameAwareConverter.newLazyConverter(
                                                        ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(), tableModel));
                                        return null;
                                    }
                                }));
        return mapper;
    }

    private Optional<com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser.StartContext> getContextRoot() {
        return Optional.ofNullable(keyCondition)
                .filter(s -> !Strings.isNullOrEmpty(s))
                .map(
                        expr ->
                                new ParserFactory<>(
                                                KeyConditionExpressionLexer.class,
                                                com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser.class)
                                        .create(expr))
                .map(com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser::start);
    }

    private Set<String> newExpressionAttributeNames() {
        final Set<String> names = new HashSet<>();
        contextRoot.ifPresent(
                root ->
                        root.accept(
                                new KeyConditionExpressionBaseVisitor<Void>() {

                                    @Override
                                    public Void visitTerminal(final TerminalNode node) {
                                        if (node.getSymbol().getType()
                                                == com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser
                                                        .EXPRESSION_ATTRIBUTE_NAME) {
                                            names.add(node.getText());
                                        }
                                        return super.visitTerminal(node);
                                    }
                                }));
        return names;
    }
}
