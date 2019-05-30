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

import com.amazon.crud4dynamo.ddbparser.UpdateExpressionBaseVisitor;
import com.amazon.crud4dynamo.ddbparser.UpdateExpressionParser.AddActionContext;
import com.amazon.crud4dynamo.ddbparser.UpdateExpressionParser.DeleteActionContext;
import com.amazon.crud4dynamo.ddbparser.UpdateExpressionParser.OperandContext;
import com.amazon.crud4dynamo.ddbparser.UpdateExpressionParser.PathContext;
import com.amazon.crud4dynamo.ddbparser.UpdateExpressionParser.SetActionContext;
import com.amazon.crud4dynamo.ddbparser.UpdateExpressionParser.StartContext;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

public class UpdateExpressionParser implements ExpressionParser {

    private final String updateExpression;
    private final DynamoDBMapperTableModel tableModel;
    private final Optional<StartContext> contextRoot;
    @Getter private final AttributeNameMapper attributeNameMapper;
    @Getter private final AttributeValueMapper attributeValueMapper;
    @Getter private final Set<String> expressionAttributeNames;

    public UpdateExpressionParser(final String updateExpression, final DynamoDBMapperTableModel tableModel) {
        this.updateExpression = updateExpression;
        this.tableModel = tableModel;
        contextRoot = ParseTreeHelper.getRootOfUpdateExpr(updateExpression);
        attributeNameMapper = newNameMapper();
        attributeValueMapper = newValueMapper();
        expressionAttributeNames = newExpressionAttributeNames();
    }

    private AttributeNameMapper newNameMapper() {
        final AttributeNameMapper mapper = new AttributeNameMapper();
        contextRoot.ifPresent(
                root ->
                        root.accept(
                                new UpdateExpressionBaseVisitor<Void>() {
                                    /* Visit value tree only when the left hand path is an expression attribute name. */
                                    @Override
                                    public Void visitSetAction(final SetActionContext ctx) {
                                        Optional.ofNullable(ctx.path().EXPRESSION_ATTRIBUTE_NAME())
                                                .map(ParseTree::getText)
                                                .ifPresent(name -> ctx.value().accept(newExpressionAttributeValueVisitor(name)));
                                        return null;
                                    }

                                    private UpdateExpressionBaseVisitor<Void> newExpressionAttributeValueVisitor(final String expAttrName) {
                                        return new UpdateExpressionBaseVisitor<Void>() {

                                            @Override
                                            public Void visitOperand(final OperandContext ctx) {
                                                Optional.ofNullable(ctx.EXPRESSION_ATTRIBUTE_VALUE())
                                                        .map(ParseTree::getText)
                                                        .ifPresent(
                                                                expAttrValue ->
                                                                        mapper.put(
                                                                                expAttrName,
                                                                                NameAwareConverter.newLazyConverter(
                                                                                        expAttrValue, tableModel)));
                                                return super.visitOperand(ctx);
                                            }
                                        };
                                    }

                                    @Override
                                    public Void visitAddAction(final AddActionContext ctx) {
                                        Optional.ofNullable(ctx.path().EXPRESSION_ATTRIBUTE_NAME())
                                                .map(ParseTree::getText)
                                                .ifPresent(
                                                        exprAttrName ->
                                                                mapper.put(
                                                                        exprAttrName,
                                                                        NameAwareConverter.newLazyConverter(
                                                                                ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(), tableModel)));
                                        return null;
                                    }

                                    @Override
                                    public Void visitDeleteAction(final DeleteActionContext ctx) {
                                        Optional.ofNullable(ctx.path().EXPRESSION_ATTRIBUTE_NAME())
                                                .map(ParseTree::getText)
                                                .ifPresent(
                                                        exprAttrName ->
                                                                mapper.put(
                                                                        exprAttrName,
                                                                        NameAwareConverter.newLazyConverter(
                                                                                ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(), tableModel)));
                                        return null;
                                    }
                                }));
        return mapper;
    }

    private AttributeValueMapper newValueMapper() {
        final AttributeValueMapper mapper = new AttributeValueMapper();
        contextRoot.ifPresent(
                root ->
                        root.accept(
                                new UpdateExpressionBaseVisitor<Void>() {
                                    @Override
                                    public Void visitSetAction(final SetActionContext setActionCtx) {
                                        if (isExpressionAttributeName(setActionCtx.path())) {
                                            return null;
                                        }
                                        setActionCtx.value().accept(newExpressionAttributeValueVisitor(setActionCtx.path()));
                                        return null;
                                    }

                                    private UpdateExpressionBaseVisitor<Void> newExpressionAttributeValueVisitor(final PathContext path) {
                                        return new UpdateExpressionBaseVisitor<Void>() {
                                            @Override
                                            public Void visitOperand(final OperandContext ctx) {
                                                final Optional<String> exprValue =
                                                        Optional.ofNullable(ctx.EXPRESSION_ATTRIBUTE_VALUE()).map(ParseTree::getText);
                                                if (!exprValue.isPresent()) {
                                                    return super.visitOperand(ctx);
                                                }
                                                if (path.nestedPath() != null) {
                                                    mapper.put(
                                                            exprValue.get(),
                                                            obj ->
                                                                    new ArgumentTypeBasedConverter(path.nestedPath().getText())
                                                                            .convert(obj));
                                                } else {
                                                    /**
                                                     * If it is not a nested path it must be a ATTRIBUTE_NAME because it is checked before
                                                     * invoking this function.
                                                     */
                                                    mapper.put(exprValue.get(), tableModel.field(path.ATTRIBUTE_NAME().getText())::convert);
                                                }
                                                return super.visitOperand(ctx);
                                            }
                                        };
                                    }

                                    @Override
                                    public Void visitAddAction(final AddActionContext ctx) {
                                        if (isExpressionAttributeName(ctx.path())) {
                                            return null;
                                        }
                                        if (ctx.path().ATTRIBUTE_NAME() != null) {
                                            mapper.put(
                                                    ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                    tableModel.field(ctx.path().ATTRIBUTE_NAME().getText())::convert);
                                        } else {
                                            mapper.put(
                                                    ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                    obj -> new ArgumentTypeBasedConverter(ctx.path().nestedPath().getText()).convert(obj));
                                        }
                                        return null;
                                    }

                                    @Override
                                    public Void visitDeleteAction(final DeleteActionContext ctx) {
                                        if (isExpressionAttributeName(ctx.path())) {
                                            return null;
                                        }
                                        if (ctx.path().ATTRIBUTE_NAME() != null) {
                                            mapper.put(
                                                    ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                    tableModel.field(ctx.path().ATTRIBUTE_NAME().getText())::convert);
                                        } else {
                                            mapper.put(
                                                    ctx.EXPRESSION_ATTRIBUTE_VALUE().getText(),
                                                    obj -> new ArgumentTypeBasedConverter(ctx.path().nestedPath().getText()).convert(obj));
                                        }
                                        return null;
                                    }

                                    private boolean isExpressionAttributeName(final PathContext pathContext) {
                                        return pathContext.EXPRESSION_ATTRIBUTE_NAME() != null;
                                    }
                                }));
        return mapper;
    }

    private Set<String> newExpressionAttributeNames() {
        final Set<String> names = new HashSet<>();
        contextRoot.ifPresent(
                ctx ->
                        ctx.accept(
                                new UpdateExpressionBaseVisitor<Void>() {
                                    @Override
                                    public Void visitTerminal(final TerminalNode node) {
                                        if (node.getSymbol().getType()
                                                == com.amazon.crud4dynamo.ddbparser.UpdateExpressionParser.EXPRESSION_ATTRIBUTE_NAME) {
                                            names.add(node.getText());
                                        }
                                        return super.visitTerminal(node);
                                    }
                                }));
        return names;
    }
}
