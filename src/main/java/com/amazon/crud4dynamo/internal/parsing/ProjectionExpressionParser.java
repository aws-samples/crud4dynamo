package com.amazon.crud4dynamo.internal.parsing;

import com.amazon.crud4dynamo.ddbparser.ParserFactory;
import com.amazon.crud4dynamo.ddbparser.ProjectionExpressionBaseVisitor;
import com.amazon.crud4dynamo.ddbparser.ProjectionExpressionLexer;
import com.google.common.base.Strings;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * Projection Expression Parser
 *
 * <p>Retrieve a set of expression attribute names.
 */
public class ProjectionExpressionParser implements ExpressionParser {
    private final String projectionExpression;
    private final Optional<com.amazon.crud4dynamo.ddbparser.ProjectionExpressionParser.StartContext> contextRoot;
    @Getter
    private final AttributeNameMapper attributeNameMapper = new AttributeNameMapper();
    @Getter
    private final AttributeValueMapper attributeValueMapper = new AttributeValueMapper();
    @Getter
    private final Set<String> expressionAttributeNames;

    public ProjectionExpressionParser(final String projectionExpression) {
        this.projectionExpression = projectionExpression;
        contextRoot = getContextRoot();
        expressionAttributeNames = newExpressionAttributeNames();
    }

    private Set<String> newExpressionAttributeNames() {
        final Set<String> names = new HashSet<>();
        contextRoot.ifPresent(
                root ->
                        root.accept(
                                new ProjectionExpressionBaseVisitor<Void>() {
                                    @Override
                                    public Void visitTerminal(final TerminalNode node) {
                                        if (isExpressionAttributeName(node)) {
                                            names.add(node.getText());
                                        }
                                        return super.visitTerminal(node);
                                    }

                                    private boolean isExpressionAttributeName(final TerminalNode node) {
                                        return node.getSymbol().getType()
                                                == com.amazon.crud4dynamo.ddbparser.ProjectionExpressionParser.EXPRESSION_ATTRIBUTE_NAME;
                                    }
                                }));
        return names;
    }

    private Optional<com.amazon.crud4dynamo.ddbparser.ProjectionExpressionParser.StartContext> getContextRoot() {
        return Optional.ofNullable(projectionExpression)
                .filter(s -> !Strings.isNullOrEmpty(s))
                .map(
                        s ->
                                new ParserFactory<>(
                                                ProjectionExpressionLexer.class,
                                                com.amazon.crud4dynamo.ddbparser.ProjectionExpressionParser.class)
                                        .create(s))
                .map(com.amazon.crud4dynamo.ddbparser.ProjectionExpressionParser::start);
    }
}
