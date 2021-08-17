#include "src/binder/include/expression_binder.h"

#include "src/binder/include/expression/existential_subquery_expression.h"
#include "src/binder/include/expression/literal_expression.h"
#include "src/binder/include/expression/property_expression.h"
#include "src/binder/include/expression/rel_expression.h"
#include "src/binder/include/query_binder.h"
#include "src/common/include/date.h"
#include "src/common/include/types.h"

namespace graphflow {
namespace binder {

shared_ptr<Expression> ExpressionBinder::bindExpression(const ParsedExpression& parsedExpression) {
    shared_ptr<Expression> expression;
    auto expressionType = parsedExpression.type;
    if (isExpressionBoolConnection(expressionType)) {
        expression = isExpressionUnary(expressionType) ?
                         bindUnaryBooleanExpression(parsedExpression) :
                         bindBinaryBooleanExpression(parsedExpression);
    } else if (isExpressionComparison(expressionType)) {
        expression = bindComparisonExpression(parsedExpression);
    } else if (isExpressionArithmetic(expressionType)) {
        expression = isExpressionUnary(expressionType) ?
                         bindUnaryArithmeticExpression(parsedExpression) :
                         bindBinaryArithmeticExpression(parsedExpression);
    } else if (isExpressionStringOperator(expressionType)) {
        expression = bindStringOperatorExpression(parsedExpression);
    } else if (CSV_LINE_EXTRACT == expressionType) {
        expression = bindCSVLineExtractExpression(parsedExpression);
    } else if (isExpressionNullComparison(expressionType)) {
        expression = bindNullComparisonOperatorExpression(parsedExpression);
    } else if (isExpressionLiteral(expressionType)) {
        expression = bindLiteralExpression(parsedExpression);
    } else if (FUNCTION == expressionType) {
        expression = bindFunctionExpression(parsedExpression);
    } else if (PROPERTY == expressionType) {
        expression = bindPropertyExpression(parsedExpression);
    } else if (VARIABLE == expressionType) {
        expression = bindVariableExpression(parsedExpression);
    } else if (EXISTENTIAL_SUBQUERY == expressionType) {
        expression = bindExistentialSubqueryExpression(parsedExpression);
    } else if (!expression) {
        throw invalid_argument(
            "Bind " + expressionTypeToString(expressionType) + " expression is not implemented.");
    }
    expression->rawExpression = parsedExpression.rawExpression;
    if (!parsedExpression.alias.empty()) {
        auto aliasExpression =
            make_shared<Expression>(ALIAS, expression->dataType, move(expression));
        aliasExpression->rawExpression = parsedExpression.alias;
        return aliasExpression;
    }
    return expression;
}

shared_ptr<Expression> ExpressionBinder::bindBinaryBooleanExpression(
    const ParsedExpression& parsedExpression) {
    auto left = bindExpression(*parsedExpression.children[0]);
    auto right = bindExpression(*parsedExpression.children[1]);
    return make_shared<Expression>(parsedExpression.type, BOOL,
        validateAsBoolAndCastIfNecessary(left), validateAsBoolAndCastIfNecessary(right));
}

shared_ptr<Expression> ExpressionBinder::bindUnaryBooleanExpression(
    const ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.children.at(0));
    return make_shared<Expression>(NOT, BOOL, validateAsBoolAndCastIfNecessary(child));
}

shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedLeft = *parsedExpression.children.at(0);
    auto& parsedRight = *parsedExpression.children.at(1);
    if (parsedLeft.type == LITERAL_NULL || parsedRight.type == LITERAL_NULL) {
        if (parsedExpression.type == EQUALS || parsedExpression.type == NOT_EQUALS) {
            return make_shared<LiteralExpression>(LITERAL_BOOLEAN, BOOL, Literal(FALSE));
        } else {
            return make_shared<LiteralExpression>(LITERAL_BOOLEAN, BOOL, Literal(NULL_BOOL));
        }
    }
    auto left = bindExpression(parsedLeft);
    auto right = bindExpression(parsedRight);
    if (left->dataType == UNSTRUCTURED || right->dataType == UNSTRUCTURED) {
        return make_shared<Expression>(parsedExpression.type, BOOL, move(left), move(right));
    }
    auto isLNumerical = TypeUtils::isNumericalType(left->dataType);
    auto isRNumerical = TypeUtils::isNumericalType(right->dataType);
    if ((isLNumerical && !isRNumerical) || (!isLNumerical && isRNumerical) ||
        (!isLNumerical && !isRNumerical && left->dataType != right->dataType)) {
        return make_shared<LiteralExpression>(LITERAL_BOOLEAN, BOOL, Literal(NULL_BOOL));
    }
    return make_shared<Expression>(NODE_ID == left->dataType ?
                                       comparisonToIDComparison(parsedExpression.type) :
                                       parsedExpression.type,
        BOOL, move(left), move(right));
}

shared_ptr<Expression> ExpressionBinder::bindBinaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(*parsedExpression.children.at(0));
    auto right = bindExpression(*parsedExpression.children.at(1));
    if (left->dataType == UNSTRUCTURED || right->dataType == UNSTRUCTURED) {
        return make_shared<Expression>(
            parsedExpression.type, UNSTRUCTURED, move(left), move(right));
    }
    if (parsedExpression.type == ADD) {
        if (left->dataType == STRING || right->dataType == STRING) {
            if (left->dataType != STRING) {
                if (isExpressionLiteral(left->expressionType)) {
                    static_pointer_cast<LiteralExpression>(left)->castToString();
                } else {
                    left = make_shared<Expression>(CAST_TO_STRING, STRING, move(left));
                }
            }
            if (right->dataType != STRING) {
                if (isExpressionLiteral(right->expressionType)) {
                    static_pointer_cast<LiteralExpression>(right)->castToString();
                } else {
                    right = make_shared<Expression>(CAST_TO_STRING, STRING, move(right));
                }
            }
            return make_shared<Expression>(parsedExpression.type, STRING, move(left), move(right));
        }
    }
    validateNumericalType(*left);
    validateNumericalType(*right);
    DataType resultType;
    if (left->dataType == DOUBLE || right->dataType == DOUBLE) {
        resultType = DOUBLE;
    } else {
        resultType = INT64;
    }
    return make_shared<Expression>(parsedExpression.type, resultType, move(left), move(right));
}

shared_ptr<Expression> ExpressionBinder::bindUnaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto child = bindExpression(*parsedExpression.children.at(0));
    validateNumericalType(*child);
    return make_shared<Expression>(parsedExpression.type, child->dataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindStringOperatorExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(*parsedExpression.children.at(0));
    validateExpectedType(*left, STRING);
    auto right = bindExpression(*parsedExpression.children.at(1));
    validateExpectedType(*right, STRING);
    return make_shared<Expression>(parsedExpression.type, BOOL, move(left), move(right));
}

shared_ptr<Expression> ExpressionBinder::bindCSVLineExtractExpression(
    const ParsedExpression& parsedExpression) {
    auto idxExpression = bindExpression(*parsedExpression.children[1]);
    if (LITERAL_INT != idxExpression->expressionType) {
        throw invalid_argument("LIST EXTRACT INDEX must be LITERAL_INT.");
    }
    auto csvVariableName = parsedExpression.children[0]->text;
    auto csvLineExtractExpressionName =
        csvVariableName + "[" + ((LiteralExpression&)*idxExpression).literal.toString() + "]";
    if (!context.containsVariable(csvLineExtractExpressionName)) {
        throw invalid_argument("Variable " + csvVariableName + " not defined or idx out of bound.");
    }
    return context.getVariable(csvLineExtractExpressionName);
}

shared_ptr<Expression> ExpressionBinder::bindNullComparisonOperatorExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto childExpression = bindExpression(*parsedExpression.children.at(0));
    return make_shared<Expression>(parsedExpression.type, BOOL, move(childExpression));
}

shared_ptr<Expression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto propertyName = parsedExpression.text;
    auto child = bindExpression(*parsedExpression.children.at(0));
    if (NODE == child->dataType) {
        auto node = static_pointer_cast<NodeExpression>(child);
        if (catalog.containNodeProperty(node->label, propertyName)) {
            auto& property = catalog.getNodeProperty(node->label, propertyName);
            return make_shared<PropertyExpression>(
                property.dataType, propertyName, property.id, move(child));
        } else {
            throw invalid_argument("Node " + node->getExternalName() + " does not have property " +
                                   propertyName + ".");
        }
    } else if (REL == child->dataType) {
        auto rel = static_pointer_cast<RelExpression>(child);
        if (catalog.containRelProperty(rel->label, propertyName)) {
            auto& property = catalog.getRelProperty(rel->label, propertyName);
            return make_shared<PropertyExpression>(
                property.dataType, propertyName, property.id, move(child));
        } else {
            throw invalid_argument(
                "Rel " + rel->getExternalName() + " does not have property " + propertyName + ".");
        }
    }
    throw invalid_argument("Type mismatch: expect NODE or REL, but " + child->getExternalName() +
                           " was " + TypeUtils::dataTypeToString(child->dataType) + ".");
}

shared_ptr<Expression> ExpressionBinder::bindFunctionExpression(
    const ParsedExpression& parsedExpression) {
    auto functionName = parsedExpression.text;
    StringUtils::toUpper(functionName);
    if (functionName == ABS_FUNC_NAME) {
        return bindAbsFunctionExpression(parsedExpression);
    } else if (functionName == COUNT_STAR_FUNC_NAME) {
        return bindCountStarFunctionExpression(parsedExpression);
    } else if (functionName == ID_FUNC_NAME) {
        return bindIDFunctionExpression(parsedExpression);
    } else if (functionName == DATE_FUNC_NAME) {
        return bindDateFunctionExpression(parsedExpression);
    }
    throw invalid_argument(functionName + " function does not exist.");
}

shared_ptr<Expression> ExpressionBinder::bindAbsFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.children[0]);
    if (child->dataType == UNSTRUCTURED) {
        return make_shared<Expression>(ABS_FUNC, child->dataType, move(child));
    }
    validateNumericalType(*child);
    return make_shared<Expression>(ABS_FUNC, child->dataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindCountStarFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 0);
    return make_shared<Expression>(COUNT_STAR_FUNC, INT64);
}

shared_ptr<Expression> ExpressionBinder::bindIDFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.children[0]);
    validateExpectedType(*child, NODE);
    return make_shared<PropertyExpression>(
        NODE_ID, INTERNAL_ID_SUFFIX, UINT32_MAX /* property key for internal id */, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindDateFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.children[0]);
    validateExpectedType(*child, STRING);
    // Currently only support bind date(string) as date literal
    GF_ASSERT(child->expressionType == LITERAL_STRING);
    auto dateInString = static_pointer_cast<LiteralExpression>(child)->literal.strVal;
    return make_shared<LiteralExpression>(LITERAL_DATE, DATE,
        Literal(Date::FromCString(dateInString.c_str(), dateInString.length())));
}

shared_ptr<Expression> ExpressionBinder::bindLiteralExpression(
    const ParsedExpression& parsedExpression) {
    auto literalVal = parsedExpression.text;
    auto literalType = parsedExpression.type;
    switch (literalType) {
    case LITERAL_INT:
        return make_shared<LiteralExpression>(
            LITERAL_INT, INT64, Literal(TypeUtils::convertToInt64(literalVal.c_str())));
    case LITERAL_DOUBLE:
        return make_shared<LiteralExpression>(
            LITERAL_DOUBLE, DOUBLE, Literal(TypeUtils::convertToDouble(literalVal.c_str())));
    case LITERAL_BOOLEAN:
        return make_shared<LiteralExpression>(
            LITERAL_BOOLEAN, BOOL, Literal(TypeUtils::convertToBoolean(literalVal.c_str())));
    case LITERAL_STRING:
        return make_shared<LiteralExpression>(
            LITERAL_STRING, STRING, Literal(literalVal.substr(1, literalVal.size() - 2)));
    default:
        throw invalid_argument("Literal " + parsedExpression.rawExpression + "is not defined.");
    }
}

shared_ptr<Expression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) {
    auto variableName = parsedExpression.text;
    if (context.containsVariable(variableName)) {
        return context.getVariable(variableName);
    }
    throw invalid_argument("Variable " + parsedExpression.rawExpression + " not defined.");
}

shared_ptr<Expression> ExpressionBinder::bindExistentialSubqueryExpression(
    const ParsedExpression& parsedExpression) {
    // Create new QueryBinder by copying the context
    auto newQueryBinder = QueryBinder(catalog, context);
    auto boundSingleQuery = newQueryBinder.bind(*parsedExpression.subquery);
    return make_shared<ExistentialSubqueryExpression>(move(boundSingleQuery));
}

void ExpressionBinder::validateNoNullLiteralChildren(const ParsedExpression& parsedExpression) {
    for (auto& child : parsedExpression.children) {
        if (LITERAL_NULL == child->type) {
            throw invalid_argument(
                "Expression " + child->rawExpression + " cannot have null literal children.");
        }
    }
}

void ExpressionBinder::validateNumberOfChildren(
    const ParsedExpression& parsedExpression, uint32_t expectedNumChildren) {
    GF_ASSERT(parsedExpression.type == FUNCTION);
    if (parsedExpression.children.size() != expectedNumChildren) {
        throw invalid_argument("Expected " + to_string(expectedNumChildren) + " parameters for " +
                               parsedExpression.text + " function but get " +
                               to_string(parsedExpression.children.size()) + ".");
    }
}

void ExpressionBinder::validateExpectedType(const Expression& expression, DataType expectedType) {
    auto dataType = expression.dataType;
    if (expectedType != dataType) {
        throw invalid_argument(expression.getExternalName() + " has data type " +
                               TypeUtils::dataTypeToString(dataType) + ". " +
                               TypeUtils::dataTypeToString(expectedType) + " was expected.");
    }
}

void ExpressionBinder::validateNumericalType(const Expression& expression) {
    auto dataType = expression.dataType;
    if (!TypeUtils::isNumericalType(dataType)) {
        throw invalid_argument(expression.getExternalName() + " has data type " +
                               TypeUtils::dataTypeToString(dataType) +
                               ". A numerical data type was expected.");
    }
}

shared_ptr<Expression> ExpressionBinder::validateAsBoolAndCastIfNecessary(
    shared_ptr<Expression> expression) {
    if (expression->dataType != UNSTRUCTURED) {
        validateExpectedType(*expression, BOOL);
        return expression;
    }
    return make_shared<Expression>(CAST_UNSTRUCTURED_VECTOR_TO_BOOL_VECTOR, BOOL, move(expression));
}

} // namespace binder
} // namespace graphflow
