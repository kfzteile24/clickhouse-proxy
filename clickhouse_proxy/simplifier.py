import sqlparse

from typing import List, Tuple


class Operand:
    """ Wrapper for any operand made for str(). If it's a token from sqlparse, then use the `normalized` attribute
    """

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return f'<Operand "{str(self)}">'

    def __str__(self):
        if hasattr(self.value, 'normalized'):
            return self.value.normalized
        return str(self.value)


class Operator:
    """ Operator (operator) class that takes a list of arguments, and tries to evaluate it. This is the base class.
    The evaluated expression will either be a simplified form (scalar) or the expression itself if it can't be
    simplified. Simplification is done using the str() conversion.
    """

    def __init__(self):
        self.operands = []

    @property
    def precedence(self):
        raise Exception(f"precedence not implemented for type {type(self).__name__}")

    def __repr__(self):
        return f'<Operator "{type(self).__name__}">'

    @property
    def can_be_unary(self):
        return False

    @property
    def can_be_binary(self):
        return True

    def has_enough_operands(self):
        return (self.can_be_unary and len(self.operands) == 1) or (len(self.operands) == 2)


class Is(Operator):
    def __str__(self):
        o0 = str(self.operands[0])
        o1 = str(self.operands[1])
        identifier = None
        null_stuff = None
        if isinstance(self.operands[0].value, sqlparse.sql.Identifier):
            identifier = o0
            null_stuff = o1
        elif isinstance(self.operands[1].value, sqlparse.sql.Identifier):
            identifier = o1
            null_stuff = o0

        # Very naive evaluation that says that we have no null values in ClickHouse. Otherwise it just won't work
        if identifier is not None:
            if null_stuff == 'NOT NULL':
                return 'TRUE'
            else:
                return 'FALSE'

        return f'({o0}) IS ({o1})'

    @property
    def precedence(self):
        return 4


class In(Operator):
    def __str__(self):
        o0 = str(self.operands[0])
        o1 = str(self.operands[1])
        return f'({o0}) IN ({o1})'

    @property
    def precedence(self):
        return 7


class Or(Operator):
    def __str__(self):
        o0 = str(self.operands[0])
        o1 = str(self.operands[1])
        if o0 == 'FALSE' and o1 == 'FALSE':
            return 'FALSE'
        if o0 == 'TRUE' or o1 == 'TRUE':
            return 'TRUE'
        if o0 == 'FALSE':
            return o1
        if o1 == 'FALSE':
            return o0
        return f'({o0}) OR ({o1})'

    @property
    def precedence(self):
        return 7


class And(Operator):
    def __str__(self):
        o0 = str(self.operands[0])
        o1 = str(self.operands[1])
        if o0 == 'FALSE' or o1 == 'FALSE':
            return 'FALSE'
        if o0 == 'TRUE' and o1 == 'TRUE':
            return 'TRUE'
        if o0 == 'TRUE':
            return o1
        if o1 == 'TRUE':
            return o0
        return f'({o0}) AND ({o1})'

    @property
    def precedence(self):
        return 6


class Not(Operator):
    def has_enough_operands(self):
        return len(self.operands) == 1

    def __str__(self):
        o = str(self.operands[0])
        if o == 'TRUE':
            return 'FALSE'
        elif o == 'FALSE':
            return 'TRUE'
        return f'NOT ({o})'

    @property
    def precedence(self):
        return 5


class IndexedOperator:
    """Acts as a read+write tuple of operator index (order in the list of operations), and operator itself
    """
    def __init__(self, index: int, operator: Operator):
        self.index: index = index
        self.operator: Operator = operator


def simplify_sql(sql):
    expr = sqlparse.parse(sql)[0]
    return simplify_tokenized(expr)


def simplify_tokenized(statement: sqlparse.sql.TokenList):
    """ Parses the token list / tree tries to identify operators and operands, and bind them together.
    Recurses over sub-expressions. The result from an expression is used in the next expression.
    :returns: str
    """
    return simplify_tokens(statement.tokens)


def simplify_tokens(tokens: List[sqlparse.sql.Token]):
    """ Parses the token list / tree tries to identify operators and operands, and bind them together.
    Recurses over sub-expressions. The result from an expression is used in the next expression.
    :returns: str
    """
    expression_elements = []
    for token in tokens:
        if isinstance(token, sqlparse.sql.Parenthesis):
            # recurse over parenthesis
            expression_elements.append(Operand(simplify_tokenized(token)))
        elif isinstance(token, sqlparse.sql.Token) and token.is_keyword:
            normtoken = token.normalized
            if normtoken == 'IS':
                expression_elements.append(Is())
            elif normtoken == 'IN':
                expression_elements.append(In())
            elif normtoken == 'OR':
                expression_elements.append(Or())
            elif normtoken == 'AND':
                expression_elements.append(And())
            elif normtoken == 'NOT NULL' or normtoken == 'NULL':
                expression_elements.append(Operand(token))
            elif normtoken == 'TRUE' or normtoken == 'FALSE':
                expression_elements.append(Operand(token))

        elif isinstance(token, sqlparse.sql.Identifier) \
                or isinstance(token, sqlparse.sql.Comparison):
            expression_elements.append(Operand(token))

    elements = evaluate(expression_elements)
    return str(elements)


def evaluate(expression_elements: list):
    """Evaluate an expression expressed as a list of operators and operands, based on operator precedence
    :returns: list
    """
    # We need to remember the order of the operators in the original expression even after we sort them according
    # to precedence
    index_preserved_elements = enumerate(expression_elements)
    operators_only: List[IndexedOperator] = [
        IndexedOperator(o[0], o[1]) for o in index_preserved_elements if isinstance(o[1], Operator)
    ]
    # sort according to precedence first, then according to natural order
    sorted_operators: List[IndexedOperator] = sorted(operators_only, key=lambda e: (e.operator.precedence, e.index))
    for iop in sorted_operators:
        index, op = iop.index, iop.operator
        if index == 0 or not op.can_be_binary:
            # is definitely unary
            operand_ids = [index - 1]
        else:
            operand_ids = [index - 1, index + 1]
        # if overflow is detected, then we have a wrong expression
        op.operands = [expression_elements[i] for i in operand_ids]
        result = Operand(str(op))
        # Update operator with its actual value
        expression_elements[index] = result
        # remove operands that have been processed into the above result
        # take care of all indexes as well
        # TODO: This would probably be prettier with a double-linked list
        for opindex in operand_ids[::-1]:
            del expression_elements[opindex]
            # Also update all bigger indexes in sorted_operators since those changed
            for sop in sorted_operators:
                if sop.index > opindex:
                    sop.index -= 1
    return expression_elements[0]


if __name__ == '__main__':
    try:
        ex = simplify_sql('(`asd`.`test` IS NULL) or false')
        assert ex == 'FALSE'
        ex = simplify_sql('(`asd`.`test` IS NOT NULL) or false')
        assert ex == 'TRUE'
        ex = simplify_sql('(`asd`.`test`=`as`.`test` OR ((`asd`.`test` IS NULL) AND (`as`.`test` IS NULL)))')
        assert ex == '`asd`.`test`=`as`.`test`'
        ex = simplify_sql('true or true and false')
        assert ex == 'TRUE', f"Expected TRUE, got {ex}"
    except AssertionError as err:
        if err.args:
            print(err.args[0], f"\n./{__file__}:{err.__traceback__.tb_lineno}")
        else:
            raise
