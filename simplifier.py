import sqlparse

class Operand:
    """ Wrapper for any operand made for str(). If it's a token from sqlparse, then use the `normalized` attribute
    """
    def __init__(self, value):
        self.value = value


    def __str__(self):
        if hasattr(self.value, 'normalized'):
            return self.value.normalized
        return str(self.value)


class Operation:
    """ Operation (operator) class that takes a list of arguments, and tries to evaluate it. This is the base class.
    The evaluated expression will either be a simplified form (scalar) or the expression itself if it can't be
    simplified. Simplification is done using the str() conversion.
    """
    def __init__(self):
        self.operands = []
        # Is either binary or unknown
        self.is_unary = False


    def hasEnoughOperands(self, operands):
        return (self.is_unary and len(perands) == 1) or (len(operands) == 2)


class Is(Operation):
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


class In(Operation):
    def __str__(self):
        o0 = str(self.operands[0])
        o1 = str(self.operands[1])
        return f'({o0}) IN ({o1})'


class Or(Operation):
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


class And(Operation):
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


class Not(Operation):
    def hasEnoughOperands(self):
        return len(self.operands) == 1

    def __str__(self):
        o = str(self.operands[0])
        if o == 'TRUE':
            return 'FALSE'
        elif o == 'FALSE':
            return 'TRUE'
        return f'NOT ({o})'


class Expression:
    def __init__(self, expression):
        self.expr = sqlparse.parse(expression)[0]
        self.simplified = self.parse(self.expr)


    def parse(self, statement):
        """ Parses the token list / tree tries to identify operators and operands, and bind them together.
        Recurses over sub-expressions. The result from an expression is used in the next expression.
        """
        operands = []
        tokens = statement.tokens
        tindex = 0
        tlen = len(tokens)
        operation = None
        while tindex < tlen:
            if isinstance(tokens[tindex], sqlparse.sql.Parenthesis):
                # recurse over parenthesis
                operands.append(Operand(self.parse(tokens[tindex])))
            elif isinstance(tokens[tindex], sqlparse.sql.Token) and tokens[tindex].is_keyword:
                if tokens[tindex].normalized == 'IS':
                    operation = Is()
                elif tokens[tindex].normalized == 'IN':
                    operation = In()
                elif tokens[tindex].normalized == 'OR':
                    operation = Or()
                elif tokens[tindex].normalized == 'AND':
                    operation = And()
                elif tokens[tindex].normalized == 'NOT NULL' or tokens[tindex].normalized == 'NULL':
                    operands.append(Operand(tokens[tindex]))
                elif tokens[tindex].normalized == 'TRUE' or tokens[tindex].normalized == 'FALSE':
                    operands.append(Operand(tokens[tindex]))

            elif isinstance(tokens[tindex], sqlparse.sql.Identifier) \
                or isinstance(tokens[tindex], sqlparse.sql.Comparison):
                operands.append(Operand(tokens[tindex]))
                if operation is not None:
                    operation.is_unary = True

            if operation is not None and operation.hasEnoughOperands(operands):
                operation.operands = operands
                operands = [Operand(operation)]
                operation = None

            tindex += 1
        return str(operands[0])


if __name__ == '__main__':
    try:
        ex = Expression('(`asd`.`test` IS NULL) or false')
        assert ex.simplified == 'FALSE'
        ex = Expression('(`asd`.`test` IS NOT NULL) or false')
        assert ex.simplified == 'TRUE'
        ex = Expression('(`asd`.`test`=`as`.`test` OR ((`asd`.`test` IS NULL) AND (`as`.`test` IS NULL)))')
        assert ex.simplified == '`asd`.`test`=`as`.`test`'
        ex = Expression('true or true and false')
        assert ex.simplified == 'TRUE', f"Expected TRUE, got {ex.simplified}"
    except AssertionError as err:
        print(err.args[0], f"\n./{__file__}:{err.__traceback__.tb_lineno}")
