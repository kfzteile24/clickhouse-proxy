from clickhouse_proxy import simplifier
import sqlparse


class FSM:
    """ A Finite State Machine that parses ODBC query and outputs a query that Clickhouse could understand, the way
    it was supposed to be
    """
    # Some say that FSM is actually a reference to his Holy Noodliness

    CONVERT_FUNCTIONS = {
        'SQL_BIGINT': 'toInt64',
        'SQL_INTEGER': 'toInt32',
        'SQL_FLOAT': 'toFloat32',
        'SQL_DOUBLE': 'toFloat64'
    }

    def __init__(self):
        # Just mention class members here
        self.query: str = ''
        self.index: int = 0
        self.qlen: int = 0

    def replace_odbc_tokens(self, odbcquery: str) -> str:
        """Replaces ODBC tokens like {fn }, {d } to appropriate functions and literals
        :param odbcquery: ODBC compatibe query string
        :return: SQL-compatible query string
        """
        self.query = odbcquery
        # Fragments of replaced syntax will be appended here
        self.index = 0
        self.qlen = len(self.query)
        fragments = self.free_scan()
        return ''.join(fragments)

    def replace_paranoid_joins(self, querystr: str) -> str:
        """Replaces NULL-safe joins with NULL-unsafe joins, that are Clickhouse compatible
        :param querystr: Original query string
        :return: Modified query string with "strong" joins that ignores NULL logic
        """
        parsed = sqlparse.parse(querystr)[0]
        self.optimise_joins(parsed)
        return str(parsed)

    def free_scan(self):
        """ Goes through regular query text, and triggers handlers for {} ODBC literals
        Only this function deals with {} delimiters. Other functions should be oblivious to it
        :return: list of query slices and replacement strings that can be concatenated
        """
        start = self.index
        fragments = []
        while self.index < self.qlen:
            if self.query[self.index] == '{':
                fragments += [self.query[start:self.index]]
                if self.query[self.index + 1:self.index + 4] == 'fn ':
                    # `{fn ` advances position to position after the space after `fn`
                    self.index += 4
                    fragments += self.process_function()
                elif self.query[self.index + 1:self.index + 3] == 'd ':
                    # `{d ` advances position to position after the space after `d`
                    self.index += 3
                    fragments += self.process_date()
                start = self.index
            elif self.query[self.index] == '}':
                self.index += 1
                # Add the fragments between {}
                fragments += [self.query[start:self.index - 1]]
                break
            self.index += 1
        else:
            # add whatever's left at the end
            fragments += [self.query[start:self.index]]
        return fragments

    def process_date(self):
        """Processes a date literal by advancing the cursor to the next closing curly brackets and ignoring it
        :return: list of query slices and replacement strings that can be concatenated
        """
        # the date is a simple string. Just grab it through free scan
        return self.free_scan()

    def process_function(self):
        """ Processes a function name and converts it to a Clickhouse compatible function
        :return: list of query slices and replacement strings that can be concatenated
        """
        function_start = self.index
        function_name = None
        # Find function name
        while self.index < self.qlen:
            if function_name is None:
                if self.query[self.index] == '(':
                    function_name = self.query[function_start:self.index].strip()
                    break
            self.index += 1

        inner_contents = self.free_scan()
        if function_name.upper() == 'CONVERT':
            last_fragment = inner_contents.pop()
            lfindex = len(last_fragment) - 1
            # Find closing parentheses
            while lfindex >= 0:
                if last_fragment[lfindex] == ')':
                    lfindex -= 1
                    break
                lfindex -= 1
            # Find the last character of the type (E in the example) in ex CONVERT(x, TYPE  )
            while lfindex >= 0:
                if last_fragment[lfindex] != ' ':
                    break
                lfindex -= 1
            convert_type_end = lfindex
            # Find the character before the TYPE name in CONVERT(x, TYPE  )
            while lfindex >= 0:
                if last_fragment[lfindex] == ',':
                    break
                lfindex -= 1
            type_name = last_fragment[lfindex + 1:convert_type_end + 1].strip().upper()
            inner_contents += [last_fragment[:lfindex] + ')']
            if type_name not in FSM.CONVERT_FUNCTIONS:
                raise Exception(f"Fragment doesn't contain a supported ODBC type name at the end. Needed for CONVERT "
                                f"function:\n\n{last_fragment}")
            function_name = FSM.CONVERT_FUNCTIONS[type_name]
        return [function_name] + inner_contents

    @staticmethod
    def replace_join_condition(tokenized_query, start, end):
        """Replace complex NULL-safe "weak" join condition with simple, NULL-unsafe, "strong" clickhouse-compatible
        join condition
        :param tokenized_query: sqlparse query fragment with tokens
        :param start: first token to replace
        :param end: replace until this token (exclusive)
        :return: None
        """
        simplified_str = simplifier.simplify_tokens(tokenized_query.tokens[start:end])
        del tokenized_query.tokens[start:end]
        simplified_tokens = sqlparse.parse(' ' + simplified_str + ' ')[0]
        for st in simplified_tokens.tokens[::-1]:
            st.parent = tokenized_query
            tokenized_query.tokens.insert(start, st)

    def optimise_joins(self, tokenized_query):
        """ Finds JOIN conditions and simplifies them in tokenized_query
        :param tokenized_query: sqlparse query fragment with tokens
        :return: None
        """
        needs_identifier = False
        needs_on_clause = False
        needs_on_conditions = False
        join_conditions_to_replace = []
        on_conditions_start = 0
        for i, t in enumerate(tokenized_query):
            if t.is_group:
                # Recurse over group
                self.optimise_joins(t)
            if needs_on_conditions:
                if t.is_keyword and t.normalized in {'JOIN', 'INNER JOIN', 'WHERE', 'GROUP BY'}:
                    needs_on_conditions = False
                    # Simply record that this has to be replaced. Replacement will happen in reverse order
                    # to preserve indexes
                    join_conditions_to_replace.append((on_conditions_start, i))
            if t.is_keyword:
                if not needs_identifier and t.normalized in {'JOIN', 'INNER JOIN', 'FROM'}:
                    needs_identifier = True
                    if t.normalized in {'JOIN', 'INNER JOIN'}:
                        needs_on_clause = True
                    continue
                if needs_on_clause and t.is_keyword and t.normalized == 'ON':
                    needs_on_conditions = True
                    on_conditions_start = i + 1
                    needs_on_clause = False
                    continue
            if needs_identifier and isinstance(t, sqlparse.sql.Identifier):
                needs_identifier = False
                continue
        # If it's the last part of the query, not followed by other keywords
        if needs_on_conditions:
            join_conditions_to_replace.append((on_conditions_start, len(tokenized_query.tokens)))

        # Process join conditions in reverse order, to preserve indexes
        for start, end in join_conditions_to_replace[::-1]:
            self.replace_join_condition(tokenized_query, start, end)


if __name__ == '__main__':
    with open('../test/sample.sql', 'r') as fp:
        query = fp.read()

    fsm = FSM()

    query = fsm.replace_odbc_tokens(query)
    query = fsm.replace_paranoid_joins(query)
    assert query.find('{fn ') == -1
    assert query.find('{d ') == -1
    # TODO: A proper test for whether a JOIN works on ClickHouse without actually having ClickHouse
    # assert query.find(' OR ') == -1
