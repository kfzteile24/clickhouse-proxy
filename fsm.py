import simplifier

class FSM: # a tribute to His Holy Noodliness
    """ A finite state machine that parses ODBC query and outputs a query that Clickhouse could understand, the way
    it was supposed to be
    """

    CONVERT_FUNCTIONS = {
        'SQL_BIGINT': 'toInt64',
        'SQL_INTEGER': 'toInt32',
        'SQL_FLOAT': 'toFloat32',
        'SQL_DOUBLE': 'toFloat64'
    }


    def replace(self, query: str) -> str:
        self.query = query
        # Fragments of replaced syntax will be appended here
        self.index = 0
        self.qlen = len(self.query)
        fragments = self.free_scan()
        return ''.join(fragments)


    def free_scan(self):
        """ Goes through regular query text, and triggers handlers for {} ODBC literals
        Only this function deals with {} delimiters. Other functions should be oblivious to it
        """
        start = self.index
        fragments = []
        while self.index < self.qlen:
            if self.query[self.index]=='{':
                fragments += [self.query[start:self.index]]
                if self.query[self.index+1:self.index+4] == 'fn ':
                    self.index += 4 # `{fn ` advances position to position after the space after `fn`
                    fragments += self.process_function()
                elif self.query[self.index+1:self.index+3] == 'd ':
                    self.index += 3 # `{d ` advances position to position after the space after `d`
                    fragments += self.process_date()
                start = self.index
            elif self.query[self.index]=='}':
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
        # the date is a simple string. Just grab it through free scan
        return self.free_scan()


    def process_function(self):
        function_start = self.index
        function_name = None
        # Find function name
        while self.index < self.qlen:
            if function_name is None:
                if self.query[self.index]=='(':
                    function_name = self.query[function_start:self.index].strip()
                    break
            self.index += 1

        inner_contents = self.free_scan()
        if function_name.upper()=='CONVERT':
            last_fragment = inner_contents.pop()
            lfindex = len(last_fragment) - 1
            # Find closing parentheses
            while lfindex>=0:
                if last_fragment[lfindex]==')':
                    lfindex -= 1
                    break
                lfindex -= 1
            # Find the last character of the type (E in the example) in ex CONVERT(x, TYPE  )
            while lfindex>=0:
                if last_fragment[lfindex]!=' ':
                    break
                lfindex -= 1
            convert_type_end = lfindex
            # Find the character before the TYPE name in CONVERT(x, TYPE  )
            while lfindex>=0:
                if last_fragment[lfindex]==',':
                    break
                lfindex -= 1
            type_name = last_fragment[lfindex+1:convert_type_end+1].strip().upper()
            inner_contents += [last_fragment[:lfindex] + ')']
            if not type_name in FSM.CONVERT_FUNCTIONS:
                raise Exception(f"Fragment doesn't contain a supported ODBC type name at the end. Needed for CONVERT "
                                f"function:\n\n{last_fragment}")
            function_name = FSM.CONVERT_FUNCTIONS[type_name]
        return [function_name] + inner_contents


import sqlparse


def replace_join_condition(tokenized_query, start, end):
    # TODO: make this pseudocode work
    new_tokens = simplifier.simplify_tokenized(tokenized_query[start:end])


def optimise_joins(tokenized_query):
    needs_identifier    = False
    needs_on_clause     = False
    needs_on_conditions = False
    for i, t in enumerate(tokenized_query):
        if t.is_keyword:
            if not needs_identifier and t.normalized in {'JOIN', 'INNER JOIN', 'FROM'}:
                needs_identifier = True
                if t.normalized in {'JOIN', 'INNER JOIN'}:
                    needs_on_clause = True
                continue
            if needs_on_clause and t.is_keyword == True and t.normalized == 'ON':
                needs_on_conditions = True
                on_conditions_start = i
                needs_on_clause = False
                continue
        if needs_identifier and isinstance(t, sqlparse.sql.Identifier):
            needs_identifier = False
            continue
        if needs_on_conditions:
            if t.is_keyword and t.normalized in {'JOIN', 'INNER JOIN', 'WHERE', 'GROUP BY'}:
                needs_on_conditions = False
                replace_join_condition(tokenized_query, on_conditions_start, i)
                continue
    if needs_on_conditions:
        replace_join_condition(tokenized_query, on_conditions_start, i)


if __name__=='__main__':
    with open('sample.sql', 'r') as fp:
        query = fp.read()

    fsm = FSM()

    query = fsm.replace(query)

    parsed = sqlparse.parse(query)[0]
    optimise_joins(parsed)
