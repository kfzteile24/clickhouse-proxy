# Join EFFicient REfactoring thingY. Jeffrey

class Jeffrey:
    """ Refactors JOIN ON clauses from Tableau-generated queries to include only "AND" expressions.
    The reason is that Tableau tries to tackle NULL values like this:
    JOIN ... `t1` ON ((`t0`.`x` = `t1`.`x`) OR ((`t0`.`x` IS NULL) AND (`t1`.`x` IS NULL)))

    ClickHouse doesn't like "OR" clauses in its joins.
    This class builds a high-level syntax tree out of large queries, identifies JOIN conditions, assumes that no
    values are actually NULL (it's a prerequisite for this to work properly), and simplifies JOIN clauses.
    Ex. `x OR FALSE` can be simplified just as `x`, `FALSE AND FALSE` can be simplified to just FALSE.
    Given the initial conditions that no values are actually NULL, the above JOIN example can thus be replaced to this:

    JOIN ... `t1` ON `t0`.`x` = `t1`.`x`

    Which works on ClickHouse
    """
    

    def replace(self, query):
        self.query = query
        self.uquery = query.upper()
        self.qlen = len(query)
        self.index = 0
        qtree = self.parentheses_tree()
        self.find_and_parse_query(qtree)



    def parentheses_tree(self):
        """ Turns an expression with parentheses into an array of query pieces, and sub-levels go in sub-arrays
        ex. "a (b) c (d(e)(f))" turns into ['a ', ['b'], ' c ', ['d', ['e'], ['f']]]
        """
        start = self.index
        intermediary = self.index
        contents = []
        # Identifiers and texts can easily contain parentheses, so make sure we don't consider them as relevant
        is_in_string = False
        is_in_identifier = False
        while self.index < self.qlen:
            c = self.query[self.index]
            if c == '\'' and not is_in_identifier:
                is_in_string = not is_in_string
                self.index += 1
                continue
            if c == '`' and not is_in_string:
                is_in_identifier = not is_in_identifier
                self.index += 1
                continue
            if not is_in_string and not is_in_identifier:
                if c == '(':
                    contents += [self.query[intermediary:self.index]]
                    self.index += 1
                    contents += [self.parentheses_tree()] # array of arrays, to indicate a deeper level
                    intermediary = self.index # indicate that the next chunk will start here
                    continue
                elif c == ')':
                    contents += [self.query[intermediary:self.index]]
                    self.index += 1
                    break
            self.index += 1
        else: # end of query, add remnants
            contents += [self.query[intermediary:self.index]]
        return contents


    def flatten_tree(self, qtree):
        chunks = []
        for node in qtree:
            if isinstance(node, str):
                chunks.append(node)
            else:
                chunks.append(self.flatten_tree(node))
        return ''.join(chunks)


    def parse_query_single_level(self, qtree):
        qindex = 0
        # flatten only current level
        l1str = ''.join([leaf for leaf in qtree if isinstance(leaf, str)]).upper()

        print('\n----------------------')
        #print(l1str)
        print(self.flatten_tree(qtree))
        print('++++++++++++++++++++++\n')

        qlen = len(l1str)

        # Then try to find an SQL query in this level and trigger the processing JOIN conditions

        is_in_string = False
        is_in_identifier = False
        # Stage - what to expect next.
        # 0 = SELECT
        # 1 = FROM
        # 2 = JOIN / WHERE
        # 3 = ON
        # 4 = JOIN / WHERE / conditional expression
        # 5 = end of query
        stage = 0
        # position where the JOIN ON condition starts
        jstart = 0
        while qindex < qlen:
            if l1str[qindex] == '\'' and not is_in_identifier:
                is_in_string = not is_in_string
                qindex += 1
                continue
            if l1str[qindex] == '`' and not is_in_string:
                is_in_identifier = not is_in_identifier
                qindex += 1
                continue
            if not is_in_string and not is_in_identifier:
                if stage == 0 and l1str[qindex:qindex + 6] == 'SELECT':
                    stage = 1
                    qindex += 6
                    continue
                if stage == 1 and l1str[qindex:qindex + 4] == 'FROM':
                    stage = 2
                    qindex += 4
                    continue
                if (stage == 2 or stage == 4):
                    is_join = False
                    if l1str[qindex:qindex + 4] == 'JOIN':
                        is_join = True
                        new_qindex = qindex + 4
                    elif l1str[qindex:qindex + 10] == 'INNER JOIN':
                        is_join = True
                        new_qindex = qindex + 10
                    elif l1str[qindex:qindex + 9] == 'LEFT JOIN':
                        is_join = True
                        new_qindex = qindex + 9
                    if is_join:
                        if stage == 4:
                            # was a join condition before, so parse it
                            self.simplify_condition(qtree, jstart, qindex)
                        stage = 3
                        qindex = new_qindex
                        continue
                if (stage == 2 or stage == 4) and l1str[qindex:qindex + 4] == 'WHERE':
                    if stage == 4:
                        # was a join condition before, so parse it
                        self.simplify_condition(qtree, jstart, qindex)
                    # Ignore the rest of the query
                    stage = 5
                    qindex = qlen
                    break
                if stage == 3 and l1str[qindex:qindex + 2] == 'ON':
                    # we might expect another JOIN or WHERE, but until then we'll parse the rest
                    stage = 4
                    qindex += 2
                    jstart = qindex
                    continue
            # end: if not is_in_string and not is_in_identifier
            # advance character by 1
            qindex += 1
        else:
            # reached the end of the query. If we were parsing a JOIN condition, finish it
            if stage == 4:
                # was a join condition before, so parse it
                self.simplify_condition(qtree, jstart, qindex)


    def find_and_parse_query(self, qtree):
        # recurse over subqueries
        for i, node in enumerate(qtree):
            if isinstance(node, list):
                qtree[i] = self.find_and_parse_query(node)
        # parse single level
        try:
            self.parse_query_single_level(qtree)
        except:
            print(qtree)
            raise


    def simplify_condition(self, qtree, start, end):
        return
        index = 0
        # A list of nodes or node pieces.
        # If it's a dict, then it's composed of "node" - index within the qtree, "start" / "end" - string start and
        # end for partial node integration
        # If it's an int, then it's the index of the entire node within the qtree
        chunks = []
        condition_started = False
        for i, node in enumerate(qtree):
            if isinstance(node, str):
                node_start, node_end = (index, index + len(node))
                # case 1. node is completely to the left of the condition
                if node_end < start:
                    condition_started = False
                    index = node_end
                    continue
                # case 2. node contains start of the condition
                if node_start <= start <= node_end:
                    chunk = {
                        "node": i,
                        "start": start - node_start
                    }
                    condition_started = True
                    # case 2.1 node also contains end of the condition
                    if node_start <= end <= node_end:
                        condition_started = False
                        chunk["end"] = node_end - end
                        chunks.append(chunk)
                        # it's done. exit
                        break
                    # case 2.2 node doesn't contain end of condition
                    chunk["end"] = len(node)
                    index = node_end
                    chunks.append(chunk)
                    continue

                # case 3. node contains only end of the condition
                if node_start <= end <= node_end:
                    condition_started = False
                    chunk = {
                        "node": i,
                        "start": 0,
                        "end": node_end - end
                    }
                    chunks.append(chunk)
                    # it's done. exit
                    break
                
                # case 4. node is completely to the right of the condition
                if node_start > end:
                    break
            # Not a string node, but an array of sub-nodes. Take them all if condition started because start and end
            # are defined in the current flat level, and the sub-nodes are considered zero-length. Start and End
            # have to be on this level
            elif condition_started:
                chunks.append(i)

        new_expression = self.evaluate(qtree, chunks)
        replaced = False
        for chunk in chunks:
            if isinstance(chunk, int):
                index = chunk
            else:
                index = chunk['node']

            if not replaced:
                # replace just one with the new combined expression
                qtree[index] = new_expression
                replaced = True
            else:
                # eliminate the rest
                qtree[index] = ''




if __name__=='__main__':
    with open('sample.sql', 'r') as fp:
        query = fp.read()

    jeff = Jeffrey()

    jeff.replace(query)
