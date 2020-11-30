import pandas as pd
global df
df = None

def getTableDict(s_list, db, table, join):
    table_dict = {}
    tables = [table]

    for j in join:
        tables.append(j.split('join ')[1].split(' ')[0])
    for t in tables:
        # print(url_for('static',filenamestatic',filename='db/{}/{}.csv'.format(db, t)))
        table_dict[t] = pd.read_csv("{}.csv".format(t))
        if len(join) == 0:
            s_list.append("df = pd.read_csv('{0}.csv')".format(t))
        else:
            s_list.append("{0} = pd.read_csv('{0}.csv')".format(t))
    for name, t in table_dict.items():
        columns = {}
        for c in t.columns:
            columns[c] = name + '_' + c
        if len(join) > 0:
            t.rename(columns=columns, inplace=True)
            s_list.append("{}.rename(columns={}, inplace=True)".format(name, str(columns)))
    return s_list, table_dict


def parse_join(df, s_list, table, joins, table_dict):

    for join in joins:
        join_table = join.split('join ')[1].split(' ')[0]
        right,left = join.split(' on ')[1].split(' = ')
        join_type = 'inner'
        if join.startswith('left'):
            join_type = 'left'
        elif join.startswith('right'):
            join_type = 'right'
        df = df.merge(table_dict[join_table], left_on=left.replace('.','_'), right_on=right.replace('.','_'), how=join_type)
        s_list.append("df = {}.merge({},left_on='{}',right_on='{}',how='{}')".format(table,join_table,left.replace('.','_'),right.replace('.','_'), join_type))
    return s_list, df, dict(df.dtypes)


def getCon(clause, type_dict, reverse=False):
    attr = clause.split(' ')[0].replace('.', '_')
    if attr.find('(') == -1:
        new_attr = attr
    else:
        new_attr = attr.split('(')[1][:-1]
    compare = clause.split(' ')[1]
    if compare == 'between':
        val_1 = clause.split(' and ')[0].split(' ')[-1]
        val_2 = clause.split(' and ')[1]
        if type_dict[new_attr] == 'O':
            val_1 = "'" + val_1 + "'"
            val_2 = "'" + val_2 + "'"
        if reverse:
            s = "~(df['{0}'] {1} {3})&(~df['{0}'] {2} {4})".format(attr, '>=', '<=', val_1, val_2)
        else:
            s = "(df['{0}'] {1} {3})&(df['{0}'] {2} {4})".format(attr, '>=', '<=', val_1, val_2)

    elif compare in ['in', 'not']:
        #         print(compare, clause.split('in')[1])
        val_list = clause.split(' in ')[1].split('(')[1][:-1].split('，')
        if type_dict[new_attr] == 'O':
            val_list = [str(i) for i in val_list]
        if (compare != 'in') ^ reverse:
            s = "~(df['{}'].isin({}))".format(attr, str(val_list))
        else:
            s = "(df['{}'].isin({}))".format(attr, str(val_list))
    else:
        value_index = clause.index(clause.split(' ')[2])
        value = clause[value_index:]
        if type_dict[new_attr] == 'O':
            value = "'" + value + "'"
        if compare == '=':
            compare = '=='
        if reverse:
            s = "~(df['{}'] {} {})".format(attr, compare, value)
        else:
            s = "(df['{}'] {} {})".format(attr, compare, value)
    return s


def parse_condition(df, s_list, where, type_dict):
    logic_con_map = {}
    for w in where:
        logic, reverse = w.split(':')[0], False
        if logic == 'not':
            reverse = True
        logic_con_map[getCon(w.split(':')[1], type_dict, reverse)] = logic
    con = ''
    for k, v in logic_con_map.items():
        if con == '':
            con += k
        else:
            if v == 'or':
                con = con + '|' + k
            else:
                con = con + '&' + k

    if con == '':
        return s_list, df
    else:
        s_list.append("df = df[{}]".format(con))
        #         print(con)
        return s_list, df[eval(con)]


def parse_group(df, s_list, group, projection, order, having):
    if len(group) == 0:
        return s_list, df
    group = [g.replace('.', '_') for g in group]
    s_list.append("df = df.groupby({}, sort=False)".format(str(group)))
    df = df.groupby(group, sort=False)
    agg_dict = {}
    attributes = projection[:]
    for h in having:
        if h.find('(') != -1:
            attributes.append(h.split(':')[1].split(' ')[0])
    for o in order:
        if o.find('(') != -1:
            attributes.append(o.split(' ')[0])
    for a in set(attributes):
        a = a.replace('.', '_')
        if a not in s_list and a.find('(') != -1:
            func = a.split('(')[0]
            if func == 'avg':
                func = 'mean'
            attr = a.split('(')[1][:-1]
            agg_dict.setdefault(attr, []).append((a, func))

    if len(agg_dict) == 0:
        df = df.count().reset_index()[group]
        s_list.append("df = df.count().reset_index()[{}]".format(group))
    else:
        s_list.append("df = df.agg({})".format(str(agg_dict)))
        s_list.append("df.columns = df.columns.droplevel(0)")
        df = df.agg(agg_dict)
        df.columns = df.columns.droplevel(0)
    return s_list, df


def parse_order(df, s_list, order):
    if len(order) == 0:
        return s_list, df
    columns, ascendings = [], []
    for o in order:
        columns.append(o.split(' ')[0].replace('.', '_'))
        if o.split(' ')[1].lower() == 'asc':
            ascendings.append(True)
        else:
            ascendings.append(False)
    s_list.append("df = df.sort_values({},ascending={})".format(str(columns), str(ascendings)))
    df = df.sort_values(columns, ascending=ascendings)
    return s_list, df


def parse_limit_offset(df, s_list, offset, limit):
    if limit == '':
        limit = 0
    else:
        limit = int(limit)
    if offset == '':
        offset = 0
    else:
        offset = int(offset)
    if df.size < offset:  # review.drop(review.index)
        s = "df = df.drop(df.index)"
        df = df.drop(df.index)
    #     else:
    if limit == 0:
        return s_list, df

    df = df.iloc[offset:offset + limit]
    s = "df = df.iloc[{}:{}]".format(offset, offset + limit)
    s_list.append(s)
    return s_list, df


def parse_projection(df, s_list, attributes, group):
    if len(attributes) == 0 and len(group) == 0:
        pass
    else:
        df = df.reset_index()
        s_list.append("df = df.reset_index()")
        if len(attributes) == 0:
            group = [g.replace('.', '_') for g in group]
            df = df[group]
            s_list.append("df = df[{}]".format(group))
        else:
            attributes = [a.replace('.', '_') for a in attributes]
            df = df[attributes]
            s_list.append("df = df[{}]".format(attributes))
    return s_list, df


def getResult(sql_dict):
    s_list = []
    s_list, table_dict = getTableDict(s_list, sql_dict['db'], sql_dict['table'], sql_dict['join'])
    df = table_dict[sql_dict['table']]

    s_list, df, type_dict = parse_join(df, s_list, sql_dict['table'], sql_dict['join'], table_dict)
    s_list, df = parse_condition(df, s_list, sql_dict['where'], type_dict)
    s_list, df = parse_group(df, s_list, sql_dict['group'], sql_dict['attribute'], sql_dict['order'],
                             sql_dict['having'])

    s_list, df = parse_condition(df, s_list, sql_dict['having'], type_dict)

    s_list, df = parse_order(df, s_list, sql_dict['order'])

    s_list, df = parse_limit_offset(df, s_list, sql_dict['offset'], sql_dict['limit'])
    s_list, df = parse_projection(df, s_list, sql_dict['attribute'], sql_dict['group'])
    return s_list, df


def translate(sql_dict):
    global df
    s_list, df = getResult(sql_dict)
    return s_list


def query(sql_dict):
    global df
    if df is None:
        _, df = getResult(sql_dict)
    return df.to_html()
