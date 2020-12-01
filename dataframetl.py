import pandas as pd
import re

def getTableDict(s_list, table, join):
    # table_dict = {}
    tables = [table]
    for j in join:
        tables.append(j.split('join ')[1].split(' ')[0])
    for t in tables:
        if len(join) == 0:
            s_list.append("df = pd.read_csv('{0}.csv')".format(t))
        else:
            s_list.append("{0} = pd.read_csv('{0}.csv')".format(t))
    for t in tables:
        columns = {}
        
        # for c in t.columns:
        #     columns[c] = t + '_' + c
           
        if len(join) > 0:
            s_list.append("columns_map = {}")
            s_list.append("for c in {}.columns:".format(t))
            s_list.append("\tcolumns_map[c]='{}_'+c".format(t) )
            s_list.append("{}.rename(columns=columns_map, inplace=True)".format(t, str(columns)))
    return s_list


def parse_join(s_list, table, joins):

    for join in joins:
        join_table = join.split('join ')[1].split(' ')[0]
        right,left = join.split(' on ')[1].split(' = ')
        join_type = 'inner'
        if join.startswith('left'):
            join_type = 'left'
        elif join.startswith('right'):
            join_type = 'right'
        s_list.append("df = {}.merge({},left_on='{}',right_on='{}',how='{}')".format(table,join_table,left.replace('.','_'),right.replace('.','_'), join_type))
    return s_list


def handle_value_type(value):
    if value[0] in ['"',"'"] and value[-1] in ['"',"'"]:
        return value[1:-1]
    else:
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            pass
        try:
            import unicodedata
            return unicodedata.numeric(value)
        except (TypeError, ValueError):
            pass
    return value
def parse_between(item, isNot, isHaving):
    key, value = item.split(' between ')
    key = key.strip()
    if isHaving:
        key = key.replace('.','_')
    v1,v2 = [handle_value_type(i.strip()) for i in value.split(' and ')]
    n = '~' if isNot else ''
    return key, "{3}(df['{0}'] >= {1})&{3}(df['{0}'] <= {2}".format(key, v1, v2, n)
def parse_in(item, isNot, isHaving):
    if item.find(' not ') != -1:
        key, value = item.split(' not in ')
        isNot = not isNot
    else:
        key, value = item.split(' in ')
    key = key.strip()
    if isHaving:
        key = key.replace('.','_')
    value = [handle_value_type(i.strip()) for i in value[1:-1].split(',')]
    n = '~' if isNot else ''
    return key, "{}(df['{}'].isin({}))".format(n, key, str(value))
 
def parse_condition(item, isHaving):
    item = item.replace('*between*', 'between').replace('*and*','and')
    if item.startswith('not '):
        isNot = True
        item = item[len('not '):]
    else:
        isNot = False
    n = '~' if isNot else ''
    if item.find(' between ') != -1:
        key, result = parse_between(item, isNot, isHaving)
    elif item.find(' in ') != -1:
        key, result = parse_in(item, isNot, isHaving)
    else:
        comparison_operators = ['=','!=','<','<=','>','>=']
        compare = re.findall('|'.join(comparison_operators), item)[0]
        key,value = [i.strip() for i in item.split(compare)]
        if isHaving:
            key = key.replace('.','_')
        result = "{}(df['{}'] {} {})".format(n, key, compare, value)
    return key, result

def parse_where(s_list, where, isHaving=False):
    if where == '':
        return [],s_list
    # repalce between and with *between*
    while where.find(' between ') != -1:
        between_index = where.find(' between ') + 1
        and_index = between_index + where[between_index:].find(' and ') + 1
        where = where[:between_index]+'*between*'+where[between_index+len('between'):and_index]+ '*and*'+where[and_index+len('and'):]
    where_list = re.split(' and | or ', where)
    logic_list = re.findall(' and | or ', where)
    logic_map = {'and':'&','or':'|'}
    keys, result = [], ''
    if len(where_list) >=1:
        key, result = parse_condition(where_list[0],isHaving)
        keys.append(key)
    for i, l in enumerate(logic_list):
        key, r = parse_condition(where_list[i+1],isHaving)
        result = result + logic_map[l.strip()] + r
    s_list.append('df = df['+result+']')
    return keys,s_list
    # logic_con_map = {}
    # for w in where:
    #     logic, reverse = w.split(':')[0], False
    #     if logic == 'not':
    #         reverse = True
    #     logic_con_map[getCon(w.split(':')[1], type_dict, reverse)] = logic

    # con = ''
    # for k, v in logic_con_map.items():
    #     if con == '':
    #         con += k
    #     else:
    #         if v == 'or':
    #             con = con + '|' + k
    #         else:
    #             con = con + '&' + k

    # if con == '':
    #     return s_list, df
    # else:
    #     s_list.append("df = df[{}]".format(con))
    #     #         print(con)
    #     return s_list, df[eval(con)]


def parse_group(s_list, group, projection, order, having):
    if len(group) == 0:
        return s_list
    group = [g.replace('.', '_') for g in group]
    s_list.append("df = df.groupby({}, sort=False)".format(str(group)))
    # df = df.groupby(group, sort=False)
    agg_dict = {}
    attributes = projection[:]
    having_attrs, _ = parse_where([],having)
    for h in having_attrs:
        if h.find('(') != -1:
            # attributes.append(h.split(':')[1].split(' ')[0])
            attributes.append(h)
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
        # df = df.count().reset_index()[group]
        s_list.append("df = df.count().reset_index()[{}]".format(group))
    else:
        s_list.append("df = df.agg({})".format(str(agg_dict)))
        s_list.append("df.columns = df.columns.droplevel(0)")
        # df = df.agg(agg_dict)
        # df.columns = df.columns.droplevel(0)
    return s_list


def parse_order(s_list, order):
    if len(order) == 0:
        return s_list
    columns, ascendings = [], []
    for o in order:
        columns.append(o.split(' ')[0].replace('.', '_'))
        if len(o.split(' ')) == 1:
            ascendings.append(True)
        else:
            if o.split(' ')[1].lower() == 'asc':
                ascendings.append(True)
            else:
                ascendings.append(False)
    s_list.append("df = df.sort_values({},ascending={})".format(str(columns), str(ascendings)))
    # df = df.sort_values(columns, ascending=ascendings)
    return s_list


def parse_limit_offset(s_list, offset, limit):
    if limit == '':
        limit = 0
    else:
        limit = int(limit)
    if offset == '':
        offset = 0
    else:
        offset = int(offset)
    # if df.size < offset:  # review.drop(review.index)
    #     s = "df = df.drop(df.index)"
        # df = df.drop(df.index)
    #     else:
    if limit == 0:
        return s_list

    # df = df.iloc[offset:offset + limit]
    s = "df = df.iloc[{}:{}]".format(offset, offset + limit)
    s_list.append(s)
    return s_list


def parse_projection(s_list, attributes, group):
    if len(attributes) == 0 and len(group) == 0:
        pass
    else:
        # df = df.reset_index()
        s_list.append("df = df.reset_index()")
        if len(attributes) == 0:
            group = [g.replace('.', '_') for g in group]
            # df = df[group]
            s_list.append("df = df[{}]".format(group))
        else:
            attributes = [a.replace('.', '_') for a in attributes]
            # df = df[attributes]
            s_list.append("df = df[{}]".format(attributes))
    return s_list


def getResult(sql_dict):
    if len(sql_dict['projection']) == 1 and sql_dict['projection'][0] == '*':
        sql_dict['projection'] = []
    s_list = []
    s_list = getTableDict(s_list, sql_dict['table'], sql_dict['join'])

    s_list = parse_join(s_list, sql_dict['table'], sql_dict['join'])
    _, s_list = parse_where(s_list, sql_dict['where'])
    s_list = parse_group(s_list, sql_dict['group'], sql_dict['projection'], sql_dict['order'],
                             sql_dict['having'])

    _, s_list= parse_where(s_list, sql_dict['having'], True)

    s_list = parse_order(s_list, sql_dict['order'])

    s_list = parse_limit_offset(s_list, sql_dict['offset'], sql_dict['limit'])
    print(s_list)
    s_list = parse_projection(s_list, sql_dict['projection'], sql_dict['group'])
    return s_list

def translate(sql_dict):
    s_list = getResult(sql_dict)
    return s_list


