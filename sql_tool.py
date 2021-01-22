import re

def validate(sql):
    sql = sql.strip()
    error_msg = ''
    if not sql.startswith('select '):
        error_msg = 'Do not have select'
        return False, error_msg
    from_index = sql.find(' from ')
    
    if from_index == -1 or sql.endswith('from'):
        error_msg = 'Do not have table'
        return False, error_msg
    projection = sql[sql.find('select')+len('select'):from_index].strip()
    if projection == '':
        error_msg = 'Your sql do not have projection'
        return False, error_msg
    if sql.find(' join') != -1:
        if sql.find(' join')+len(' join') >= len(sql):
            error_msg = "lack of joined table"
            return False, error_msg
        join_type = sql[:sql.find(' join')].strip().split(' ')[-1]
        from_index = sql.find(' from ')
        table = sql[from_index+len('from'):].strip().split(' ')[0]
        type_list = [ 'left', 'right']
        if join_type != table and join_type in type_list and sql.find(' on ') == -1:
            error_msg = "{} join mush have 'on' clause".format('join_type') 
            return False, error_msg
    if sql.find(' having ')!=-1 and sql.find('group by') == -1:
        error_msg = "having must after grouping"
        return False, error_msg
    return True, 'success'

def getReservedWordsAndOrder(sql):
    default_list = ['select ',' from ', ' join ', ' where ', ' group by ', ' having ', ' order by ', ' limit ', ' offset ']
    word_list = re.findall('|'.join(default_list), sql)
    words_ascending_pair = []
    for word in word_list:
        pair = (word, sql.find(word))
        words_ascending_pair.append(pair)
        sql = sql.replace(word,'*'*len(word),1)
    return words_ascending_pair    
    # words_order_dict = {}
    # for w in default_list:
    #     if sql.find(w) != -1:
    #         words_order_dict[w] = sql.find(w)
    # words_ascending_dict = sorted(words_order_dict.items(), key=lambda x: x[1])
    # return words_ascending_dict


# def getProjection(sql):
#     select_index = sql.find('select ')
#     from_index = sql.find(' from ')
#     projection = sql[select_index+len('select'):from_index].strip()
#     return projection.split(','), from_index + len(' from ')

# def getTable(index_after_from, sql):
#     table = sql[index_after_from:].strip().split(' ')[0]
#     return table, index_after_from + len(table)

# def getJoin(index_after_table, table, sql):
#     index_after_join = index_after_table
#     isJoin, join_table, join_type, join_on = False, None, None, None
#     if sql.find(' join ') != -1:
#         isJoin = True
#         type_list = ['inner', 'left', 'right']
#         join_type = sql[:sql.find(' join ')].strip().split(' ')[-1]
#         join_table = sql[sql.find(' join ')+len(' join '):].strip().split(' ')[0]
#         index_after_join = sql.find(' join ')+len(' join ') + len(join_table)
#         sql_dict['join_table'] = join_table
#         if join_type in type_list: 
#             join_attrs = sql[sql.find(' on ')+len(' on '):].split('=')
#             a1 = join_attrs[0].strip()
#             a2 = join_attrs[1].strip().split(' ')[0].strip()
#             join_on = [a1, a2]
#             index_after_join = sql.find(a2)+len(a2)
#         elif join_type == table:
#                 join_type = 'inner'
#         # sql_dict['join_type'] = join_type 
#     return isJoin, join_table, join_type, join_on, index_after_join

# def getSelection(index_after_join, sql):
#     index_after_where = index_after_join
#     # if sql.find(' where ') != -1:

def getProjection(start_index, end_index, sql):
    projection = sql[start_index:end_index].strip()
    projection = [i.strip() for i in projection.split(',')]
    return projection

def getTable(start_index, end_index, sql):
    return sql[start_index:end_index].strip().split(' ')[0]

def getSelection(start_index, end_index, sql):
    return sql[start_index: end_index].strip()
# def getSelection(start_index, end_index, sql):
#     # print(start_index, end_index)
#     if end_index <= start_index:
#         return None
#     where_list = []
#     where = sql[start_index:end_index].strip()
#     while where.find(' between ') != -1:
#         between_index = where.find(' between ') + 1
#         where = where[:between_index] + '*******' + where[between_index+len(' between ')-2:]
#         and_index = where[where.find(' between ')+len(' between '):].strip().find(' and ')
#         and_index = where.find(' between ')+len(' between ') + and_index + 1
#         where = where[:and_index] + '&&&' + where[and_index+3:]
#     logic_list = re.findall(' and | or ', where)
#     print(where, logic_list)
#     where = where.replace('*******','between').replace('&&&','and')
#     for i, logic in enumerate(logic_list):
#         if i == 0:
#             where_list.append('and:'+where[:where.find(logic)])
#         if i+1 < len(logic_list):
#             where = where[where.find(logic)+len(logic):]
#             condition = where[:where.find(logic_list[i+1])].strip()
#             where_list.append(logic.strip()+':'+condition)
#         else:
#             condition = where[where.find(logic)+len(logic):].strip()
#             where_list.append(logic.strip()+':'+condition)
#     if len(logic_list) == 0:
#         where_list.append('and:'+where)
#     return where_list

def getGroupBy(start_index, end_index, sql):
    group = sql[start_index: end_index].strip()
    group = [i.strip() for i in group.split(',')]
    return group

def getOrderBy(start_index, end_index, sql):
    order = sql[start_index: end_index].strip()
    order = [i.strip() for i in order.split(',')]
    return order

def init_sql_dict():
    sql_dict = {}
    sql_dict['projection'] = []
    sql_dict['table'] = ''
    sql_dict['join'] = []
    sql_dict['where'] = ''
    sql_dict['group'] = []
    sql_dict['having'] = ''
    sql_dict['order'] = []
    sql_dict['limit'] = ''
    sql_dict['offset'] = ''
    return sql_dict

def parse_sql(sql):
    if not validate(sql):
        return None
    sql = sql.strip()
    sql_dict = init_sql_dict()
    words_ascending_pair = getReservedWordsAndOrder(sql)
    print(words_ascending_pair)
    # dict_keys = words_ascending_dict.keys()
    for i, word_pair in enumerate(words_ascending_pair):
        word, index = word_pair
        index = index+len(word)
        if i == len(words_ascending_pair) - 1:
            next_index = len(sql)
        else:
            next_index = words_ascending_pair[i+1][1]
        if word == 'select ':
            sql_dict['projection'] = getProjection(index, next_index, sql)
        elif word == ' from ':
            sql_dict['table'] = getTable(index, next_index, sql)
        elif word == ' join ':
            join_type = sql[:sql.find(' join ')].split(' ')[-1]
            if join_type == sql_dict['table']:
                join_type = 'inner'
            join = join_type + ' ' + sql[sql.find(' join '):next_index].strip()
            if 'join' in sql_dict.keys():
                sql_dict['join'].append(join)
            else:
                sql_dict['join'] = [join]
        elif word == ' where ':
            sql_dict['where'] = getSelection(index, next_index, sql)
        elif word == ' group by ':
            sql_dict['group'] = getGroupBy(index, next_index, sql)
        elif word == ' having ':
            sql_dict['having'] = getSelection(index, next_index, sql)
        elif word == ' order by ':
            sql_dict['order'] = getOrderBy(index, next_index, sql)
        elif word == ' limit ':
            sql_dict['limit'] = sql[index: next_index].strip()
        elif word == ' offset ':
            sql_dict['offset'] = sql[index: next_index].strip()    
# default_list = ['select ',' from ', ' join ', ' where ', ' group by ', ' having ', ' order by ', ' limit ', ' offset ']
    


    return sql_dict


if __name__ == '__main__':
    # sql = 'select * from listing left join host on listing.host_id = host.id where listing.price > 200 or list.neighbourhood in (Hollywood, Chinatown)'
    sql = "select listing.room_type,avg(listing.price) from host join listing on listing.host_id = host.id where listing.neighbourhood in ('Hollywood','Chinatown') or listing.price between 1000 and 1100 group by listing.room_type having avg(listing.price) > 100 order by avg(listing.price) ASC limit 10"
    sql_dict = parse_sql(sql)
    print(sql_dict)