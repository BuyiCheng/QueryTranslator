import re
import json

'''
join type: join , left join
'''
def tl_join(table, joins):
    join_list = []
    for join in joins:
        join_index = join.index('join') + 4
        join_type = join[:join_index]
        temp = join[join_index:].strip()
        join_table = temp.split(' on ')[0].strip()
        on = temp.split(' on ')[1].strip()
        attrs = [i.strip() for i in on.split('=')]
        # print("attrs:",attrs)
        if attrs[0].split('.')[0] == join_table:
            foreignField = attrs[0].split('.')[1]
            localField = table + '.' + attrs[1].split('.')[1]
        else:
            foreignField = attrs[1].split('.')[1]
            localField = table + '.' + attrs[0].split('.')[1]

        if join_type == 'inner join':
            isPreserveNull = False
        elif join_type == 'left join':
            isPreserveNull = True
        else:
            return None
        # if len(joins) > 1:
        #     localField = table + '.' + localField
        join_list.append({'$lookup': {'from': join_table, 'localField': localField, 'foreignField':foreignField, 'as': join_table}})
        join_list.append({'$unwind': {"path": '$' + join_table, "preserveNullAndEmptyArrays": isPreserveNull}})
        join_list.append({'$project': {table+'._id': 0, join_table+'._id': 0}})
    return join_list


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

def parse_in(item, isNot, isHaving):
    if item.find(' not ') != -1:
        key, value = item.split(' not in ')
        key = key.strip()
        if isHaving:
            key = key.replace('.','_')
        value = [handle_value_type(i.strip()) for i in value[1:-1].split(',')]
        if isNot:
            return key, {key.strip:{'$in':value}}
        else:
            return key, {key:{'$not':{'$in':value}}}
    else:
        key, value = item.split(' in ')
        key = key.strip()
        if isHaving:
            key = key.replace('.','_')
        value = [handle_value_type(i.strip()) for i in value[1:-1].split(',')]
        if isNot:
            return key, {key:{'$not':{'$in':value}}}
        else:
            return key, {key:{'$in':value}}

#listing.price between 4000 and 5000
def parse_between(item, isNot, isHaving):
    key, value = item.split(' between ')
    key = key.strip()
    if isHaving:
        key = key.replace('.','_')
    v1,v2 = [handle_value_type(i.strip()) for i in value.split(' and ')]
    if isNot:
        return key, {'$or':[{key:{'$lt':v1}},{key:{'$gt':v2}}]}
    else:
        return key, {'$and':[{key:{'$gte':v1}},{key:{'$lte':v2}}]}

def parse_item(item,isHaving):
    
    comparison_dict = {'=': '$eq', '!=': '$ne', '>': '$gt', '>=': '$gte', '<': '$lt', '<=': '$lte'}
    # recover between and
    item = item.replace('*between*', 'between').replace('*and*','and')
    isNot = False
    # print("item:",item)
    if item.startswith('not '):
        isNot = True
        item = item[len('not '):]
    if item.find(' between ') != -1:
        return parse_between(item, isNot, isHaving)
    elif item.find(' in ') != -1:
        return parse_in(item, isNot, isHaving)
    else:
        comparison_operators = list(comparison_dict.keys())
        compare = re.findall('|'.join(comparison_operators),item)[0]
        key, value = [handle_value_type(i.strip()) for i in item.split(compare)]
        if isHaving:
            key = key.replace('.','_')
        if isNot:
            return key, {key:{'$not':{comparison_dict[compare]:value}}}
        else:
            return key, {key:{comparison_dict[compare]:value}}


def handle_and(item, isHaving):
    result = {}
    and_list = item.split(' and ')
    key_list = []
    if len(and_list) == 1:
        
        key, new_item = parse_item(and_list[0], isHaving)
        result = new_item
        key_list.append(key)
    else:
        result['$and'] = []
        for item in and_list:
            key, new_item = parse_item(item, isHaving)
            result['$and'].append(new_item)
            key_list.append(key)
    return key_list, result

# where:listing.room_type = 'Private room' or listing.neighboourhood = 'Hollywood' 
# and not listing.price between 4000 and 5000
def tl_where(where, isHaving=False):
    if where == '':
        return [],None
    # repalce between and with *between*
    while where.find(' between ') != -1:
        between_index = where.find(' between ') + 1
        and_index = between_index + where[between_index:].find(' and ') + 1
        where = where[:between_index]+'*between*'+where[between_index+len('between'):and_index]+ '*and*'+where[and_index+len('and'):]

    or_list = where.split(" or ")
    # listing.room_type = 'Private room'
    # listing.neighboourhood = 'Hollywood' and not listing.price between 4000 and 5000
    keys = []
    if len(or_list) == 0:
        return keys, None
    elif len(or_list) == 1:
        result = {"$match":{}}
        keys, result['$match'] = handle_and(or_list[0], isHaving)
    else:
        result = {"$match":{"$or":[]}}
        for item in or_list:
            key_list, new_item = handle_and(item, isHaving)
            result['$match']['$or'].append(new_item)
            for key in key_list:
                keys.append(key)
    return list(set(keys)), result

def tl_group(group, having, order, projection):
    group_dict = {'$group': {'_id': {}}}
    for g in group:
        group_dict['$group']['_id'][g.replace('.', '_')] = '$' + g
    attrs = set()
    having_attrs, _ = tl_having(having)
    for h in having_attrs:
        if h.find('(') != -1:
            # attrs.add(h.split(':')[1].split(' ')[0])
            attrs.add(h)
    for o in order:
        if o.find('(') != -1:
            attrs.add(o.split(' ')[0])
    for p in projection:
        if p.find('(') != -1:
            attrs.add(p)
    for a in attrs:
        func, attr = a.split('(')[0], a.split('(')[1][:-1]
        if func == 'count':
            value = { '$sum': 1 }
        else:
            value = {'$' + func: '$' + attr}
        group_dict['$group'][a.replace('.', '_')] = value
    return group_dict if len(group) != 0 else None

def tl_having(having, isHaving=True):
    return tl_where(having, isHaving)
# def tl_having(having):
#     result = {}
#     having_dict = {'and': [], 'or': [], 'not': []}
#     for item in having:
#         key, value = item.split(':')
#         for i in parse_where_value(value,True):
#             having_dict[key].append(i)
#     for key, value in having_dict.items():
#         if key == 'not':
#             for x in value:
#                 for k, v in x.items():
#                     result.setdefault('$and', []).append({k: {'$not': v}})
#         else:
#            result['$'+key] = value
#     match = {'$match': {}}
#     for key, value in result.items():
#         if len(value) > 0:
#             match['$match'][key] = value
#     return match if len(having) != 0 else None


def tl_order(order, group):
    order_dict = {'$sort': {}}
    for o in order:
        if len(o.split(' ')) <= 1:
            attr = o.split(' ')[0]
            order_type = 'asc'
        else:
            attr, order_type = o.split(' ')
        if attr in group:
            attr = '_id.' + attr.replace('.', '_')
        else:
            if len(group) > 0:
                attr = attr.replace('.', '_')
        if order_type.lower() == 'asc':
            order_dict['$sort'][attr] = 1
        elif order_type.lower() == 'desc':
            order_dict['$sort'][attr] = -1
    return order_dict if len(order) != 0 else None


def tl_projection(attribute, group, joins):
    if '*' in attribute:
        attribute.remove('*')
    projection_dict = None
    if len(group) > 0:
        projection_dict = {'$project': {}}
        if len(attribute) == 0:
            for g in group:
                projection_dict['$project'][g.replace('.', '_')] = '$_id.' + g.replace('.', '_')
        else:
            for a in attribute:
                if a in group:
                    projection_dict['$project'][a.replace('.', '_')] = '$_id.' + a.replace('.', '_')
                else:
                    projection_dict['$project'][a.replace('.', '_')] = 1
        projection_dict['$project']['_id'] = 0
    else:

        if len(attribute) != 0:
            if len(joins) == 0:
                projection_dict = {'$project': {'_id': 0}}
            else:
                projection_dict = {'$project': {}}
            for a in attribute:
                projection_dict['$project'][a] = 1
        else:
            projection_dict = {'$project': {'_id': 0}}
        # print('projection_dict', projection_dict)

    return projection_dict


def tl_limit(limit=0):
    return {'$limit': int(limit)} if limit != '' else None


def tl_offset(offset=0):
    return {'$skip': int(offset)} if offset != '' else None


def translate(sql_dict):
    tables = [sql_dict['table']]
    pipeline = []
    if len(sql_dict['join']) > 0:
        pipeline.append({"$project": {"_id": 0, sql_dict['table']: "$$ROOT"}})
        for j in tl_join(sql_dict['table'], sql_dict['join']):
            # print(j)
            pipeline.append(j)
            if '$lookup' in j.keys():
                tables.append(j['$lookup']['from'])
    _, where = tl_where(sql_dict['where'])
    if where is not None:
        pipeline.append(where)
    if tl_group(sql_dict['group'], sql_dict['having'], sql_dict['order'], sql_dict['projection']) is not None:
        pipeline.append(tl_group(sql_dict['group'], sql_dict['having'], sql_dict['order'], sql_dict['projection']))
    _, having = tl_having(sql_dict['having'])
    if having is not None:
        pipeline.append(having)
    if tl_order(sql_dict['order'], sql_dict['group']) is not None:
        pipeline.append(tl_order(sql_dict['order'], sql_dict['group']))
    if tl_projection(sql_dict['projection'], sql_dict['group'], sql_dict['join']) is not None:
        pipeline.append(tl_projection(sql_dict['projection'], sql_dict['group'], sql_dict['join']))

    if tl_offset(sql_dict['offset']) is not None:
        pipeline.append(tl_offset(sql_dict['offset']))
    if tl_limit(sql_dict['limit']) is not None:
        pipeline.append(tl_limit(sql_dict['limit']))

    # print('db.{}.aggregate('.format(sql_dict['table']) + json.dumps(pipeline) + ')')
    return 'db.{}.aggregate('.format(sql_dict['table']) + json.dumps(pipeline) + ')'