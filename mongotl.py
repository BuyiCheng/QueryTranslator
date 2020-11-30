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
        print("attrs:",attrs)
        if attrs[0].split('.')[0] == join_table:
            foreignField = attrs[0].split('.')[1]
            localField = table + '.' + attrs[1].split('.')[1]
        else:
            foreignField = attrs[1].split('.')[1]
            localField = table + '.' + attrs[0].split('.')[1]
            

                
        # join_table, _, foreignField, _, localField = join[join_index:].strip().split(' ')
#         print(join_table,foreignField,localField)
        # localField = localField.split('.')[1]
        # foreignField = foreignField.split('.')[1]
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


def parse_where_value(value, ishaving=False):
    # value price between 100 and 300
    value_list = []
    compare_dict = {'=': '$eq', '!=': '$ne', '>': '$gt', '>=': '$gte', '<': '$lt', '<=': '$lte', 'in': '$in',
                    'not in': '$nin'}  # gt,gte,lt,lte,eq,ne (in,nin, all)
    attr, compare = value.split(' ')[:2]

    if attr.find('(') != -1:
        attr_new = attr.split('(')[1][:-1]
    else:
        attr_new = attr
    if compare == 'between':
        between_index = value.index('between') + len('between')
        between_1, between_2 = [i.strip() for i in value[between_index:].split('and')]
        if between_1.startswith("'") and between_1.endswith("'") or between_1.startswith('"') and between_1.endswith('"'):
            between_1,between_2 = between_1[1:-1],between_2[1:-1]
        else:
            if between_1.find('.') !=-1 or between_2.find('.') != -1:
                between_1,between_2 = float(between_1), float(between_2)
            else:
                between_1,between_2 = int(between_1), int(between_2)
        if ishaving:
            attr = attr.replace('.', '_')
        value_list.append({attr: {compare_dict['>=']: between_1}})
        value_list.append({attr: {compare_dict['<=']: between_2}})
    elif compare == 'in' or compare == 'not':  # not -> not in
        if compare == 'not':
            compare = 'not in'
        in_index = value.index(' in ') + len(' in ')
        in_list = value[in_index:].strip()[1:-1].split(',')
        print(in_list)
        if len(in_list) > 0 and in_list[0][0] in ['"',"'"] and in_list[0][-1] in ['"',"'"]:
            in_list = [i[1:-1] for i in in_list]
        else:
            if in_list[0].find('.') != -1:
                in_list = [float(i) for i in in_list]
            else:
                in_list =[int(i) for i in in_list]
        if ishaving:
            attr = attr.replace('.', '_')
        value_list.append({attr: {compare_dict[compare]: in_list}})
    else:
        attr_value = value.split(compare)[-1].strip()
        if attr_value[0] in ['"',"'"] and attr_value[-1] in ['"',"'"]:
            attr_value = attr_value[1:-1]
        elif attr_value.find('.') != -1:
            attr_value = float(attr_value)
        else:
            attr_value = int(attr_value)
        if ishaving:
            attr = attr.replace('.', '_')
        value_list.append({attr: {compare_dict[compare]: attr_value}})
    return value_list


def tl_where(where):
    where_dict = {}
    or_list = []
    temp_list = []
    for item in where:
        if len(temp_list) == 0:
            temp_list.append(item)
        else:
            logic = item.split(':')[0]
            if logic == 'or':
                or_list.append(temp_list)
                temp_list = []
                temp_list.append(item)
            else:
                temp_list.append(item)
    if len(temp_list) > 0:
        or_list.append(temp_list)
    if len(or_list) > 1:
        result = {'$or':[]}
        for i, item in enumerate(or_list):
            result['$or'].append({'$and': []})
            for s in item:

                for j in parse_where_value(s.split(':')[1]):
                    if s.split(':')[0] == 'not':
                        j = {'$not':j}
                    result['$or'][i]['$and'].append(j)
    elif len(or_list) == 1:
        result = {'$and':[]}
        for s in or_list[0]:
            for j in parse_where_value(s.split(':')[1]):
                if s.split(':')[0] == 'not':
                    for k,v in j.items():
                        result['$and'].append({k:{'$not': v}})
                else:
                    result['$and'].append(j)

    else:
        result = None
    print('result:',result if result is not None else 'None')
    return {'$match': result} if len(where) != 0 else None


def tl_group(group, having, order, projection):
    group_dict = {'$group': {'_id': {}}}
    for g in group:
        group_dict['$group']['_id'][g.replace('.', '_')] = '$' + g
    attrs = set()
    for h in having:
        if h.find('(') != -1:
            attrs.add(h.split(':')[1].split(' ')[0])
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


def tl_having(having):
    result = {}
    having_dict = {'and': [], 'or': [], 'not': []}
    for item in having:
        key, value = item.split(':')
        for i in parse_where_value(value,True):
            having_dict[key].append(i)
    for key, value in having_dict.items():
        if key == 'not':
            for x in value:
                for k, v in x.items():
                    result.setdefault('$and', []).append({k: {'$not': v}})
        else:
           result['$'+key] = value
    match = {'$match': {}}
    for key, value in result.items():
        if len(value) > 0:
            match['$match'][key] = value
    return match if len(having) != 0 else None


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
        print('projection_dict', projection_dict)

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
            print(j)
            pipeline.append(j)
            if '$lookup' in j.keys():
                tables.append(j['$lookup']['from'])
    if tl_where(sql_dict['where']) is not None:
        pipeline.append(tl_where(sql_dict['where']))
    if tl_group(sql_dict['group'], sql_dict['having'], sql_dict['order'], sql_dict['projection']) is not None:
        pipeline.append(tl_group(sql_dict['group'], sql_dict['having'], sql_dict['order'], sql_dict['projection']))

    if tl_having(sql_dict['having']) is not None:
        pipeline.append(tl_having(sql_dict['having']))
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