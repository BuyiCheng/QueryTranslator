def parse_where_value(value, ishaving=False):
    # value price between 100 and 300
    isNot = False
    preLogic = '$and'
    if value.startswith("not "):
        isNot, value = True, value[len('not '):]
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
        if isNot:
            preLogic = '$or'
            value_list.append({attr: {'$not':{compare_dict['>=']: between_1}}})
            value_list.append({attr: {'$not':{compare_dict['<=']: between_2}}})
        else:
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
        if isNot:
            value_list.append({attr: {'$not':{compare_dict[compare]: in_list}}})
        else:
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
        if isNot:
            value_list.append({attr: {'$not':{compare_dict[compare]: attr_value}}})
        else:
            value_list.append({attr: {compare_dict[compare]: attr_value}})
    return preLogic, value_list


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
                preLogic, parse_list = parse_where_value(s.split(':')[1])
                if preLogic == '$or':
                    result['$or'].append({preLogic: []})
                    i = i + 1
                for j in parse_list:
                    # if isNot:
                    #     j = {'$not':j}
                    if preLogic == '$or':
                        result['$or'][i][preLogic].append(j)
                    else:
                        result['$or'][i]['$and'].append(j)
    elif len(or_list) == 1:
        result = {'$and':[]}
        for s in or_list[0]:
            # isNot, parse_list = parse_where_value(s.split(':')[1])
            for j in parse_where_value(s.split(':')[1]):
                # if isNot:
                #     for k,v in j.items():
                #         result['$and'].append({k:{'$not': v}})
                # else:
                #     result['$and'].append(j)
                result['$and'].append(j)

    else:
        result = None
    print('result:',result if result is not None else 'None')
    return {'$match': result} if len(where) != 0 else None
