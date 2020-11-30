# projection = 'id, name, price' # check the comma, split by comma
# join = 'host on ...' #check joined table and on
# where 


def parse_sql(sql):
    print(sql.find('select '))
    if sql.find('select ') == -1 or sql.find(' from ') == -1:
        return None


if __name__ == '__main__':
    sql = "select * from listing join host on listing.host_id = host.id"
    result = parse_sql(sql)
    print(result)