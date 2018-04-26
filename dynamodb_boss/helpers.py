from boto3.dynamodb.conditions import And, Key


def EZQuery(tbl, key_attr, key, index_name=None, range_attr=None, op=None, range=None):
    kce = Key(key_attr).eq(key)
    if range_attr:
        op_func = getattr(Key(range_attr), op)
        range_op = op_func(*range) \
                if type(range) in (tuple, list) \
                else op_func(range)
        kce = And(kce, range_op)
    kwargs = {'KeyConditionExpression': kce}
    if index_name:
        kwargs['IndexName'] = index_name
    return tbl.query(**kwargs)

