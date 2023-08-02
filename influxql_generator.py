from datetime import timezone
from collections import defaultdict

def generate_fields_string(fields_dict, aggregate):
    query = ''
    for field_name, field_type in fields_dict.items():
        if field_is_num(field_name, fields_dict):
            if query != '':
                query += ',\n'
            query += f'\t{aggregate}("{field_name}") as "{field_name}"'
    return query

def generate_group_by_string(tags_list, interval):
    group_by_clause = f'time({interval})'

    for tag in tags_list:
        group_by_clause += f', {tag}'
    return group_by_clause

def generate_tag_filter_clause(tag_values):
    if tag_values is None:
        return ""
    
    influxql = ""
    for key, values in tag_values.items():
        joined_values = "|".join(values)
        influxql += f'AND\n\t"{key}" =~ /({joined_values})/\n'
    return influxql

def get_query(fields_dict, measurement, then, now, tags_list, interval, aggregate, tag_values ):
    fields_clause = generate_fields_string(fields_dict, aggregate)
    tags_clause = generate_group_by_string(tags_list, interval)
    tag_values = generate_tag_filter_clause(tag_values)

    query = f"""
SELECT
    {fields_clause}
FROM
    {measurement}
WHERE
    time > '{then.strftime('%Y-%m-%d %H:%M:%S')}'
AND
    time < '{now.strftime('%Y-%m-%d %H:%M:%S')}'
{tag_values}
GROUP BY
    {tags_clause}
    """

    return query

def field_is_num(field_name, fields_dict):
    numeric_types = ['integer', 'float', 'double']
    field_type = fields_dict.get(field_name)
    if field_type in numeric_types:
        return True
    return False    