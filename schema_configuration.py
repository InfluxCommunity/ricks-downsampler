
def populate_fields(client, measurement):
  
    if client is None or measurement == "":
        print("Source InfluxDB instane not defined. Existing ...")
        exit(1)
    else:
        query = f'SHOW FIELD KEYS FROM "{measurement}"'
        fields_table = client.query(query, language="influxql")
        fields = dict(zip([f.as_py() for f in fields_table["fieldKey"]], 
                            [f.as_py() for f in fields_table["fieldType"]]))
        return fields
    

def populate_tags(client, measurement):  
    if client is None or measurement == "":
        print("Source InfluxDB instane not defined. Existing ...")
        exit(1)
    else:
        query = f'SHOW TAG KEYS FROM "{measurement}"'
        tags_table = client.query(query, language="influxql")
        tags = tags_table["tagKey"]
        return tags


