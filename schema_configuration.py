import os
import json
import logging

logger = logging.getLogger()

def populate_fields(client, measurement):
  
    if client is None or measurement == "":
        logger.critical("Source InfluxDB instane not defined. Existing ...")
        exit(1)
    else:
        query = f'SHOW FIELD KEYS FROM "{measurement}"'
        logger.debug(f"Fetching fields from database with query:\n{query}")
        fields_table = client.query(query, language="influxql")
        fields = dict(zip([f.as_py() for f in fields_table["fieldKey"]], 
                            [f.as_py() for f in fields_table["fieldType"]]))
        return fields
    

def populate_tag_values():
    json_str = os.getenv("INCLUDE_TAG_VALUES")
    logger.debug(f"INCLUDE_TAG_VALUES envar set to: {json_str}")

    try:
        if json_str is not None:
            return json.loads(json_str)
        else:
            return None
    except Exception as e:
        logger.critical(f"Failed to parse INCLUDE_TAG_VALUES: {str(e)}")
        exit(1)

def populate_tags(client, measurement):  
    if client is None or measurement == "":
        logger.critical("Source InfluxDB instance not defined. Existing ...")
        exit(1)
    
    include_tags = os.getenv('INCLUDE_TAGS')
    logger.debug(f"INCLUDE_TAGS set to: {include_tags}")
    if include_tags is not None:
        try: 
            if include_tags.isspace() or len(include_tags) == 0:
                logger.debug(f"INCLLUDE_TAGS is empty: {include_tags}")
                return []
            else:
                it = include_tags.split(",")
                logger.debug(f"Pared INCLUDE_TAGS:\n{it}")
                return it
        except Exception as e:
            logging.critical(f"parsing INCLUDE_TAGS failed: {str(e)}")
            exit(1)

    else:
        query = f'SHOW TAG KEYS FROM "{measurement}"'
        logger.debug(f"No tag list set by user, retrieving from InfluxDB with query: {query}")
        tags_table = client.query(query, language="influxql")
        tags = tags_table["tagKey"]
        tags_list = tags.to_pylist()
        logger.debug(f"Retrieved tag list from {measurement}:\n{tags_list}")
        return tags_list


