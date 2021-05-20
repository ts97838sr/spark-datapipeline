import yaml
import json


def convertInputArgs(inFile):
    """

    :param inFile: Takes YML/YAML file as input and converts into json layout consumed by workflow
    :return:  json content as a string.
    """
    with open(inFile, 'r') as yaml_in:
        try:
            yaml_object = yaml.safe_load(yaml_in)
            # yaml_object will be a list or a dict
        except Exception as e:
            print(f'============= Invalid yaml Error {e} =============')
            exit(1)
        else:
            json_out = json.dumps(yaml_object)
            print('Conversion to Json complete')
    return json_out

"""
Work in progress .. json generated has additional root tag .

import xmltodict
with open("/Users/tamalsarkar/IdeaProjects/spark-datapipeline/config_param/TEST_XML_INPUT.xml") as xml_file:
    data_dict = xmltodict.parse(xml_file.read())
    json_data = json.dumps(data_dict)
    xml_file.close()

with open("/Users/tamalsarkar/IdeaProjects/spark-datapipeline/config_param/test2.json", "w") as json_file:
    json_file.write(json_data)
    json_file.close()
"""