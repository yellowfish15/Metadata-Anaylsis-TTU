'''
This python script generates a three-column list
from the metadata json file 'output.json'

Created by: Yu Lim
'''

import json

data = json.load(open('output.json'))
out_file = open("three_column_list.txt", "w")

out_file.write("Attribute name".ljust(26, ' ') + 'Data Type'.ljust(14, ' ') + '# Of Attribute Values\n')
for key in data["attributes"]:
    if data["attributes"][key]["simplified_datatype"] == 'str':
        out_file.write(key.ljust(26, ' ') + 'string'.ljust(14, ' ') + str(len(data["attributes"][key]["values"])) + '\n')
    else:
        out_file.write(key.ljust(26, ' ') + data["attributes"][key]["simplified_datatype"].ljust(14, ' ') + str(len(data["attributes"][key]["values"])) + '\n')

out_file.close()
