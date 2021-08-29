'''
Merge instances of unique attributes from output txt file
'''
import sys, statistics, json


'''
Add these statistics
- number of values
- min
- max
- range
- mean
- median
- mode
- standard deviation
'''
def add_stats_numerical(attr):
    if len(attr["values"]) > 0:
        if attr["type"] == "float":
            attr["values"] = [float(elem) for elem in attr["values"]]
        else:
            attr["values"] = [int(elem) for elem in attr["values"]]
        min_val = attr["values"][0]
        max_val = attr["values"][0]
        sum_val = 0
        for i in range(len(attr["values"])):
            curr = attr["values"][i]
            sum_val += curr
            if curr < min_val:
                min_val = curr
            if curr > max_val:
                max_val = curr
        attr["min_value"] = min_val
        attr["max_value"] = max_val
        attr["range"] = max_val - min_val
        attr["mean"] = (sum_val)/(len(attr["values"]))
        attr["median"] = statistics.median(attr["values"])
        attr["mode"] = statistics.mode(attr["values"])
        if len(attr["values"]) > 1:
            attr["standard_deviation"] = statistics.stdev(attr["values"])


'''
Add theses statistics
- number of values
- min of string value lengths
- max of string value lengths
- range of string value lengths
- mean of string value lengths
- median of string value lengths
- mode of string value lengths
- standard deviation of string value lengths
'''
def add_stats_string(attr):
    if len(attr["values"]) > 0:
        str_lengths = [len(elem) for elem in attr["values"]]
        min_val = str_lengths[0]
        max_val = str_lengths[0]
        sum_val = 0
        for i in range(len(str_lengths)):
            curr = str_lengths[i]
            sum_val += curr
            if curr < min_val:
                min_val = curr
            if curr > max_val:
                max_val = curr
        attr["min_value"] = min_val
        attr["max_value"] = max_val
        attr["range"] = max_val - min_val
        attr["mean"] = (sum_val)/(len(str_lengths))
        attr["median"] = statistics.median(str_lengths)
        attr["mode"] = statistics.mode(str_lengths)
        if len(attr["values"]) > 1:
            attr["standard_deviation"] = statistics.stdev(str_lengths)


attributes = {}
def read(f):
    global attributes
    #sys.stdout = open('output_processed.txt', 'w') # Direct standard output to file
    for line in f:
        if line == '\tEND\t\n':
            break
        split_line = line.strip().split('\t')
        if len(split_line) < 4:
            split_line.append('')
        if split_line[0] not in attributes:
            attributes[split_line[0]] = {}
            attributes[split_line[0]]['type'] = split_line[1]
            attributes[split_line[0]]['num_values'] = int(split_line[2])
            attributes[split_line[0]]['values'] = split_line[3].split('~~')
        else:
            attributes[split_line[0]]['num_values'] += int(split_line[2])
            attributes[split_line[0]]['values'] += split_line[3].split('~~')

    for key in attributes.keys():
        if attributes[key]['type'] == 'str':
            add_stats_string(attributes[key])
        elif not(attributes[key]['type'] == 'other'):
            add_stats_numerical(attributes[key])


files = ['output_raw.txt']
for name in files:
    f = open(name, 'r')
    read(f)
f = open('output_processed.txt', 'w')
f.write(json.dumps(attributes, indent=4))
f.close()