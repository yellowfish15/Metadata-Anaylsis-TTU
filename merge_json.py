import json
import statistics

data = json.load(open('./json results/output1.json'))
data2 = json.load(open('./json results/output2.json'))


def recur(obj, obj2, keyname):
    if isinstance(obj, dict):
        for key in obj2:
            if key not in obj:
                obj[key] = obj2[key]
            else:
                obj[key] = recur(obj[key], obj2[key], key)
        return obj
    else:
        if keyname == "datatype(s)":
            for tp in obj2:
                if tp not in obj:
                    obj.append(tp)
        elif not isinstance(obj, str):
            return obj+obj2
        return obj

out_file = open("output_merged.json", "w")
data3 = recur(data, data2, "")

def calculate_stats(attr):
    if "values" in attr and len(attr["values"]) > 0:
        attr["num_values"] = len(attr["values"])
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


def calculate_stats_str(attr):
    if "values" in attr and len(attr["values"]) > 0:
        attr["num_values"] = len(attr["values"])
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


for key in data3["attributes"]:
    if data3["attributes"][key]["simplified_datatype"] == "str":
        calculate_stats_str(data3["attributes"][key])
    else:
        calculate_stats(data3["attributes"][key])

out_file.write(json.dumps(data3, indent=4))
out_file.close()