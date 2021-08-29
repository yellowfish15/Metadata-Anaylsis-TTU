'''
This script is used to analyze metadata stored in JSON format
- A json file named "output.json" must be stored in the same directory as this script
in order for it to run properly
- Should be used in conjunction with script 'sdss_reader_json.py'

Created by: Yu Lim
'''

import json
import os


f_count = 1

# print options and get user choice
def getchoice(temp_choices):
    choice = None
    for key in temp_choices.keys():
        print('\t' + key + ') ' + temp_choices[key])
    while choice not in temp_choices:
        choice = str(input('> '))
        if choice not in temp_choices:
            print('\tSorry, that isn\'t a valid option! Try again...')
    return choice


# save json data to a file
def save_data(data):
    global f_count
    json_data = json.dumps(data, indent=4)
    print(json_data)
    print('\nWould you like to save this data to a file?')
    choice = getchoice({'y': 'yes', 'n': 'no'})
    if choice == 'y':
        f_name = 'save' + str(f_count) + '.json'
        while f_name in os.listdir(os.getcwd()):
            f_count += 1
            f_name = 'save' + str(f_count) + '.json'
        json_out_file = open(f_name, "w")
        json_out_file.write(json_data)
        json_out_file.close()
        print('\tData saved to file \'' + f_name +'\'')
        print('\tYou can view the file with any text editor')
    else:
        print('\tOk if you still need it, you can copy paste from the console output')
        return
    return f_name


def recur(obj, data):
    print('\nWhat would you like to analyze with \'' + str(obj) + '\'?')
    temp_choices = {}
    if isinstance(data, dict):
        count = 1
        for key in data.keys():
            temp_choices[str(count)] = key
            count += 1
        temp_choices['v'] = 'view statistics/properties of this object'
    temp_choices['r'] = 'view raw value(s) in this object'
    temp_choices['c'] = 'cancel'
    choice = getchoice(temp_choices)
    if choice == 'c':
        return
    elif choice == 'v':
        print('\nIndividual properties of \''+str(obj)+'\' -\n')
        for key in data.keys():
            if not isinstance(data[key], dict) and not isinstance(data[key], list):
                print(key + ': ' + str(data[key]))
        input('\n\tPress Enter to continue...')
        recur(obj, data)
    elif choice == 'r':
        print('\tThe raw data is:')
        save_data(data)
    else:
        if isinstance(data[temp_choices[choice]], dict):
            recur(temp_choices[choice], data[temp_choices[choice]])
        else:
            print('\tThe value of \'' + str(temp_choices[choice]) + '\' is: ' + str(data[temp_choices[choice]]))
            input('\tPress Enter to continue...')
            recur(obj, data)

try:
    data = json.load(open('output.json'))
except Exception as e:
    print('You must have a file named \'output.json\' in the same directory as this python script!')
    exit()

print('\n\n\tHi!')
print('\tThis is a tool created for analyzing metadata stored in json format')
print('\tThe file \'output.json\' will be used for this analysis')
print('\n\t- Created by Yu Lim')

while True:
    print('\n\nWhat do you want to do?')
    temp_choices = {
        '1': 'View total dataset statistics',
        '2': 'Get names and counts of unique attributes',
        '3': 'Analyze a particular attribute',
        'q': 'Exit program'
    }
    choice = getchoice(temp_choices)
    if choice == '1':
        recur('total dataset statistics', data['total_statistics'])
    elif choice == '2':
        print('\nWhat types of attributes would you like to look at')
        temp_choices = {
            '1': 'Get all unique string attribute names and counts',
            '2': 'Get all unique integer attribute names and counts',
            '3': 'Get all unique float attribute names and counts',
            '4': 'Get all other type unique attribute names and counts',
            '5': 'Get all unique attribute names and counts (regardless of datatype)',
            'c': 'Go back'
        }
        choice2 = getchoice(temp_choices)
        attr_count = 0
        if choice2 == '1':
            for attr_key in data['attributes']:
                if data['attributes'][attr_key]['simplified_datatype'] == 'str':
                    print('Attribute name:', attr_key.ljust(25, ' '), "Frequency of occurrence:", data['attributes'][attr_key]['appearances'])
                    attr_count += 1
            print('Found', attr_count, 'unique string attributes')
        elif choice2 == '2':
            for attr_key in data['attributes']:
                if data['attributes'][attr_key]['simplified_datatype'] == 'int':
                    print('Attribute name:', attr_key.ljust(25, ' '), "Frequency of occurrence:", data['attributes'][attr_key]['appearances'])
                    attr_count += 1
            print('Found', attr_count, 'unique integer attributes')
        elif choice2 == '3':
            for attr_key in data['attributes']:
                if data['attributes'][attr_key]['simplified_datatype'] == 'float':
                    print('Attribute name:', attr_key.ljust(25, ' '), "Frequency of occurrence:", data['attributes'][attr_key]['appearances'])
                    attr_count += 1
            print('Found', attr_count, 'unique float attributes')
        elif choice2 == '4':
            for attr_key in data['attributes']:
                if data['attributes'][attr_key]['simplified_datatype'] == 'other':
                    print('Attribute name:', attr_key.ljust(25, ' '), "Frequency of occurrence:", data['attributes'][attr_key]['appearances'])
                    attr_count += 1
            print('Found', attr_count, 'unique \'other\' type attributes')
        elif choice2 == '5':
            for attr_key in data['attributes']:
                print('Attribute name:', attr_key.ljust(25, ' '), "Frequency of occurrence:", data['attributes'][attr_key]['appearances'])
                attr_count += 1
            print('Found', attr_count, 'unique attributes (all datatypes combined)')
        else:
            continue
        input('Press Enter to continue...')
    elif choice == '3':
        recur('unique attributes', data['attributes'])
    else:
        print('\tProgram exited')
        quit()
