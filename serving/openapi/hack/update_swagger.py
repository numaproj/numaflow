import json
import sys

def update_swagger(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)

    definitions = data.get('definitions', {})

    for key in ['Blackhole', 'Log', 'NoStore']:
        if key in definitions:
            definitions[key]['allOf'] = []

    with open(file_path, 'w') as file:
        json.dump(data, file, indent=2)

if __name__ == "__main__":
    update_swagger(sys.argv[1])