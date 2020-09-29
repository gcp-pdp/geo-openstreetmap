import fileinput
import json

if __name__ == '__main__':
    num_columns = None
    for line in fileinput.input():
        columns = line.strip().split(' ')

        if num_columns is None:
            num_columns = len(columns) - 1

        result = {'word': columns[0]}
        for i in range(num_columns):
            result['f{}'.format(i+1)] = columns[i+1]
        print(json.dumps(result))
