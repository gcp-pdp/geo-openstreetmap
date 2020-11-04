import sys

NUM_COLUMNS = 50
# Template for resulting schema word:STRING,f1:FLOAT,f2...
SCHEMA = 'word:STRING,{}'
if __name__ == '__main__':
    num_columns = NUM_COLUMNS
    if len(sys.argv) > 1:
        num_columns = int(sys.argv[1])

    print(SCHEMA.format(','.join(['f{}:FLOAT'.format(x) for x in range(1, num_columns + 1)])))
