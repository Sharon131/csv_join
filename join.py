import argparse
import pandas


def print_header(columns1, columns2, column_name):
    for col1 in columns1:
        print(col1, end='\t')
    for col2 in columns2:
        if col2 != column_name:
            print(col2, end='\t')
    print("")


def print_joined(row1, row2, columns1, columns2, column_name):
    for col1 in columns1:
        print(row1[col1], end='\t')
    for col2 in columns2:
        if col2 != column_name:
            if row2.empty:
                print("NULL", end='\t')
            else:
                print(row2[col2], end='\t')
    print("")


def left_join(filename1, filename2, column_name, nrows=50):
    print("Left join:")
    df1 = pandas.read_csv(filename1, nrows=nrows)
    df2 = pandas.read_csv(filename2, nrows=nrows)
    columns1 = df1.columns.to_list()
    columns2 = df2.columns.to_list()
    iter1 = 0
    iter2 = 0

    print_header(df1.columns.to_list(), df2.columns.to_list(), column_name)
    while not df1.empty:
        flags = [False for i in range(nrows)]
        while not df2.empty:
            for i, row1 in df1.iterrows():
                for j, row2 in df2.iterrows():
                    if row1[column_name] == row2[column_name]:
                        flags[i] = True
                        print_joined(row1, row2, columns1, columns2, column_name)
            iter2 += 1
            df2 = pandas.read_csv(filename2, nrows=nrows, skiprows=nrows * iter2 + 1, names=columns2)
        for i in range(len(flags)):
            if not flags[i]:
                print_joined(df1.iloc[i], pandas.Series([], dtype=object), columns1, columns2, column_name)
        iter2 = 0
        iter1 += 1
        df1 = pandas.read_csv(filename1, nrows=nrows, skiprows=nrows * iter1 + 1, names=columns1)
        df2 = pandas.read_csv(filename2, nrows=nrows)

    print("-----")


def inner_join(filename1, filename2, column_name, nrows=50):
    print("Inner join:")
    df1 = pandas.read_csv(filename1, nrows=nrows)
    df2 = pandas.read_csv(filename2, nrows=nrows)
    columns1 = df1.columns.to_list()
    columns2 = df2.columns.to_list()
    iter1 = 0
    iter2 = 0

    print_header(df1.columns.to_list(), df2.columns.to_list(), column_name)
    while not df1.empty:
        while not df2.empty:
            for i, row1 in df1.iterrows():
                for j, row2 in df2.iterrows():
                    if row1[column_name] == row2[column_name]:
                        print_joined(row1, row2, columns1, columns2, column_name)
            iter2 += 1
            df2 = pandas.read_csv(filename2, nrows=nrows, skiprows=nrows * iter2 + 1, names=columns2)
        iter2 = 0
        iter1 += 1
        df1 = pandas.read_csv(filename1, nrows=nrows, skiprows=nrows * iter1 + 1, names=columns1)
        df2 = pandas.read_csv(filename2, nrows=nrows)

    print("-----")


def right_join(filename1, filename2, column_name, nrows=50):
    left_join(filename2, filename1, column_name, nrows)


joins = {
    "left":     left_join,
    "inner":    inner_join,
    "right":    right_join
}


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description='Join records from two csv files.')
    arg_parser.add_argument('strings', nargs=2, type=str, help='names of files to join')
    arg_parser.add_argument('column_name', nargs='?', type=str, help='join condition')
    arg_parser.add_argument('join_type', nargs='?', type=str, default='inner',
                            choices=['left', 'inner', 'right'], help='type of join')
    args = arg_parser.parse_args()

    joins[args.join_type](args.strings[0], args.strings[1], args.column_name, nrows=1)
