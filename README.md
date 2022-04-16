## CSV joiner

Program for VirtusLab Big Data internship recruitment process joining two CSV files which names are given as program arguments.

It uses Pandas library for parsing CSV files. 

### Running program

You can run program with two example files `file1.csv` and `file2.csv` with command `python join file1.csv file2.csv column_name join_type` where column_name is column by which join should be performed and join_type is type of join to perform (left, right or inner). Suggested value for `column_name` for running example is `Color`.

Running command `python join.py -h` will print help.
