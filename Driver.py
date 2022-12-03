import argparse
import sys
import TransactionManager as t
import re
import os
fname = ""

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="specify the input file to be read or the directory having multiple files")
args = parser.parse_args()

fname = args.filename

tm = t.TransactionManager()


def check_file_or_dir(fdname):
    if os.path.exists(fdname) and os.path.isdir(fdname):
        return True
    return False

def start(fdname):
    if check_file_or_dir(fdname):
        files = os.listdir(fdname)
        if len(files) == 0:
            print("ERROR: Empty Directory.")
        else:
            print("Input argument is a directory. Start executing all files in directory.")
            for filename in files:
                f = os.path.join(fdname, filename)
                # checking if it is a file
                if os.path.isfile(f):
                    print("Start executing file:", f)
                    print()
                    read_file(f)
                    print()
    else:
        if os.path.isfile(fdname):
            print("Input argument is a file. Start executing file:", fdname)
            print()
            read_file(fdname)
        else:
            print("ERROR: Input argument not a file or directory. Please check the arguments again.")

def read_file(file):
    f = open(file, 'r')
    for x in f:
        x = x.replace(")", "").strip()
        s = re.split('\(|\)|,', x)
        #print(s)
        tm.execute_transaction(s)
    f.close()

try:
    start(fname)
except FileNotFoundError as e:
    print("Error Opening File:" + str(e))



