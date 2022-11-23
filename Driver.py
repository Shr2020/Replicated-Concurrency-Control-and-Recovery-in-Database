import argparse
import sys
#import TransactionManager as t
import re
fname = ""

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="specify the input file to be read")
args = parser.parse_args()

fname = args.filename

#tm = t.TransactionManager()

def start(file):
    f = open(file, 'r')
    for x in f:
        x = x.replace(")", "").strip()
        s = re.split('\(|\)|,', x)
        print(s)
    f.close()

try:
    start(fname)
except FileNotFoundError as e:
    print("Error Opening File:" + str(e))



