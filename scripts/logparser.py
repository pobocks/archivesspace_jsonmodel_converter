
""" Take a log produced by one of the create* modules, and turn it into a csv file for further analysis """

import pandas as pd
import argparse

def convert(infile, outfile):
    with open(infile, encoding='utf-8') as inputfile:
        df = pd.read_json(inputfile, lines=True)

    df.to_csv(outfile, encoding='utf-8', index=False)
    
if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='Get filepaths.')
    parser.add_argument("-i", help="Path to input file containing json", required=True)
    parser.add_argument("-o", help="Path to File for output csv", required=True)
    args = parser.parse_args()
    convert(args.i, args.o)
