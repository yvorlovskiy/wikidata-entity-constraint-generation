"""Assortment of useful utility functions 
"""

import os
import ujson as json


def jsonl_generator(fname):
    """ Returns generator for jsonl file """
    with open(fname, 'r', encoding='utf-8', errors='ignore') as file:
        for line in file:
            line = line.strip()
            if not line:  # Skip empty lines
                continue
            try:
                if line[-1] == ',':
                    d = json.loads(line[:-1])
                else:
                    d = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON line: {e}")
                continue
            yield d

def get_batch_files(fdir):
    """ Returns paths to files in fdir """
    filenames = os.listdir(fdir)
    filenames = [os.path.join(fdir, f) for f in filenames]
    print(f"Fetched {len(filenames)} files from {fdir}")
    return filenames

