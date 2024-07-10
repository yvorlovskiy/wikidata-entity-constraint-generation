# python3 decoding.py --labels_dir /data/yury/wikidata/labels --properties_file /data/yury/wikidata/properties/en.json --input_json input.json --output_json output_decoded.json
import argparse
import json
import re
from tqdm import tqdm
from multiprocessing import Pool
from functools import partial
from utils import jsonl_generator, get_batch_files

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--labels_dir', type=str, default='/data/yury/wikidata/labels', help='path to labels directory')
    parser.add_argument('--properties_file', type=str, default='/data/yury/wikidata/properties/en.json', help='path to properties file')
    parser.add_argument('--input_json', type=str, required=True, help='Path to input JSON file')
    parser.add_argument('--num_procs', type=int, default=40, help='Number of processes')
    return parser

def load_labels_chunk(filename):
    labels = {}
    for item in jsonl_generator(filename):
        if 'qid' in item and 'label' in item and item['qid'].startswith('Q'):
            labels[item['qid']] = item['label']
    return labels

def load_properties(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def decode_term(term, labels, properties):
    if term.startswith('Q'):
        return labels.get(term, term)
    elif term.startswith('P'):
        return properties.get(term, term)
    return term

def decode_key(key, labels, properties):
    def replace_match(match):
        return decode_term(match.group(0), labels, properties)
    
    return re.sub(r'[PQ]\d+', replace_match, key)

def decode_json(data, labels, properties):
    if isinstance(data, dict):
        return {
            decode_key(k, labels, properties): decode_json(v, labels, properties)
            for k, v in data.items()
        }
    elif isinstance(data, list):
        return [decode_json(item, labels, properties) for item in data]
    elif isinstance(data, str):
        return decode_key(data, labels, properties)
    else:
        return data

def main():
    args = get_arg_parser().parse_args()

    print("Loading labels...")
    label_files = get_batch_files(args.labels_dir)
    pool = Pool(processes=args.num_procs)
    labels = {}
    for chunk_labels in tqdm(
        pool.imap_unordered(load_labels_chunk, label_files, chunksize=1),
        total=len(label_files),
        desc="Loading label files"
    ):
        labels.update(chunk_labels)
    print(f"Loaded {len(labels)} labels")

    print("Loading properties...")
    properties = load_properties(args.properties_file)
    print(f"Loaded {len(properties)} properties")

    print("Loading and decoding input JSON...")
    with open(args.input_json, 'r') as infile:
        input_data = json.load(infile)

    decoded_data = decode_json(input_data, labels, properties)
    output_file = args.input_json.replace('.json', '_decoded.json')
    print("Saving decoded JSON...")
    with open(output_file, 'w') as outfile:
        json.dump(decoded_data, outfile, indent=2)
    print(f"Decoded JSON saved to {output_file}")

    # Print a sample of the decoded data
    print("\nSample of decoded data:")
    sample = list(decoded_data.items())[:2]
    for k, v in sample:
        print(f"{k}: {v}")

if __name__ == "__main__":
    main()