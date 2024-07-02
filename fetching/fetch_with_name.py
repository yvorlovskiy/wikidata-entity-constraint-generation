"""
This script analyzes the entity_rels file for a specific Wikidata item and returns its properties
sorted by the number of values each property has.

To run:
python3.6 fetch_item_properties_sorted.py --data $DATA --item $ITEM_QID --top_n $N
"""
import argparse
from tqdm import tqdm
from multiprocessing import Pool
from collections import Counter
from fetching.utils import jsonl_generator, get_batch_files

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type=str, default='/data/yury/dump/entity_rels', help='path to input data directory')
    parser.add_argument('--item', type=str, required=True, help='QID of the Wikidata item to analyze')
    parser.add_argument('--top_n', type=int, default=20, help='Number of top properties to display')
    parser.add_argument('--num_procs', type=int, default=10, help='Number of processes')
    return parser

def count_item_properties(item_qid, filename):
    property_values = Counter()
    for entry in jsonl_generator(filename):
        if entry['qid'] == item_qid:
            property_values[entry['property_id']] += 1
    return property_values

def main():
    args = get_arg_parser().parse_args()
    table_files = get_batch_files(args.data)
    
    pool = Pool(processes=args.num_procs)
    
    print(f"Analyzing properties for item {args.item}...")
    all_property_values = Counter()
    for counts in tqdm(
        pool.imap_unordered(lambda f: count_item_properties(args.item, f), table_files, chunksize=1),
        total=len(table_files)
    ):
        all_property_values.update(counts)
    
    print(f"\nTop {args.top_n} properties for item {args.item} by number of values:")
    for i, (prop, count) in enumerate(all_property_values.most_common(args.top_n), 1):
        print(f"{i}. Property {prop}: {count} value{'s' if count != 1 else ''}")
    
    print(f"\nTotal unique properties found for item {args.item}: {len(all_property_values)}")

if __name__ == "__main__":
    main()