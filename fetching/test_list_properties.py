"""
For example: all entities which played 'quarterback' on a football team (corresponding to P413 and a value of Q622747)

To run:
python3.6 fetch_with_rel_and_value.py --data $DATA --out_dir $OUT
"""



import argparse
from tqdm import tqdm
from multiprocessing import Pool
from functools import partial
from collections import defaultdict, Counter
from fetching.utils import jsonl_generator, get_batch_files


def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type=str, default='/data/yury/dump/entity_rels', help='path to output directory')
    parser.add_argument('--entity', type=str, default='Q622747', help='entity value')
    parser.add_argument('--num_procs', type=int, default=35, help='Number of processes')
    return parser


def filtering_func(entity, filename):
    filtered = defaultdict(set)
    try:
        for item in jsonl_generator(filename):
            if not isinstance(item, dict):
                print(f"Expected dict, but got {type(item)}: {item}")
                continue
            if item.get('value') == entity:
                filtered[item.get('property_id')].add(item.get('qid'))
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
    return filtered


def merge_dictionaries(dict_list):
    merged = defaultdict(set)
    for d in dict_list:
        for key, value in d.items():
            merged[key].update(value)
    return merged


def main():
    args = get_arg_parser().parse_args()

    table_files = get_batch_files(args.data)
    pool = Pool(processes=args.num_procs)
    filtered = []

    for output in tqdm(
            pool.imap_unordered(
                partial(filtering_func, args.entity), table_files, chunksize=1),
            total=len(table_files)
    ):
        filtered.append(output)

    pool.close()
    pool.join()

    merged_filtered = merge_dictionaries(filtered)

    property_counts = Counter({k: len(v) for k, v in merged_filtered.items()})
    top_20_properties = property_counts.most_common(20)

    print(f"Top 20 properties by count:")
    for prop, count in top_20_properties:
        print(f"Property: {prop}, Count: {count}")

    print(f"Extracted {len(filtered)} rows:")
    for i, item in enumerate(filtered):
        print(f"Row {i}: {item}")


if __name__ == "__main__":
    main()