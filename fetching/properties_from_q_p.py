import argparse
from tqdm import tqdm
from multiprocessing import Pool
from functools import partial
from collections import defaultdict, Counter
from fetching.utils import jsonl_generator, get_batch_files

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type=str, default='/data/yury/dump/entity_rels', help='path to output directory')
    parser.add_argument('--item', type=str, required=True, help='item value (e.g., Q95153477 for a specific country)')
    parser.add_argument('--property', type=str, required=True, help='property id (e.g., P119)')
    parser.add_argument('--num_procs', type=int, default=35, help='Number of processes')
    return parser

def nested_dict():
    return defaultdict(set)

def filtering_func(item, property_id, filename):
    filtered = defaultdict(nested_dict)
    try:
        for entry in jsonl_generator(filename):
            if not isinstance(entry, dict):
                print(f"Expected dict, but got {type(entry)}: {entry}")
                continue
            if entry.get('property_id') == property_id and entry.get('value') == item:
                # This is our initial property-item pair
                qid = entry.get('qid')
                filtered[qid]['initial'] = {(property_id, item)}
            elif entry.get('property_id') == property_id and entry.get('qid') == item:
                # This is a new property for an item that was previously a value
                new_property = entry.get('property_id')
                new_value = entry.get('value')
                filtered[item][new_property].add(new_value)
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
    return dict(filtered)  # Convert to regular dict for pickling

def merge_dictionaries(dict_list):
    merged = defaultdict(nested_dict)
    for d in dict_list:
        for qid, props in d.items():
            for prop, values in props.items():
                merged[qid][prop].update(values)
    return merged

def count_properties(merged_filtered):
    property_counts = Counter()
    for qid, props in merged_filtered.items():
        if 'initial' in props:
            for prop, values in props.items():
                if prop != 'initial':
                    property_counts[prop] += len(values)
    return property_counts

def main():
    args = get_arg_parser().parse_args()
    table_files = get_batch_files(args.data)
    
    pool = Pool(processes=args.num_procs)
    filtered = []
    for output in tqdm(
        pool.imap_unordered(
            partial(filtering_func, args.item, args.property), table_files, chunksize=1
        ),
        total=len(table_files)
    ):
        filtered.append(output)
    pool.close()
    pool.join()
    
    merged_filtered = merge_dictionaries(filtered)
    property_counts = count_properties(merged_filtered)
    
    sorted_properties = sorted(property_counts.items(), key=lambda x: x[1], reverse=True)
    print(f"Properties ranked by count for entities with {args.property}={args.item}:")
    for prop, count in sorted_properties:
        if prop != args.property:  # Exclude the initial property
            print(f"Property: {prop}, Count: {count}")

    # Print some example entries to verify the structure
    print("\nExample entries:")
    for qid, props in list(merged_filtered.items())[:5]:  # Print first 5 entries
        if 'initial' in props:
            print(f"QID: {qid}")
            print(f"  Initial: {props['initial']}")
            for prop, values in props.items():
                if prop != 'initial':
                    print(f"  {prop}: {values}")
            print()

if __name__ == "__main__":
    main()