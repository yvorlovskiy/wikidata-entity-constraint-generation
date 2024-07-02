import argparse
from collections import defaultdict, Counter
from functools import partial
from multiprocessing import Pool
from tqdm import tqdm
from fetching.utils import jsonl_generator, get_batch_files

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type=str, default='/data/yury/dump/entity_rels', help='path to output directory')
    parser.add_argument('--item', type=str, required=True, help='initial item value (e.g., Q6256 for country)')
    parser.add_argument('--property', type=str, required=True, help='initial property id (e.g., P31 for instance of)')
    parser.add_argument('--num_procs', type=int, default=35, help='Number of processes')
    parser.add_argument('--max_depth', type=int, default=3, help='Maximum recursive depth')
    parser.add_argument('--min_group_size', type=int, default=10, help='Minimum group size to consider')
    return parser

def nested_dict():
    return defaultdict(set)

def merge_sets(set_list):
    return set().union(*set_list)

def merge_counters(counter_list):
    return sum(counter_list, Counter())

def merge_dictionaries(dict_list):
    merged = defaultdict(nested_dict)
    for d in dict_list:
        for qid, props in d.items():
            for prop, values in props.items():
                merged[qid][prop].update(values)
    return merged


def find_qids(item, property_id, filename):
    qid_bank = set()
    try:
        for entry in jsonl_generator(filename):
            if not isinstance(entry, dict):
                print(f"Expected dict, but got {type(entry)}: {entry}")
                continue
            if entry.get('property_id') == property_id and entry.get('value') == item:
                qid = entry.get('qid')
                qid_bank.add(qid)
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
    return qid_bank

def find_properties(qid_bank, seen_properties, filename):
    property_bank = defaultdict(Counter)
    try:
        for entry in jsonl_generator(filename):
            if not isinstance(entry, dict):
                print(f"Expected dict, but got {type(entry)}: {entry}")
                continue
            if entry.get('qid') in qid_bank and entry.get('property_id') not in seen_properties:
                property_bank[entry.get('property_id')][entry.get('value')] += 1
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
    return property_bank

def property_item_counts(property_bank, filename, seen_items={}):
    property_item_counts = {}
    for property, value_counts in property_bank.items():
        property_item_counts[property] = {qid: count for qid, count in value_counts.items() if qid not in seen_items}
        
    return property_item_counts  

def filter_results_by_count(filtered_results, min_group_size=10):
    filtered_property_bank = {}
    for p, q_counts in filtered_results.items():
        # Filter out Qs with counts over 50
        filtered_q_counts = {q: count for q, count in q_counts.items() if count > min_group_size}
        if filtered_q_counts:
            filtered_property_bank[p] = filtered_q_counts
    return filtered_property_bank


def next_q_p(item, property_id, data_files, num_procs, seen_properties={}, seen_items={}):
    print("First pass: Collecting QIDs")
    pool = Pool(processes=num_procs)
    qid_results = list(tqdm(
        pool.imap_unordered(
            partial(find_qids, item, property_id),
            data_files
        ),
        total=len(data_files)
    ))
    
    all_qids = merge_sets(qid_results)
    print(f"Found {len(all_qids)} initial QIDs")
    
    print("Second pass: Collecting properties and values...")
    property_results = list(tqdm(
        pool.imap_unordered(
            partial(find_properties, all_qids, seen_properties),
            data_files
        ),
        total=len(data_files)
    ))
    pool.close()
    pool.join()
    
    merged_results = defaultdict(Counter)
    
    for prop_dict in property_results:
        for prop, counter in prop_dict.items():
            merged_results[prop] += counter
    
    print("Filtering out seen items...")
    seen_items = set()  # You might want to populate this with actually seen items
    filtered_results = property_item_counts(merged_results, seen_items)
    filtered_results = filter_results_by_count(filtered_results, 20)
    print(filtered_results)
    return filtered_results

def search_distributor(item, property_id, data_files, num_procs, max_depth, min_group_size):
    seen_items = {item}
    seen_properties = {property_id}
    results = {}
    
    for depth in range(max_depth):
        print(f"Searching at depth {depth + 1}")
        current_results = next_q_p(item, property_id, data_files, num_procs, seen_properties=seen_properties, seen_items=seen_items)
        for new_property, items in current_results.items():
            for new_item, count in items.items():
                if count > min_group_size:
                    print(f"Found significant group: Property {new_property}, Item {new_item}, Count {count}")
                    if new_property not in results:
                        results[new_property] = {}
                    results[new_property][new_item] = count
                    
                    # Recursively search for this new item and property
                    if count > min_group_size * 2:  # Example threshold for recursion
                        print(f"Recursively searching for Property {new_property}, Item {new_item}")
                        seen_properties.add(new_property)
                        seen_items.add(new_item)
                        nested_results = next_q_p(new_item, new_property, data_files, num_procs, seen_properties=seen_properties, seen_items=seen_items)
                        # You might want to process or store nested_results here
                        
                        # Break for testing purposes
                        print("Breaking after one iteration for testing")
                        return results
    
    return results

def main():
    parser = get_arg_parser()
    args = parser.parse_args()
    data_files = get_batch_files(args.data)
    
    results = search_distributor(args.item, args.property, data_files, args.num_procs, 1, 50)
    print(results)
    # print("First pass: Collecting QIDs")
    # pool = Pool(processes=args.num_procs)
    # qid_results = list(tqdm(
    #     pool.imap_unordered(
    #         partial(find_qids, args.item, args.property),
    #         data_files
    #     ),
    #     total=len(data_files)
    # ))
    
    # all_qids = merge_sets(qid_results)
    # print(f"Found {len(all_qids)} initial QIDs")
    
    # print("Second pass: Collecting properties and values...")
    # property_results = list(tqdm(
    #     pool.imap_unordered(
    #         partial(find_properties, all_qids, {}),
    #         data_files
    #     ),
    #     total=len(data_files)
    # ))
    # pool.close()
    # pool.join()
    
    # merged_results = defaultdict(Counter)
    
    # for prop_dict in property_results:
    #     for prop, counter in prop_dict.items():
    #         merged_results[prop] += counter
    
    # print("Filtering out seen items...")
    # seen_items = set()  # You might want to populate this with actually seen items
    # filtered_results = property_item_counts(merged_results, seen_items)
    # filtered_results = filter_results_by_count(filtered_results, 20)
    # print(filtered_results)
    

if __name__ == "__main__":
    main()