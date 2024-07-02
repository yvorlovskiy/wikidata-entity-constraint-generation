import argparse
from math import floor
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
    triples = []
    try:
        for entry in jsonl_generator(filename):
            if not isinstance(entry, dict):
                print(f"Expected dict, but got {type(entry)}: {entry}")
                continue
            if entry.get('property_id') == property_id and entry.get('value') == item:
                qid = entry.get('qid')
                triples.append((qid, property_id, item))
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
    return triples

def find_properties(valid_qids, seen_properties, filename):
    property_bank = defaultdict(Counter)
    try:
        for entry in jsonl_generator(filename):
            if not isinstance(entry, dict):
                print(f"Expected dict, but got {type(entry)}: {entry}")
                continue
            if entry.get('qid') in valid_qids and entry.get('property_id') not in seen_properties:
                property_bank[entry.get('property_id')][entry.get('value')] += 1
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
    return property_bank

def property_item_counts(property_bank, seen_items={}):
    property_item_counts = {}
    for property, value_counts in property_bank.items():
        property_item_counts[property] = {qid: count for qid, count in value_counts.items() if qid not in seen_items}
    return property_item_counts  

def filter_results_by_count(filtered_results, min_group_size=10, max_group_size=1000):
    filtered_property_bank = {}
    for p, q_counts in filtered_results.items():
        filtered_q_counts = {q: count for q, count in q_counts.items() if count >= min_group_size and count <= max_group_size}
        if filtered_q_counts:
            filtered_property_bank[p] = filtered_q_counts
    return filtered_property_bank

def next_q_p(item, property_id, data_files, num_procs, seen_properties={}, seen_items={}, min_group_size=20):
    print("First pass: Collecting triples")
    pool = Pool(processes=num_procs)
    triple_results = list(tqdm(
        pool.imap_unordered(
            partial(find_qids, item, property_id),
            data_files
        ),
        total=len(data_files)
    ))
    
    all_triples = [triple for sublist in triple_results for triple in sublist]
    valid_qids = {triple[0] for triple in all_triples}
    print(f"Found {len(valid_qids)} valid QIDs")
    
    print("Second pass: Collecting properties and values...")
    property_results = list(tqdm(
        pool.imap_unordered(
            partial(find_properties, valid_qids, seen_properties),
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
    filtered_results = property_item_counts(merged_results, seen_items)
    
    return filtered_results, valid_qids

def search_distributor(item, property_id, data_files, num_procs, max_depth, min_group_size, max_group_size, depth=0, seen_items=None, seen_properties=None, chain=None, valid_qids=None):
    if depth >= max_depth:
        return 
    
    seen_items = seen_items or {item}
    seen_properties = seen_properties or {property_id}
    chain = chain or [[item, property_id]]
    
    # Find next potential qs and ps to search
    current_results, new_valid_qids = next_q_p(item, property_id, data_files, num_procs, seen_properties=seen_properties, seen_items=seen_items)
    
    # If this is the first call, initialize valid_qids
    if valid_qids is None:
        valid_qids = new_valid_qids
    else:
        # Intersect with existing valid_qids to maintain the initial condition
        valid_qids = valid_qids.intersection(new_valid_qids)
    
    in_range_results = filter_results_by_count(current_results, min_group_size, max_group_size)
    over_results = filter_results_by_count(current_results, min_group_size=min_group_size * 2)
    print(chain)
    print(in_range_results)
    
    if not over_results:
        return chain, in_range_results
    
    for new_property, new_items in over_results.items():
        for new_item, count in new_items.items():
            # Only proceed if the new item is in our valid_qids set
            if new_item in valid_qids:
                print(f"Adding to search: Property {new_property}, Item {new_item}, Count {count}")
                seen_properties.add(new_property)
                seen_items.add(new_item)
                new_chain = chain + [[new_item, new_property]]
                depth += 1
                search_distributor(new_item, new_property, data_files, num_procs, max_depth, min_group_size, max_group_size, 
                                   depth=depth, seen_items=seen_items, seen_properties=seen_properties, chain=new_chain, valid_qids=valid_qids)
                break
 
    return chain, in_range_results

def main():
    parser = get_arg_parser()
    args = parser.parse_args()
    data_files = get_batch_files(args.data)
    
    results = search_distributor(args.item, args.property, data_files, args.num_procs, max_depth=args.max_depth, min_group_size=args.min_group_size, max_group_size=args.min_group_size*2)
    print(results)

if __name__ == "__main__":
    main()