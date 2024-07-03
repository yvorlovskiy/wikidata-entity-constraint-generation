# example:
# python3 fetching/recursive_search.py --item Q6256 --property P31 --test --output out/out_instance_country.json
import argparse
from math import floor
from collections import defaultdict, Counter
from functools import partial
from multiprocessing import Pool
from tqdm import tqdm
from fetching.utils import jsonl_generator, get_batch_files
import json

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type=str, default='/data/yury/wikidata/entity_rels', help='path to output directory')
    parser.add_argument('--item', type=str, required=True, help='initial item value (e.g., Q6256 for country)')
    parser.add_argument('--property', type=str, required=True, help='initial property id (e.g., P31 for instance of)')
    parser.add_argument('--num_procs', type=int, default=50, help='Number of processes')
    parser.add_argument('--max_depth', type=int, default=3, help='Maximum recursive depth')
    parser.add_argument('--min_group_size', type=int, default=20, help='Minimum group size to consider')
    parser.add_argument('--test', action='store_true', help='Run in test mode (process only first 50 files)')
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

def find_qids(item, property_id, filename, valid_qids):
    triples = []
    try:
        for entry in jsonl_generator(filename):
            if not isinstance(entry, dict):
                print(f"Expected dict, but got {type(entry)}: {entry}")
                continue
            if entry.get('property_id') == property_id and entry.get('value') == item:
                if not valid_qids or entry.get('qid') in valid_qids:
                    qid = entry.get('qid')
                    triples.append((qid, property_id, item))
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
    return triples

def filter_file(args):
    filename, valid_qids = args
    filtered_data = []
    try:
        for entry in jsonl_generator(filename):
            if not isinstance(entry, dict):
                continue
            if entry.get('qid') in valid_qids:
                filtered_data.append(entry)
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
    return filtered_data

def filter_data_files(data_files, valid_qids, num_procs):
    pool = Pool(processes=num_procs)
    filtered_results = list(tqdm(
        pool.imap_unordered(filter_file, [(filename, valid_qids) for filename in data_files]),
        total=len(data_files),
        desc="Filtering data files"
    ))
    pool.close()
    pool.join()

    return [item for sublist in filtered_results for item in sublist]

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

def next_q_p(item, property_id, data_files, filtered_data, num_procs, valid_qids=None, seen_properties={}, seen_items={}, min_group_size=20):
    if valid_qids is None:
        print("First pass: Collecting triples")
        pool = Pool(processes=num_procs)
        triple_results = list(tqdm(
            pool.imap_unordered(
                partial(find_qids, item, property_id, valid_qids=None),
                data_files
            ),
            total=len(data_files),
            desc="Finding initial QIDs"
        ))
        pool.close()
        pool.join()
        
        all_triples = [triple for sublist in triple_results for triple in sublist]
        valid_qids = {triple[0] for triple in all_triples}
        print(f"Found {len(valid_qids)} valid QIDs")
        
        if filtered_data is None:
            print("Filtering data files based on valid QIDs...")
            filtered_data = filter_data_files(data_files, valid_qids, num_procs)
    else:
        print(f"Using {len(valid_qids)} pre-existing valid QIDs")
    
    print("Collecting properties and values...")
    property_bank = defaultdict(Counter)
    item_groups = defaultdict(lambda: defaultdict(list))
    for entry in tqdm(filtered_data, desc="Processing filtered data"):
        if entry.get('qid') in valid_qids and entry.get('property_id') not in seen_properties:
            property_bank[entry.get('property_id')][entry.get('value')] += 1
            item_groups[entry.get('property_id')][entry.get('value')].append(entry.get('qid'))
    
    print("Filtering out seen items...")
    filtered_results = property_item_counts(property_bank, seen_items)
    return filtered_results, valid_qids, item_groups, filtered_data


def search_distributor(item, property_id, data_files, num_procs, max_depth, min_group_size, max_group_size, depth=0, seen_items=None, seen_properties=None, chain=None, valid_qids=None, filtered_data=None):
    if depth >= max_depth:
        return None

    seen_items = seen_items or set()
    seen_properties = seen_properties or set()
    chain = chain or []

    # Add current item and property to the chain
    current_chain = chain + [[item, property_id]]


    # Add current item and property to the seen sets
    seen_items.add(item)
    seen_properties.add(property_id)
    chain.append([item, property_id])

    current_results, new_valid_qids, item_groups, filtered_data = next_q_p(item, property_id, data_files, filtered_data, num_procs, seen_properties=seen_properties, seen_items=seen_items, valid_qids=valid_qids)

    if valid_qids is None:
        valid_qids = new_valid_qids
    else:
        valid_qids = valid_qids.intersection(new_valid_qids)

    in_range_results = filter_results_by_count(current_results, min_group_size, max_group_size)
    over_results = filter_results_by_count(current_results, min_group_size=min_group_size * 2)

    print(f'Current depth: {depth}')
    print(chain)
    print(in_range_results)

    result = {
        "chain": current_chain,
        "results": in_range_results,
        "item_groups": item_groups,
        "children": {}
    }

    for new_property, new_items in over_results.items():
        for new_item, count in new_items.items():
            if new_item not in seen_items and new_property not in seen_properties:
                print(f"Adding to search: Property {new_property}, Item {new_item}, Count {count}")
                new_depth = depth + 1
                child_result = search_distributor(new_item, new_property, data_files, num_procs, max_depth, min_group_size, max_group_size,
                                                  depth=new_depth, seen_items=seen_items, seen_properties=seen_properties, chain=current_chain,
                                                  valid_qids=valid_qids, filtered_data=filtered_data)
                if child_result:
                    result["children"][f"{new_property}, {new_item}"] = child_result

    # No need to pop from the chain here

    return result

def convert_to_json_format(result):
    def process_node(node):
        chain = node["chain"]
        current_key = ", ".join([f"{p}, {q}" for q, p in chain])
        json_data = {current_key: {}}

        for property_id, items in node["results"].items():
            json_data[current_key][property_id] = {}
            for item, count in items.items():
                json_data[current_key][property_id][item] = {
                    "count": count,
                    "items": node["item_groups"].get(property_id, {}).get(item, [])[:]  # Limit to first 10 items
                }

        for child_key, child_node in node["children"].items():
            json_data[current_key][child_key] = process_node(child_node)

        return json_data

    return process_node(result)

def save_json_results(results, output_file):
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

def main():
    parser = get_arg_parser()
    parser.add_argument('--output', type=str, required=True, help='Output JSON file path')
    args = parser.parse_args()
    data_files = get_batch_files(args.data)
    if args.test:
        data_files = data_files[:50]
    
    result = search_distributor(args.item, args.property, data_files, args.num_procs, max_depth=args.max_depth, min_group_size=args.min_group_size, max_group_size=args.min_group_size*2)
    
    if result:
        json_results = convert_to_json_format(result)
        save_json_results(json_results, args.output)
        print(f"Results saved to {args.output}")
    else:
        print("No results found.")

if __name__ == "__main__":
    main()