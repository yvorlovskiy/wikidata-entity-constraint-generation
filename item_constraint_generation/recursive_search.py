# example:
#  python3 item_constraint_generation/recursive_search.py --initial_conditions "[(\'Q6256\', \'P31\')]" --output item_constraint_generation/out/out_instance_country.json --test --blacklist item_constraint_generation/blacklist.json 
import argparse
from collections import defaultdict, Counter
from functools import partial
from multiprocessing import Pool
from tqdm import tqdm
from utils import jsonl_generator, get_batch_files
import json
import ast

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type=str, default='/data/yury/wikidata/entity_rels', help='path to output directory')
    parser.add_argument('--initial_conditions', type=str, required=True, help='Initial conditions as a string representation of a list of tuples, e.g., "[(\'Q6256\', \'P31\')]"')
    parser.add_argument('--num_procs', type=int, default=50, help='Number of processes')
    parser.add_argument('--max_depth', type=int, default=3, help='Maximum recursive depth')
    parser.add_argument('--min_group_size', type=int, default=20, help='Minimum group size to consider')
    parser.add_argument('--test', action='store_true', help='Run in test mode (process only first 50 files)')
    parser.add_argument('--blacklist', type=str, required=False, help='Path to JSON file containing blacklisted properties and items')
    parser.add_argument('--output', type=str, required=True, help='Output JSON file path')
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

def load_blacklist(blacklist_file):
    with open(blacklist_file, 'r') as f:
        blacklist = json.load(f)
    return set(blacklist.get('properties', [])), set(blacklist.get('items', []))

def find_qids(initial_conditions, filename, valid_qids):
    qid_dict = {}
    try:
        for entry in jsonl_generator(filename):
            if not isinstance(entry, dict):
                print(f"Expected dict, but got {type(entry)}: {entry}")
                continue
            
            qid = entry.get('qid')
            if valid_qids and qid not in valid_qids:
                continue
            
            prop = entry.get('property_id')
            value = entry.get('value')
            
            if qid not in qid_dict:
                qid_dict[qid] = set()
            qid_dict[qid].add((prop, value))
    
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
        return []

    result_qids = set(qid_dict.keys())
    for item, prop in initial_conditions:
        result_qids &= {qid for qid, prop_values in qid_dict.items() if (prop, item) in prop_values}
        if not result_qids:
            return []

    return [(qid, initial_conditions[-1][1], initial_conditions[-1][0]) for qid in result_qids]

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

def next_q_p(item, property_id, data_files, filtered_data, num_procs, initial_conditions=None, valid_qids=None, seen_properties=None, blacklisted_properties=None, seen_items=None, blacklisted_items=None, min_group_size=20):
    seen_properties = seen_properties or set()
    seen_items = seen_items or set()
    blacklisted_properties = blacklisted_properties or set()
    blacklisted_items = blacklisted_items or set()

    if valid_qids is None:
        print("First pass: Collecting triples")
        pool = Pool(processes=num_procs)
        triple_results = list(tqdm(
            pool.imap_unordered(
                partial(find_qids, initial_conditions, valid_qids=None),
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
        if (entry.get('qid') in valid_qids and
            entry.get('property_id') not in seen_properties and
            entry.get('property_id') not in blacklisted_properties and
            entry.get('value') not in seen_items and
            entry.get('value') not in blacklisted_items and
            (entry.get('value'), entry.get('property_id')) not in seen_items):
            property_bank[entry.get('property_id')][entry.get('value')] += 1
            item_groups[entry.get('property_id')][entry.get('value')].append(entry.get('qid'))
    
    print("Filtering out seen and blacklisted items...")
    filtered_results = property_item_counts(property_bank, seen_items.union(blacklisted_items))
    return filtered_results, valid_qids, item_groups, filtered_data

def search_distributor(initial_conditions, data_files, num_procs, max_depth, min_group_size, max_group_size, blacklisted_items=None, blacklisted_properties=None, depth=0, seen_items=None, seen_properties=None, chain=None, valid_qids=None, filtered_data=None):
    if depth >= max_depth:
        return None

    seen_items = seen_items or set()
    seen_properties = seen_properties or set()
    blacklisted_items = blacklisted_items or set()
    blacklisted_properties = blacklisted_properties or set()
    chain = chain or []

    if depth == 0:
        # First pass: use initial_conditions
        item, property_id = initial_conditions[-1]  # Use the last condition for the first pass
    else:
        # Subsequent passes: use regular item-property pairs
        item, property_id = initial_conditions

    # Convert the chain to a tuple of tuples for hashability
    chain_key = tuple((str(i), str(p)) for i, p in chain + [(item, property_id)])
    if chain_key in seen_items or item in blacklisted_items or property_id in blacklisted_properties:
        return None

    print(f'Chain key {chain_key}')

    # Add current item and property to the chain
    current_chain = chain + [[item, property_id]]

    # Add current chain to the seen sets
    seen_items.add(chain_key)
    seen_properties.add(property_id)

    if depth == 0:
        current_results, new_valid_qids, item_groups, filtered_data = next_q_p(item, property_id, data_files, filtered_data, num_procs, initial_conditions=initial_conditions, seen_properties=seen_properties, blacklisted_properties=blacklisted_properties, seen_items=seen_items, blacklisted_items=blacklisted_items, valid_qids=valid_qids)
    else:
        current_results, new_valid_qids, item_groups, filtered_data = next_q_p(item, property_id, data_files, filtered_data, num_procs, seen_properties=seen_properties, blacklisted_properties=blacklisted_properties, seen_items=seen_items, blacklisted_items=blacklisted_items, valid_qids=valid_qids)

    if valid_qids is None:
        valid_qids = new_valid_qids
    else:
        valid_qids = valid_qids.intersection(new_valid_qids)

    in_range_results = filter_results_by_count(current_results, min_group_size, max_group_size)
    over_results = filter_results_by_count(current_results, min_group_size=min_group_size * 2)

    print(f'Current depth: {depth}')
    print(current_chain)
    print(in_range_results)

    result = {
        "chain": current_chain,
        "results": in_range_results,
        "item_groups": item_groups,
        "children": {}
    }

    for new_property, new_items in over_results.items():
        for new_item, count in new_items.items():
            new_chain_key = tuple((str(i), str(p)) for i, p in current_chain + [(new_item, new_property)])
            if (new_chain_key not in seen_items and
                new_property not in seen_properties and
                new_item not in blacklisted_items and
                new_property not in blacklisted_properties):
                print(f"Adding to search: Property {new_property}, Item {new_item}, Count {count}")
                new_depth = depth + 1
                
                # Filter valid_qids to only those that satisfy the entire chain
                chain_valid_qids = valid_qids
                for chain_item, chain_property in current_chain + [(new_item, new_property)]:
                    chain_valid_qids = {qid for qid in chain_valid_qids if any(
                        entry.get('qid') == qid and entry.get('property_id') == chain_property and entry.get('value') == chain_item
                        for entry in filtered_data
                    )}
                
                if not chain_valid_qids:
                    continue  # Skip this branch if no QIDs satisfy the entire chain
                
                child_result = search_distributor((new_item, new_property), data_files, num_procs, max_depth, 
                                                  min_group_size, max_group_size, blacklisted_items, 
                                                  blacklisted_properties, depth=new_depth, seen_items=seen_items, 
                                                  seen_properties=seen_properties, chain=current_chain,
                                                  valid_qids=chain_valid_qids, filtered_data=filtered_data)
                if child_result:
                    result["children"][f"{new_property}, {new_item}"] = child_result

    return result

def convert_to_json_format(result):
    def format_chain(chain):
        formatted_chain = []
        for pair in chain:
            if isinstance(pair, list) and len(pair) == 2:
                formatted_chain.extend([f"[{pair[1]}]", f"[{pair[0]}]"])
            elif isinstance(pair, str):
                formatted_chain.append(f"[{pair}]")
        return ", ".join(formatted_chain)

    def process_node(node):
        json_data = {}
        chain = node.get('chain', [])
        
        path_string = format_chain(chain)
        json_data[path_string] = {}
        
        for property_id, items in node['results'].items():
            property_path = f"{path_string}, [{property_id}]"
            json_data[path_string][property_path] = {}
            
            for item, count in items.items():
                item_path = f"{property_path}, [{item}]"
                json_data[path_string][property_path][item_path] = {
                    "count": count,
                    "items": node["item_groups"].get(property_id, {}).get(item, [])[:] # Limit to first 10 items
                }
        
        for child_key, child_node in node['children'].items():
            child_data = process_node(child_node)
            json_data[path_string].update(child_data)
        
        return json_data
    
    return process_node(result)

def save_json_results(results, output_file):
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

def main():
    parser = get_arg_parser()
    args = parser.parse_args()
    blacklisted_properties, blacklisted_items = load_blacklist(args.blacklist) if args.blacklist else (set(), set())

    data_files = get_batch_files(args.data)
    if args.test:
        data_files = data_files[:50]
    
    initial_conditions = ast.literal_eval(args.initial_conditions)
    # Convert the list of dictionaries to a list of tuples
    initial_conditions = [(condition['item'], condition['property']) for condition in initial_conditions]
    
    result = search_distributor(initial_conditions, data_files, args.num_procs, max_depth=args.max_depth, 
                                min_group_size=args.min_group_size, max_group_size=args.min_group_size*2,
                                blacklisted_items=blacklisted_items, blacklisted_properties=blacklisted_properties)
    
    if result:
        json_results = convert_to_json_format(result)
        save_json_results(json_results, args.output)
        print(f"Results saved to {args.output}")
    else:
        print("No results found.")

if __name__ == "__main__":
    main()