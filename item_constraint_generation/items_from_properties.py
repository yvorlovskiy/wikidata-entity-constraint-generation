import argparse
from tqdm import tqdm
from multiprocessing import Pool
from functools import partial
from collections import Counter
from utils import jsonl_generator, get_batch_files

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type=str, default='/data/yury/wikidata/entity_rels', help='path to output directory')
    parser.add_argument('--property', type=str, default='P413', help='property id')
    parser.add_argument('--num_procs', type=int, default=25, help='Number of processes')
    parser.add_argument('--qid_min', type=int, default=100, help='Minimum Qid number')
    parser.add_argument('--qid_max', type=int, default=1000, help='Maximum Qid number')
    parser.add_argument('--top_n', type=int, default=20, help='Number of top entities to display')
    return parser

def is_valid_qid(qid, qid_min, qid_max):
    if qid.startswith('Q'):
        try:
            num = int(qid[1:])
            return qid_min <= num <= qid_max
        except ValueError:
            return False
    return False

def filtering_func(args, filename):
    filtered = Counter()
    try:
        for item in jsonl_generator(filename):
            if isinstance(item, dict) and item.get('property_id') == args.property:
                value = item.get('value')
                if value and is_valid_qid(value, args.qid_min, args.qid_max):
                    filtered[value] += 1
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
    return filtered

def main():
    args = get_arg_parser().parse_args()
    table_files = get_batch_files(args.data)
    
    with Pool(processes=args.num_procs) as pool:
        results = Counter()
        for partial_result in tqdm(
            pool.imap_unordered(partial(filtering_func, args), table_files, chunksize=1),
            total=len(table_files)
        ):
            results.update(partial_result)

    print(f"Top {args.top_n} entities by count (Qids range: {args.qid_min} - {args.qid_max}):")
    for entity, count in results.most_common(args.top_n):
        print(f"Entity: {entity}, Count: {count}")

if __name__ == "__main__":
    main()