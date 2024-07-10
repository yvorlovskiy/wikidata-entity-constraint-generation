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
    parser.add_argument('--num_procs', type=int, default=35, help='Number of processes')
    parser.add_argument('--qid_min', type=int, default=100, help='Minimum Qid number')
    parser.add_argument('--qid_max', type=int, default=1000, help='Maximum Qid number')
    parser.add_argument('--top_n', type=int, default=20, help='Number of top entities to display')
    return parser

def filtering_func(args, filename):
    property_id, qid_min, qid_max = args
    filtered = Counter()
    try:
        for item in jsonl_generator(filename):
            if not isinstance(item, dict):
                continue
            if item.get('property_id') == property_id:
                qid = item.get('qid')
                if qid and qid_min <= int(qid[1:]) <= qid_max:
                    filtered[item.get('value')] += 1
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
    return filtered

def main():
    args = get_arg_parser().parse_args()
    table_files = get_batch_files(args.data)
    
    pool = Pool(processes=args.num_procs)
    filter_args = (args.property, args.qid_min, args.qid_max)
    
    results = Counter()
    for partial_result in tqdm(
        pool.imap_unordered(
            partial(filtering_func, filter_args), table_files, chunksize=1),
        total=len(table_files)
    ):
        results.update(partial_result)
    
    pool.close()
    pool.join()

    print(f"Top {args.top_n} entities by count (Qids range: {args.qid_min} - {args.qid_max}):")
    for entity, count in results.most_common(args.top_n):
        print(f"Entity: {entity}, Count: {count}")

if __name__ == "__main__":
    main()