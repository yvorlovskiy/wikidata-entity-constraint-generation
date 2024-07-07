import pytest
import json
from typing import List, Dict, Any

def load_json(file_path: str) -> Dict[str, Any]:
    with open(file_path, 'r') as f:
        return json.load(f)

def find_query_results(data: Dict[str, Any], query_key: str) -> List[str]:
    if isinstance(data, dict):
        for key, value in data.items():
            if key == query_key and isinstance(value, dict) and 'items' in value:
                return value['items']
            result = find_query_results(value, query_key)
            if result:
                return result
    elif isinstance(data, list):
        for item in data:
            result = find_query_results(item, query_key)
            if result:
                return result
    return []

def calculate_accuracy(expected: List[str], actual: List[str]) -> float:
    expected_set = set(expected)
    actual_set = set(actual)
    correct = len(expected_set.intersection(actual_set))
    total = len(expected_set)
    return correct / total if total > 0 else 0

def get_false_positives_negatives(expected: List[str], actual: List[str]) -> tuple:
    expected_set = set(expected)
    actual_set = set(actual)
    false_positives = actual_set - expected_set
    false_negatives = expected_set - actual_set
    return false_positives, false_negatives

def match_query_key(query_name: str) -> str:
    query_mappings = {
        "FRENCH_SPEAKING_COUNTRIES": "[instance of], [country], [official language], [French]",
        "SPANISH_SPEAKING_COUNTRIES": "[instance of], [country], [official language], [Spanish]",
        "ARABIC_SPEAKING_COUNTRIES": "[instance of], [country], [official language], [Arabic]",
        "OECD_MEMBER_COUNTRIES": "[instance of], [country], [member of], [Organization for Economic Cooperation and Development]",
        "EU_BORDERING_COUNTRIES": "[instance of], [country], [shares border with], [European Union]",
        "APEC_MEMBER_COUNTRIES": "[instance of], [country], [member of], [Asia-Pacific Economic Cooperation]"
    }
    return query_mappings.get(query_name, "")

# Load ground truth data
GROUND_TRUTH = load_json('recursive_search_ground_truth.json')

@pytest.mark.parametrize("query_name", [
    "FRENCH_SPEAKING_COUNTRIES",
    "OECD_MEMBER_COUNTRIES",
    "APEC_MEMBER_COUNTRIES",
    "EU_BORDERING_COUNTRIES",
    "SPANISH_SPEAKING_COUNTRIES",
    "ARABIC_SPEAKING_COUNTRIES",
])
def test_query_results(query_name: str):
    # Load results from file
    results = load_json('../out/out_instance_country_decoded.json')
    
    # Match the query key
    query_key = match_query_key(query_name)
    
    # Get actual results
    actual_results = find_query_results(results, query_key)
    
    # Get expected results
    expected_results = GROUND_TRUTH[query_name]
    
    # Calculate accuracy
    accuracy = calculate_accuracy(expected_results, actual_results)
    
    # Get false positives and negatives
    false_positives, false_negatives = get_false_positives_negatives(expected_results, actual_results)
    
    # Print false positives and negatives
    print(f"\nResults for {query_name}:")
    print(f"False Positives: {', '.join(false_positives) if false_positives else 'None'}")
    print(f"False Negatives: {', '.join(false_negatives) if false_negatives else 'None'}")
    
    # Assert that accuracy is at least 85%
    assert accuracy >= 0.85, f"Accuracy for {query_name} is {accuracy:.2%}, which is below the 85% threshold"