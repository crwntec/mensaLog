"""
Meal Admin CLI
"""
import sys
from app.services.meal_intelligence import MealIntelligence

def command_index():
    """Build the embeddings index"""
    intel = MealIntelligence()
    intel.build_embeddings_index
    print("\n✓ Index built successfully")

def command_find_dupes(threshold=0.92):
    """Find and display duplicates"""
    intel = MealIntelligence()
    intel.build_embeddings_index()
    
    duplicates = intel.find_duplicates(threshold=threshold)
    
    print(f"\nFound {len(duplicates)} duplicate pairs (threshold={threshold})\n")
    
    for i, (id1, id2, score) in enumerate(duplicates[:20], 1):
        name1 = intel.get_meal_name(id1)
        name2 = intel.get_meal_name(id2)
        print(f"{i}. Similarity: {score:.3f}")
        print(f"   {name1}")
        print(f"   {name2}\n")
    
    if len(duplicates) > 20:
        print(f"... and {len(duplicates) - 20} more pairs")

    for i, (id1, id2, score) in enumerate(duplicates[-20:], 1):
        name1 = intel.get_meal_name(id1)
        name2 = intel.get_meal_name(id2)
        print(f"{i}. Similarity: {score:.3f}")
        print(f"   {name1}")
        print(f"   {name2}\n")
    
    # LEARNING TASK: Add statistics
    # - Average similarity
    # - Distribution of similarity scores
    # - Most duplicated meal (appears in most pairs)

def command_search(query: str):
    """Search for similar meals"""
    intel = MealIntelligence()
    intel.build_embeddings_index()
    
    # Encode query
    query_embedding = intel.encode_meal(query)
    
    # Find similar
    results = []
    for meal_id, embedding in intel.meal_embeddings.items():
        similarity = intel.compute_similarity(query_embedding, embedding)
        name = intel.get_meal_name(meal_id)
        results.append((meal_id, name, similarity))
    
    results.sort(key=lambda x: x[2], reverse=True)
    
    print(f"\nTop 10 results for: '{query}'\n")
    for i, (meal_id, name, score) in enumerate(results[:10], 1):
        print(f"{i}. [{score:.3f}] {name}")

def main():
    if len(sys.argv) < 2:
        print("""
Usage: python meal_admin.py <command> [args]

Commands:
  index                  - Build embeddings index
  find-dupes [threshold] - Find duplicates (default threshold=0.92)
  search <query>         - Search for similar meals

Examples:
  python meal_admin.py index
  python meal_admin.py find-dupes 0.90
  python meal_admin.py search "Hähnchen mit Reis"
        """)
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == 'index':
        command_index()
    elif command == 'find-dupes':
        threshold = float(sys.argv[2]) if len(sys.argv) > 2 else 0.92
        command_find_dupes(threshold)
    elif command == 'search':
        if len(sys.argv) < 3:
            print("Error: search requires a query")
            sys.exit(1)
        query = ' '.join(sys.argv[2:])
        command_search(query)
    elif command == 'merge':
        apply = '--apply' in sys.argv
        intel = MealIntelligence()
        intel.build_embeddings_index()
        intel.merge_duplicates(threshold=0.9, dry_run=not apply)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()