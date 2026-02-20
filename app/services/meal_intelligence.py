import numpy as np
from sentence_transformers import SentenceTransformer
import sqlite3
import pickle
import os
PROTEIN_GROUPS = [
    ('rind', 'rindfleisch'),
    ('schwein', 'schweinefleisch'),
    ('hähnchen', 'huhn', 'geflügel', 'pute'),
    ('lamm',),
    ('fisch', 'lachs', 'seelachs', 'dorsch', 'hoki'),
]
SKIP_PAIRS = {
    "Chili sin Carne  mit Mais, Bohnen dazu Baguette",
    "Gemüsebratling mit Currysauce dazu Reis",
    "Gnocchi a1.c mit tomatisiertem Pfannengemüse und K",  # different from Schupfnudeln
    "Fischfrikadelle mit Joghurtremoulade dazu Kartoffe",  # different from Fischfilet in Backteig
}
class MealIntelligence:
    def __init__(self, db_path: str = 'mealplan.db', cache_path: str = './cache'):
        self.db_path = db_path
        self.cache_path = cache_path
        self.cache_file = os.path.join(cache_path, 'embeddings.pkl')

        os.makedirs(cache_path, exist_ok=True)

        print("Loading embedding model...")
        self.model = SentenceTransformer("T-Systems-onsite/cross-en-de-roberta-sentence-transformer")
        print("Model loaded!")
        self.meal_embeddings = {}
    
    def save_cache(self):
        """Save embeddings to disk"""
        with open(self.cache_file, 'wb') as f:
            pickle.dump(self.meal_embeddings, f)
        print(f"✓ Cached {len(self.meal_embeddings)} embeddings")
    
    def load_cache(self):
        """Load embeddings from disk if available"""
        if os.path.exists(self.cache_file):
            with open(self.cache_file,'rb') as f:
                self.meal_embeddings = pickle.load(f)
                print(f"✓ Loaded {len(self.meal_embeddings)} cached embeddings")
            return True
        return False
    
    def get_meal_name(self, meal_id: int) -> str:
        """Helper to get meal name from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM meal WHERE id = ?", (meal_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def encode_meal(self, meal_name:str) -> np.ndarray:
        """Calculate cosine similarity between two embeddings."""
        embedding = self.model.encode([meal_name], convert_to_numpy=True, normalize_embeddings=True)
        return embedding[0]
    
    def compute_similarity(self, emb1: np.ndarray, emb2: np.ndarray) -> float:
        return float(np.dot(emb1,emb2)/(np.linalg.norm(emb1)*np.linalg.norm(emb2)))
    
    def build_embeddings_index(self, force_rebuild: bool = False):
        """
        Encode all meals from database and store embeddings.
        """
        if not force_rebuild and self.load_cache():
            print("Using cached embeddings")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM meal")
            all_ids = {row[0] for row in cursor.fetchall()}
            conn.close()

            cached_ids = set(self.meal_embeddings.keys())
            new_ids = all_ids - cached_ids

            if new_ids:
                print(f"Found {len(new_ids)} new meals to encode")
                self._encode_and_store_meals(new_ids)
                print(f"✓ Indexed all {len(self.meal_embeddings)} meals")
                self.save_cache()
            else:
                print("Cache is up to date")
                return
        else:
            print("Rebuilding embeddings...")
            all_ids = self._get_all_meal_ids()
            self._encode_and_store_meals(all_ids)
            print(f"✓ Indexed all {len(self.meal_embeddings)} meals")
            self.save_cache()

    def _get_all_meal_ids(self) -> set:
        """Get all meal IDs from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM meal")
        all_ids = {row[0] for row in cursor.fetchall()}
        conn.close()
        return all_ids

    def _encode_and_store_meals(self, meal_ids):
        """Encode meals and store embeddings"""
        meal_names = [self.get_meal_name(i) for i in meal_ids]
        embeddings = self.model.encode(
            meal_names,
            batch_size=32,
            show_progress_bar=True,
            convert_to_numpy=True,
            normalize_embeddings=True
        )
        for meal_id, embedding in zip(meal_ids, embeddings):
            self.meal_embeddings[meal_id] = embedding
    def find_duplicates(self, threshold: float=0.792):
        duplicates = []
        meal_ids = list(self.meal_embeddings.keys())
        print(f"Comparing {len(meal_ids)} meals...")
    
        # Compare each meal with every other meal
        total_comparisons = len(meal_ids) * (len(meal_ids) - 1) // 2
        print(f"Total comparisons needed: {total_comparisons}")
    
        compared = 0
        for i in range(len(meal_ids)):
            for j in range(i + 1, len(meal_ids)):
                id1, id2 = meal_ids[i], meal_ids[j]
                emb1 = self.meal_embeddings[id1]
                emb2 = self.meal_embeddings[id2]
                
                similarity = self.compute_similarity(emb1, emb2)
                
                if similarity >= threshold:
                    duplicates.append((id1, id2, similarity))
                
                compared += 1
                
                # Progress indicator
                if compared % 10000 == 0:
                    print(f"  Progress: {compared}/{total_comparisons}")
        
        duplicates.sort(key=lambda x: x[2], reverse=True)
    
        print(f"✓ Found {len(duplicates)} duplicate pairs")
        return duplicates
    

    def _get_protein(self, text: str):
        t = text.lower()
        for i, group in enumerate(PROTEIN_GROUPS):
            if any(k in t for k in group):
                return i
        return None
    def merge_duplicates(self, threshold: float = 0.792, dry_run: bool = True):
        duplicates = self.find_duplicates(threshold=threshold)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        merged = 0

        for id1, id2, score in duplicates:
            name1 = self.get_meal_name(id1)
            name2 = self.get_meal_name(id2)

            # Apply protein guard - skip false positives
            if (self._get_protein(name1) is not None and
                self._get_protein(name2) is not None and
                self._get_protein(name1) != self._get_protein(name2)):
                print(f"  SKIP (protein mismatch): {name1[:50]} | {name2[:50]}")
                continue

            # Check if one name is in skip list
            if name1 in SKIP_PAIRS or name2 in SKIP_PAIRS:
                print(f"  SKIP (skip list): {name1[:50]} | {name2[:50]}")
                continue

            # Keep the longer/more descriptive name as canonical
            canonical_id, duplicate_id = (id1, id2) if len(name1) >= len(name2) else (id2, id1)
            canonical_name = self.get_meal_name(canonical_id)
            duplicate_name = self.get_meal_name(duplicate_id)

            print(f"  {'[DRY]' if dry_run else 'MERGE'} ({score:.3f}): {duplicate_name[:50]} -> {canonical_name[:50]}")

            if not dry_run:
                for col in ['tagesgericht_id', 'vegetarisch_id', 'pizza_pasta_id', 'wok_id']:
                    cursor.execute(f"UPDATE day SET {col} = ? WHERE {col} = ?", (canonical_id, duplicate_id))
                cursor.execute("DELETE FROM meal WHERE id = ?", (duplicate_id,))
                if duplicate_id in self.meal_embeddings:
                    del self.meal_embeddings[duplicate_id]
                merged += 1

        if not dry_run:
            conn.commit()
            self.save_cache()
            print(f"\n✓ Merged {merged} duplicates")
        else:
            print(f"\n[DRY RUN] Would merge {merged} pairs")

        conn.close()
    
    def find_similar_meal(self, meal_name, threshold: float = 0.78):
        if not self.meal_embeddings:
            return None, 0
        
        new_embedding = self.encode_meal(meal_name)

        best_match = None
        best_score = 0

        for meal_id, existing_embedding in self.meal_embeddings.items():
            score = self.compute_similarity(new_embedding, existing_embedding)
            if score > best_score:
                best_score = score
                best_match = meal_id

        if best_score >= threshold:
            return best_match, best_score
        
        return None, best_score
    
    def find_top_similar_meals(self, meal_name: str, top_k: int = 10, threshold: float = 0.5) -> list:
        if not self.meal_embeddings:
            print("[MealIntelligence] No meal embeddings loaded")
            return []
        
        query_embedding = self.encode_meal(meal_name)
        
        scores = [
            (meal_id, self.compute_similarity(query_embedding, emb))
            for meal_id, emb in self.meal_embeddings.items()
        ]
        
        scores.sort(key=lambda x: x[1], reverse=True)
        # print(threshold, scores)
        return [(meal_id, score) for meal_id, score in scores[:top_k] if score >= threshold]
        # return [(meal_id, score) for meal_id, score in scores[:top_k]]