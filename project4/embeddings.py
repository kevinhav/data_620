import gensim.downloader as api
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.figure import Figure
from sklearn.metrics.pairwise import cosine_similarity
from itertools import combinations
import pandas as pd

# Will need to download and cache on first run
GLOVE = api.load("glove-wiki-gigaword-300")


class Embedder:
    def __init__(self, words: list[str]):
        self.words = words
        self.glove = GLOVE
        self.embeddings: np.ndarray | None = None
        self.similarity_matrix: np.ndarray | None = None
        self.sample_puzzle: list[str] = [
            "club",
            "diamond",
            "heart",
            "spade",
            "sandwich",
            "knuckle",
            "open",
            "ham",
            "ace",
            "king",
            "queen",
            "jack",
            "flush",
            "straight",
            "full",
            "royal",
        ]
    
    def get_glove_embeddings(self) -> np.ndarray:
        "Load GloVe word embeddings"

        vectors = []

        for word in self.words:
            w = word.lower().replace(" ", "_")
            if w in self.glove:
                vectors.append(self.glove[w])
            else:
                vectors.append(np.zeros(300))  # GLoVE vectors are 300

        self.embeddings = np.array(vectors)

        return self.embeddings

    def get_cosine_similarity(self) -> np.ndarray:
        if self.embeddings is None:
            raise ValueError("No embeddings found")

        if isinstance(self.embeddings, np.ndarray):
            self.similarity_matrix = cosine_similarity(self.embeddings)
            return self.similarity_matrix
        else:
            raise ValueError(
                "Could not compute similarity matrix with current embeddings"
            )
            
    def plot_similarity_matrix(self) -> Figure:
        if self.similarity_matrix is None:
            raise ValueError("No similarity matrix found")

        n = len(self.words)
        fig, ax = plt.subplots(figsize=(max(6, n * 0.6), max(5, n * 0.6)))
        sns.heatmap(
            self.similarity_matrix,
            xticklabels=self.words,
            yticklabels=self.words,
            vmin=-1,
            vmax=1,
            center=0,
            cmap="coolwarm",
            annot=n <= 20,
            fmt=".2f",
            square=True,
            linewidths=0.5,
            ax=ax,
        )
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right", fontsize=9)
        ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=9)
        ax.set_title("Cosine Similarity Matrix")
        fig.tight_layout()
        return fig

    def demo(self):
        self.words = self.sample_puzzle

        print(f"Words: {self.words}")
        print(f"\nLoading GloVe embeddings for {len(self.words)} words...")
        self.get_glove_embeddings()
        assert self.embeddings is not None
        print(f"Embeddings shape: {self.embeddings.shape}")

        print("\nComputing cosine similarity matrix...")
        self.get_cosine_similarity()
        print("Similarity matrix:")
        print(self.similarity_matrix)

        print("\nPlotting similarity matrix...")
        self.plot_similarity_matrix()
        plt.show()


if __name__ == "__main__":
    Embedder(words=[]).demo()

def mean_pairwise_similarity(indices, similarity_matrix):
        """
        Computes mean cosine similarity across all pairs in a group.
        """
        pair_scores = []
    
        for i, j in combinations(indices, 2):
            pair_scores.append(similarity_matrix[i, j])
    
        return np.mean(pair_scores)
    
def rank_word_combinations(words, similarity_matrix):
        """
        Scores every possible 4-word combination.
        """
        rows = []
    
        for combo_indices in combinations(range(len(words)), 4):
            combo_words = tuple(words[i] for i in combo_indices)
    
            score = mean_pairwise_similarity(
                combo_indices,
                similarity_matrix
            )
    
            rows.append({
                "combo_words": combo_words,
                "combo_indices": combo_indices,
                "mean_cosine_similarity": score
            })
    
        ranked_df = pd.DataFrame(rows)
    
        ranked_df = ranked_df.sort_values(
            by="mean_cosine_similarity",
            ascending=False
        ).reset_index(drop=True)
    
        return ranked_df
    