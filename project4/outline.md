## General Components

### TODOs
  
#### Kevin

#### Aali


### Main Algo ("Play the game")
1. Encode all words
2. Rank all combinations
3. "test" against ground truth
4. If not a matching category,
   1. Penalize the similarity coefficients
   2. Recompute best categories
   3. Submit again
5. If matching category,
   1. Remove words from pool and recompute best grouping
6. Rinse & Repeat for next two
7. If 3 are correct, consider a wing


### Evaluation
1. Random grouping of words (baseline)
2. Naive cosine similarity (group four highest by cosine, then next four, etc.)
3. Simple classifier trained on existing data
4. Full algo approach
5. Measure by categories correct, puzzles correct, number of attempts

## Ideas

- Features
  - Embeddings
    - GLove (Semantic) + BERT (Context)?
- Modeling: Classification or Clustering?
  - Classification: Train on existing data
  - Clustering: Use only the immediate puzzle
- Network - useful or not really?
- 