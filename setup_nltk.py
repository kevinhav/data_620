"""Download required NLTK data packages for DATA 620."""

import nltk


def main() -> None:
    packages = [
        "punkt",
        "punkt_tab",
        "stopwords",
        "wordnet",
        "averaged_perceptron_tagger",
        "averaged_perceptron_tagger_eng",
        "maxent_ne_chunker",
        "maxent_ne_chunker_tab",
        "words",
        "vader_lexicon",
        "book",
    ]
    for package in packages:
        nltk.download(package)


if __name__ == "__main__":
    main()
