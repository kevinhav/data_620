# Spotify Artist Collaboration Network Analysis

## Project Overview

For our first project, we will analyze a Spotify artist collaboration dataset sourced from Kaggle (https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset), which contains artist names, genre classifications, collaboration relationships, and popularity metrics.

Using this dataset, we will construct an artist collaboration network to examine structural influence within the music industry. The network will allow us to explore how artists are embedded within collaborative structures and whether their structural positions differ across musical genres.

## Data Loading and Preparation Plan

The dataset will be downloaded from Kaggle and loaded into Python using the Pandas library.

The following steps will be taken to prepare the data for network construction:

1. Import the dataset into a Pandas DataFrame.
2. Clean and standardize artist names to ensure consistency.
3. Extract collaboration relationships by identifying tracks with multiple credited artists.
4. Filter the dataset to include the top artists by collaboration frequency.
5. Restrict the analysis to the top 5 most represented genres to ensure balanced comparison.
6. Construct an edge list representing artist collaborations.
7. Use NetworkX to build the artist collaboration graph.

## Network Structure

In this network, each **node** represents an artist, and an **edge** exists between two artists if they appear on the same track. Each artist node will include a categorical attribute representing their **primary genre**.

We will use this network to calculate the following centrality measures for each artist:

- **Degree Centrality** — measures the number of direct collaborators an artist has, capturing collaboration volume.
- **Eigenvector Centrality** — measures structural influence by accounting for both the number and importance of an artist’s collaborators.

By comparing these centrality measures across genre groups, we aim to better understand how collaboration patterns vary across musical genres and whether certain genres occupy more central or influential positions within the broader collaboration network.

## Research Questions

1. Do artists from different genres differ in **degree centrality** (number of collaborators)?
2. Do artists from different genres differ in **eigenvector centrality** (influence within the collaboration network)?
3. Are differences in degree and eigenvector centrality across musical genres **statistically significant**?
4. Do certain genres have more **extreme high-centrality artists** than others?
5. Does the **relationship between centrality and popularity** differ by genre?

## Predictive Hypothesis

In addition to comparing centrality across genres, we will explore a hypothetical predictive outcome. Specifically, we hypothesize that:

> Artists with higher degree and eigenvector centrality will have higher Spotify popularity scores, as increased collaboration and network influence may expand audience reach.

This allows us to examine whether **structural position within the collaboration network** is associated with measurable success outcomes. 
If certain genres exhibit significantly higher degree centrality, this may indicate that those genres rely more heavily on collaborative production structures, potentially contributing to broader audience reach and commercial success.
