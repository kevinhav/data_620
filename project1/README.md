# Spotify Artist Collaboration Network Analysis

## Project Overview

For our first project, we are going to extract genre and artist data from the Spotify API. We will then use this data to create a network of artists and genres to analyze overlap, centrality, and breadth of the network to better understand how artists move between and across genres.

## Network Structure

In this network, each **node** represents an artist, and an **edge** exists between two artists if they appear on the same track. Each artist node will include a categorical attribute representing their **primary genre**.

We will use this network to calculate the following centrality measures for each artist:

- **Degree Centrality** — measures differences in connectivity across genres
- **Eigenvector Centrality** — measures influence within the collaboration network

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
