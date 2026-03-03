from pathlib import Path

import polars as pl

df = pl.read_csv(Path(r"project1\dataset.csv"))

# Get the top 5 artists by unique genres
top_5_most_varied_artists = (
    df.group_by("artists")
    .agg(pl.col("track_genre").n_unique().alias("distinct_count"))
    .sort(by="distinct_count", descending=True)
    .select("artists")
    .to_series()
    .head(5)
)

df_top_5 = df.filter(pl.col("artists").is_in(top_5_most_varied_artists))

# --- Network graph data ---
# Each unique artist, track_name, and track_genre becomes its own node.
# Edges connect: artist <-> track_name, and track_name <-> track_genre.

# Nodes: one row per unique node with a type label
artist_nodes = (
    df_top_5.select(pl.col("artists").alias("id"))
    .unique()
    .with_columns(pl.lit("artist").alias("type"))
)

track_nodes = (
    df_top_5.select(pl.col("track_name").alias("id"))
    .unique()
    .with_columns(pl.lit("track").alias("type"))
)

genre_nodes = (
    df_top_5.select(pl.col("track_genre").alias("id"))
    .unique()
    .with_columns(pl.lit("genre").alias("type"))
)

nodes = pl.concat([artist_nodes, track_nodes, genre_nodes])

# Edges: artist -> track, track -> genre (deduplicated)
artist_track_edges = (
    df_top_5.select(
        pl.col("artists").alias("source"),
        pl.col("track_name").alias("target"),
        pl.lit("artist_track").alias("relationship"),
    )
    .unique()
)

track_genre_edges = (
    df_top_5.select(
        pl.col("track_name").alias("source"),
        pl.col("track_genre").alias("target"),
        pl.lit("track_genre").alias("relationship"),
    )
    .unique()
)

edges = pl.concat([artist_track_edges, track_genre_edges])

# --- Save processed data ---
output_dir = Path(r"project1\processed")
output_dir.mkdir(exist_ok=True)

nodes.write_csv(output_dir / "nodes.csv")
edges.write_csv(output_dir / "edges.csv")
