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
)  # .to_list()

df_top_5 = df.filter(pl.col("artists").is_in(top_5_most_varied_artists))
