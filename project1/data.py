from pathlib import Path

import polars as pl

GENRE_MAP = {
    "acoustic": "Acoustic",
    "afrobeat": "R&B",
    "alt-rock": "Alternative",
    "alternative": "Alternative",
    "ambient": "House / Electronic",
    "anime": "Pop",
    "black-metal": "Rock",
    "bluegrass": "Folk",
    "blues": "Blues",
    "brazil": "World",
    "breakbeat": "House / Electronic",
    "british": "Rock",
    "cantopop": "Pop",
    "chicago-house": "House / Electronic",
    "children": "Pop",
    "chill": "R&B",
    "classical": "Acoustic",
    "club": "House / Electronic",
    "comedy": "Pop",
    "country": "Folk",
    "dance": "House / Electronic",
    "dancehall": "Rap / Hiphop",
    "death-metal": "Rock",
    "deep-house": "House / Electronic",
    "detroit-techno": "House / Electronic",
    "disco": "House / Electronic",
    "disney": "Pop",
    "drum-and-bass": "House / Electronic",
    "dub": "R&B",
    "dubstep": "House / Electronic",
    "edm": "House / Electronic",
    "electro": "House / Electronic",
    "electronic": "House / Electronic",
    "emo": "Alternative",
    "folk": "Folk",
    "forro": "World",
    "french": "World",
    "funk": "R&B",
    "garage": "House / Electronic",
    "german": "World",
    "gospel": "R&B",
    "goth": "Alternative",
    "grindcore": "Rock",
    "groove": "R&B",
    "grunge": "Rock",
    "guitar": "Acoustic",
    "happy": "Pop",
    "hard-rock": "Rock",
    "hardcore": "Rock",
    "hardstyle": "House / Electronic",
    "heavy-metal": "Rock",
    "hip-hop": "Rap / Hiphop",
    "honky-tonk": "Blues",
    "house": "House / Electronic",
    "idm": "House / Electronic",
    "indian": "World",
    "indie": "Indie",
    "indie-pop": "Indie",
    "industrial": "Rock",
    "iranian": "World",
    "j-dance": "House / Electronic",
    "j-idol": "Pop",
    "j-pop": "Pop",
    "j-rock": "Rock",
    "jazz": "Jazz",
    "k-pop": "Pop",
    "kids": "Pop",
    "latin": "World",
    "latino": "World",
    "malay": "World",
    "mandopop": "Pop",
    "metal": "Rock",
    "metalcore": "Rock",
    "minimal-techno": "House / Electronic",
    "mpb": "World",
    "new-age": "Acoustic",
    "opera": "Acoustic",
    "pagode": "World",
    "party": "House / Electronic",
    "piano": "Acoustic",
    "pop": "Pop",
    "pop-film": "Pop",
    "power-pop": "Pop",
    "progressive-house": "House / Electronic",
    "psych-rock": "Rock",
    "punk": "Rock",
    "punk-rock": "Rock",
    "r-n-b": "R&B",
    "reggae": "R&B",
    "reggaeton": "Rap / Hiphop",
    "rock": "Rock",
    "rock-n-roll": "Rock",
    "rockabilly": "Rock",
    "romance": "Pop",
    "sad": "Acoustic",
    "salsa": "World",
    "samba": "World",
    "sertanejo": "World",
    "show-tunes": "Pop",
    "singer-songwriter": "Indie",
    "ska": "Alternative",
    "sleep": "Acoustic",
    "songwriter": "Indie",
    "soul": "R&B",
    "spanish": "World",
    "study": "Acoustic",
    "swedish": "World",
    "synth-pop": "Pop",
    "tango": "World",
    "techno": "House / Electronic",
    "trance": "House / Electronic",
    "trip-hop": "Rap / Hiphop",
    "turkish": "World",
    "world-music": "World",
}


def split_artists(df: pl.DataFrame) -> pl.DataFrame:
    """Explode the artists column on ';' so each artist gets its own row."""
    return (
        df.with_columns(pl.col("artists").str.split(";").alias("artists"))
        .explode("artists")
        .with_columns(pl.col("artists").str.strip_chars())
    )


def get_top_n_artists(df: pl.DataFrame, n: int = 5) -> pl.Series:
    """Return the n artists with the most distinct genres."""
    return (
        df.group_by("artists")
        .agg(pl.col("track_genre").n_unique().alias("distinct_count"))
        .sort("distinct_count", descending=True)
        .select("artists")
        .to_series()
        .head(n)
    )


def build_nodes(df: pl.DataFrame) -> pl.DataFrame:
    """Build artist, track, and genre node DataFrames and concatenate.

    Genre nodes use genre_category as the id (e.g. "House / Electronic").
    Multiple granular genres map to the same category node via GENRE_MAP.
    """
    artist_nodes = (
        df.select(pl.col("artists").alias("id"))
        .unique()
        .with_columns(pl.lit("artist").alias("type"))
    )

    track_nodes = (
        df.select(pl.col("track_name").alias("id"))
        .unique()
        .with_columns(pl.lit("track").alias("type"))
    )

    genre_nodes = (
        df.select(pl.col("track_genre").replace(GENRE_MAP).alias("id"))
        .unique()
        .with_columns(pl.lit("genre").alias("type"))
    )

    return pl.concat([artist_nodes, track_nodes, genre_nodes], how="diagonal")


def build_edges(df: pl.DataFrame) -> pl.DataFrame:
    """Build artist->track and track->genre edge DataFrames and concatenate."""
    artist_track_edges = df.select(
        pl.col("artists").alias("source"),
        pl.col("track_name").alias("target"),
        pl.lit("artist_track").alias("relationship"),
    ).unique()

    track_genre_edges = df.select(
        pl.col("track_name").alias("source"),
        pl.col("track_genre").replace(GENRE_MAP).alias("target"),
        pl.lit("track_genre").alias("relationship"),
    ).unique()

    return pl.concat([artist_track_edges, track_genre_edges])

def save(nodes: pl.DataFrame, edges: pl.DataFrame, output_dir: Path) -> None:
    """Write nodes.csv and edges.csv to output_dir."""
    output_dir.mkdir(exist_ok=True)
    nodes.write_csv(output_dir / "nodes.csv")
    edges.write_csv(output_dir / "edges.csv")

def build_artist_artist_edges(df: pl.DataFrame) -> pl.DataFrame:
    """
    Build an artist-only collaboration edge list:
    Connect two artists if they appear on the same track.
    Weight = # of tracks they share.
    """
    ta = df.select(["track_id", "artists"]).unique()

    pairs = (
        ta.join(ta, on="track_id", how="inner", suffix="_b")
        .filter(pl.col("artists") < pl.col("artists_b"))  # keep unique pairs
        .group_by(["artists", "artists_b"])
        .agg(pl.len().alias("weight"))
        .rename({"artists": "source", "artists_b": "target"})
        .with_columns(pl.lit("artist_artist").alias("relationship"))
    )
    return pairs

def build_artist_attributes(df: pl.DataFrame) -> pl.DataFrame:
    """
    Create one row per artist with:
    - primary_genre (most common genre category)
    - popularity (mean or max, your choice)
    - n_tracks
    """
    df2 = df.with_columns(
        pl.col("track_genre").replace(GENRE_MAP).alias("broad_genre")
    )

    # primary genre = mode (most common)
    primary = (
        df2.group_by(["artists", "broad_genre"])
        .agg(pl.len().alias("cnt"))
        .sort(["artists", "cnt"], descending=[False, True])
        .group_by("artists")
        .agg(pl.first("broad_genre").alias("primary_genre"))
    )

    stats = (
        df2.group_by("artists")
        .agg(
            pl.col("track_id").n_unique().alias("n_tracks"),
            pl.col("popularity").mean().alias("popularity_mean"),
            pl.col("popularity").max().alias("popularity_max"),
        )
    )

    return primary.join(stats, on="artists", how="inner").rename({"artists": "artist"})

if __name__ == "__main__":
    df = pl.read_csv(Path(r"project1/dataset.csv"))
    df = split_artists(df)
    top_artists = get_top_n_artists(df, n=1000)
    df_top = df.filter(pl.col("artists").is_in(top_artists.to_list()))
    nodes = build_nodes(df_top)
    edges_mixed = build_edges(df_top)
    edges_artist = build_artist_artist_edges(df_top)
    artist_attrs = build_artist_attributes(df_top)

    out = Path("project1/processed")
    save(nodes, edges_mixed, out)
    edges_artist.write_csv(out / "artist_artist_edges.csv")
    artist_attrs.write_csv(out / "artist_attributes.csv")