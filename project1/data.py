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

    Genre nodes include a genre_category attribute mapped from GENRE_MAP.
    Artist and track nodes will have null for genre_category.
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
        df.select(pl.col("track_genre").alias("id"))
        .unique()
        .with_columns(pl.lit("genre").alias("type"))
        .with_columns(pl.col("id").replace(GENRE_MAP).alias("genre_category"))
    )

    return pl.concat([artist_nodes, track_nodes, genre_nodes], how="diagonal")


def build_edges(df: pl.DataFrame) -> pl.DataFrame:
    """Build artist->track and track->genre edge DataFrames and concatenate."""
    artist_track_edges = (
        df.select(
            pl.col("artists").alias("source"),
            pl.col("track_name").alias("target"),
            pl.lit("artist_track").alias("relationship"),
        )
        .unique()
    )

    track_genre_edges = (
        df.select(
            pl.col("track_name").alias("source"),
            pl.col("track_genre").alias("target"),
            pl.lit("track_genre").alias("relationship"),
        )
        .unique()
    )

    return pl.concat([artist_track_edges, track_genre_edges])


def save(nodes: pl.DataFrame, edges: pl.DataFrame, output_dir: Path) -> None:
    """Write nodes.csv and edges.csv to output_dir."""
    output_dir.mkdir(exist_ok=True)
    nodes.write_csv(output_dir / "nodes.csv")
    edges.write_csv(output_dir / "edges.csv")


if __name__ == "__main__":
    df = pl.read_csv(Path(r"project1\dataset.csv"))
    top_artists = get_top_n_artists(df, n=5)
    df_top = df.filter(pl.col("artists").is_in(top_artists))
    nodes = build_nodes(df_top)
    edges = build_edges(df_top)
    save(nodes, edges, Path(r"project1\processed"))
