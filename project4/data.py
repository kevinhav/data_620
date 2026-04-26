"""
Load dataset into a pandas dataframe
"""

import pandas as pd
import requests


class DataLoader:
    def __init__(self):
        self.url = "https://raw.githubusercontent.com/Eyefyre/NYT-Connections-Answers/refs/heads/main/connections.json"
        self.raw: list[dict]
        self.df: pd.DataFrame
        self.words: list[list[str]]
        self.load()

    def load(self) -> None:
        response = requests.get(self.url)
        response.raise_for_status()
        data: list[dict] = response.json()
        self.raw = data
        self.words = [
            [word for answer in puzzle["answers"] for word in answer["members"]]
            for puzzle in data
        ]
        self.df = pd.json_normalize(data, record_path=["answers"], meta=["id", "date"])
        self.df["date"] = pd.to_datetime(self.df["date"])
