import hashlib
import os

import pandas as pd
from sqlalchemy import Column, Date, DateTime, Float, Integer, Sequence, String
from sqlalchemy.ext.declarative import declarative_base

from dormouse.extras.utils import clean_db_col_names, native_dtype


def populate_team_data(session, auto_commit=True):
    this_file_dir = os.path.dirname(__file__)
    data_path = os.path.join(
        this_file_dir, "..", "resources", "team_abbrevs.csv"
    )

    team_df = pd.read_csv(data_path, index_col=None)

    query = session.query(Teams.UID).all()
    UIDs = [x[0] for x in query]

    for _, row in team_df.iterrows():
        team = Teams(row)
        if team.UID not in UIDs:
            session.add(team)
            UIDs.append(team.UID)

    if auto_commit:
        session.commit()


class Teams(declarative_base()):
    """
    Contains all relevant data about a given team
    """

    __tablename__ = "meta_team_data"

    UID = Column(String(32), index=True, primary_key=True, unique=True)
    mlbam_abbrev = Column(String(3))
    rs_abbrev = Column(String(3))
    league = Column(String(2))
    name = Column(String(50))
    division = Column(String(3))

    def __init__(self, team_row):
        for key, value in team_row.items():
            if key is not None and value is not None:
                setattr(self, clean_db_col_names(key), native_dtype(value))
        self.UID = self._get_uid()

    def _get_uid(self):
        hash_str = (
            "".join([str(x) for x in [self.name, self.league]])
            .replace(".", "")
            .encode("utf-8")
        )
        return hashlib.md5(hash_str).hexdigest()
