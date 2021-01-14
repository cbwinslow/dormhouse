import hashlib
import os

import pandas as pd
from sqlalchemy import Column, Date, DateTime, Float, Integer, Sequence, String
from sqlalchemy.ext.declarative import declarative_base

from dormouse.extras.utils import clean_db_col_names, native_dtype


def populate_team_data(session, auto_commit=True):

    team_df = pd.DataFrame.from_dict(
        {
            "name": {
                0: "Arizone Diamonbacks",
                1: "Tampa Bay Rays",
                2: "St. Louis Cardinals",
                3: "Baltimore Orioles",
                4: "Houston Atros",
                5: "Los Angeles Dodgers",
                6: "Boston Red Sox",
                7: "Cincinati Reds",
                8: "Washington Nationals",
                9: "Oakland Athleticss",
                10: "Minnisota Twins",
                11: "Milwauke Brewers",
                12: "Texas Rangers",
                13: "New York Mets",
                14: "Chicago White Sox",
                15: "Philidelphia Phillies",
                16: "Los Angeles Angels",
                17: "San Diego Padres",
                18: "Colorado Rockies",
                19: "Detroit Tigers",
                20: "Pittsburgh Pirates",
                21: "New York Yankees",
                22: "Kansas City Royals",
                23: "Chicago Cubs",
                24: "Seattle Mariners",
                25: "San Francisco Giants",
                26: "Toronto Blue Jays",
                27: "Cleveland Indians",
                28: "Miami Marlins",
                29: "Atlanta Braves",
            },
            "league": {
                0: "NL",
                1: "AL",
                2: "NL",
                3: "AL",
                4: "AL",
                5: "NL",
                6: "AL",
                7: "NL",
                8: "NL",
                9: "AL",
                10: "AL",
                11: "NL",
                12: "AL",
                13: "NL",
                14: "AL",
                15: "NL",
                16: "AL",
                17: "NL",
                18: "NL",
                19: "AL",
                20: "NL",
                21: "AL",
                22: "AL",
                23: "NL",
                24: "AL",
                25: "NL",
                26: "AL",
                27: "AL",
                28: "NL",
                29: "NL",
            },
            "rs_abbrev": {
                0: "ARI",
                1: "TBA",
                2: "SLN",
                3: "BAL",
                4: "HOU",
                5: "LAN",
                6: "BOS",
                7: "CIN",
                8: "WAS",
                9: "OAK",
                10: "MIN",
                11: "MIL",
                12: "TEX",
                13: "NYN",
                14: "CHA",
                15: "PHI",
                16: "ANA",
                17: "SDN",
                18: "COL",
                19: "DET",
                20: "PIT",
                21: "NYA",
                22: "KCA",
                23: "CHN",
                24: "SEA",
                25: "SFN",
                26: "TOR",
                27: "CLE",
                28: "MIA",
                29: "ATL",
            },
            "mlbam_abbrev": {
                0: "ARI",
                1: "TB",
                2: "STL",
                3: "BAL",
                4: "HOU",
                5: "LAD",
                6: "BOS",
                7: "CIN",
                8: "WSH",
                9: "OAK",
                10: "MIN",
                11: "MIL",
                12: "TEX",
                13: "NYM",
                14: "CWS",
                15: "PHI",
                16: "LAA",
                17: "SD",
                18: "COL",
                19: "DET",
                20: "PIT",
                21: "NYY",
                22: "KC",
                23: "CHC",
                24: "SEA",
                25: "SF",
                26: "TOR",
                27: "CLE",
                28: "MIA",
                29: "ATL",
            },
            "division": {
                0: "NLW",
                1: "ALE",
                2: "NLC",
                3: "ALE",
                4: "ALW",
                5: "NLW",
                6: "ALE",
                7: "NLC",
                8: "NLE",
                9: "ALW",
                10: "ALC",
                11: "NLC",
                12: "ALW",
                13: "NLE",
                14: "ALC",
                15: "NLE",
                16: "ALW",
                17: "NLW",
                18: "NLW",
                19: "ALC",
                20: "NLC",
                21: "ALE",
                22: "ALC",
                23: "NLC",
                24: "ALW",
                25: "NLW",
                26: "ALE",
                27: "ALC",
                28: "NLE",
                29: "NLE",
            },
        }
    )

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

    __tablename__ = "tblTeams"

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
