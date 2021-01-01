import hashlib
import io
from datetime import datetime
from zipfile import ZipFile

import pandas as pd
import requests
from sqlalchemy import Column, Date, DateTime, Float, Integer, Sequence, String
from sqlalchemy.ext.declarative import declarative_base

from dormouse.extras.utils import (
    clean_db_col_names,
    native_dtype,
    cast_fiel_dtypes,
)


def _unzip_content(content) -> ZipFile:
    """
    Unzips the byte content and returns a zipfile object
    """
    f = io.BytesIO(content)
    data = ZipFile(f)
    return data


def populate_game_log(year, game_type, session, auto_commit=True):
    """
    Populates the game log table with data from the given year
    TODO: Implement other game types (WS, DS, etc.)
    """
    if game_type != "rs":
        raise NotImplementedError("Only regular season is supported")

    rs_columns = [
        "Date",
        "GameSeriesNumber",
        "DOW",
        "VisitingTeam",
        "VisitingLeague",
        "VisitingTeamGameNumber",
        "HomeTeam",
        "HomeLeague",
        "HomeTeamGameNumber",
        "VistingScore",
        "HomeScore",
        "NumberOuts",
        "DayNight",
        "CompletionInfo",
        "ForfeitInfo",
        "ProtestInfo",
        "ParkID",
        "Attendance",
        "TimeOfGame",
        "VistingLineScore",
        "HomeLineScore",
        "Visiting_AB",
        "Visiting_B1",
        "Visiting_B2",
        "Visiting_B3",
        "Visiting_HR",
        "Visiting_RBI",
        "Visiting_SH",
        "Visiting_SF",
        "Visiting_HBP",
        "Visiting_BB",
        "Visiting_IBB",
        "Visiting_K",
        "Visiting_SB",
        "Visiting_CS",
        "Visiting_GDP",
        "Visiting_INT",
        "Visiting_LOB",
        "Visiting_PitchersUsed",
        "Visiting_IndividualER",
        "Visiting_TeamER",
        "Visiting_WP",
        "Visiting_BK",
        "Visiting_PO",
        "Visiting_A",
        "Visiting_E",
        "Visiting_PassedBall",
        "Visiting_DP",
        "Visiting_TP",
        "Home_AB",
        "Home_B1",
        "Home_B2",
        "Home_B3",
        "Home_HR",
        "Home_RBI",
        "Home_SH",
        "Home_SF",
        "Home_HBP",
        "Home_BB",
        "Home_IBB",
        "Home_K",
        "Home_SB",
        "Home_CS",
        "Home_GDP",
        "Home_INT",
        "Home_LOB",
        "Home_PitchersUsed",
        "Home_IndividualER",
        "Home_TeamER",
        "Home_WP",
        "Home_BK",
        "Home_PO",
        "Home_A",
        "Home_E",
        "Home_PassedBall",
        "Home_DP",
        "Home_TP",
        "HP_UmpireID",
        "HP_UmpireName",
        "B1_UmpireID",
        "B1_UmpireName",
        "B2_UmpireID",
        "B2_UmpireName",
        "B3_UmpireID",
        "B3_UmpireName",
        "LF_UmpireID",
        "LF_UmpireName",
        "RF_UmpireID",
        "RF_UmpireName",
        "Visiting_ManagerID",
        "Visiting_ManagerName",
        "Home_ManagerID",
        "Home_ManagerName",
        "WinningPitcherID",
        "WinningPitcherName",
        "LosingPitcherID",
        "LosingPitcherName",
        "SavingPitcherID",
        "SavingPitcherName",
        "GameWinRBIID",
        "GameWinRBIName",
        "Visiting_StartingPID",
        "Visiting_StartingPName",
        "Home_StartingPID",
        "Home_StartingPName",
        "Visiting_Batter1ID",
        "Visiting_Batter1Name",
        "Visiting_Batter1Pos",
        "Visiting_Batter2ID",
        "Visiting_Batter2Name",
        "Visiting_Batter2Pos",
        "Visiting_Batter3ID",
        "Visiting_Batter3Name",
        "Visiting_Batter3Pos",
        "Visiting_Batter4ID",
        "Visiting_Batter4Name",
        "Visiting_Batter4Pos",
        "Visiting_Batter5ID",
        "Visiting_Batter5Name",
        "Visiting_Batter5Pos",
        "Visiting_Batter6ID",
        "Visiting_Batter6Name",
        "Visiting_Batter6Pos",
        "Visiting_Batter7ID",
        "Visiting_Batter7Name",
        "Visiting_Batter7Pos",
        "Visiting_Batter8ID",
        "Visiting_Batter8Name",
        "Visiting_Batter8Pos",
        "Visiting_Batter9ID",
        "Visiting_Batter9Name",
        "Visiting_Batter9Pos",
        "Home_Batter1ID",
        "Home_Batter1Name",
        "Home_Batter1Pos",
        "Home_Batter2ID",
        "Home_Batter2Name",
        "Home_Batter2Pos",
        "Home_Batter3ID",
        "Home_Batter3Name",
        "Home_Batter3Pos",
        "Home_Batter4ID",
        "Home_Batter4Name",
        "Home_Batter4Pos",
        "Home_Batter5ID",
        "Home_Batter5Name",
        "Home_Batter5Pos",
        "Home_Batter6ID",
        "Home_Batter6Name",
        "Home_Batter6Pos",
        "Home_Batter7ID",
        "Home_Batter7Name",
        "Home_Batter7Pos",
        "Home_Batter8ID",
        "Home_Batter8Name",
        "Home_Batter8Pos",
        "Home_Batter9ID",
        "Home_Batter9Name",
        "Home_Batter9Pos",
        "AdditionalInformation",
        "AcquisitionInformation",
    ]

    def _fix_date(dt_string):
        """
        Convert YYYYMMDD integer to datetime object
        """
        dt_string = str(dt_string)
        return datetime(
            year=int(dt_string[0:4]),
            month=int(dt_string[4:6]),
            day=int(dt_string[6:8]),
        )

    url = "https://www.retrosheet.org/gamelogs/gl{}.zip".format(year)
    res = requests.get(url)
    data = _unzip_content(res.content)
    rs_file = data.read(data.namelist()[0])
    df = pd.read_csv(io.BytesIO(rs_file), header=None, names=rs_columns)
    df = df.fillna(0)

    # df = cast_fiel_dtypes(df, TeamLineup)
    df = cast_fiel_dtypes(df, GameLog)

    # Fix game date columns
    df["Date"] = df["Date"].apply(_fix_date)

    # Get list of UIDs in db
    query = session.query(TeamLineup.UID).all()
    UIDs = [x[0] for x in query]

    for _, row in df.iterrows():
        game = GameLog(row)
        session.add(game)
        hlu = TeamLineup(game, "Home")
        vlu = TeamLineup(game, "Visiting")
        if hlu.UID not in UIDs:
            session.add(hlu)
        if vlu.UID not in UIDs:
            session.add(vlu)

    if auto_commit:
        session.commit()


def populate_team_roster(year, session, auto_commit=True):
    """
    Populates the team roster table with data from team for the season year

    team is the 3 letter RS team code
    """

    query = session.query(TeamRoster.UID).all()
    UIDs = [x[0] for x in query]

    roster_cols = [
        "rs_id",
        "name_first",
        "name_last",
        "bats",
        "throws",
        "team",
        "position",
    ]
    base_url = "https://www.retrosheet.org/events/{}eve.zip"
    res = requests.get(base_url.format(str(year)))
    data = _unzip_content(res.content)
    file_list = data.namelist()

    for f_name in [x for x in file_list if x[-3:] == "ROS"]:
        df = pd.read_csv(
            io.BytesIO(data.read(f_name)), header=None, names=roster_cols
        )
        df["year"] = year
        for _, row in df.iterrows():
            roster = TeamRoster(row)
            if roster.UID not in UIDs:
                session.add(roster)
                UIDs.append(roster.UID)

    if auto_commit:
        session.commit()


def get_line_score(game_row: pd.Series, side="Home"):
    """
    Parses out the line score for a given side and returns a list
    :param game_row: The full row of game data from the GameLog table
    :type class: 'pd.Series', required
    :param side: The side of the game to return
    :type ["Home", "Visiting"], required
    """
    raise NotImplementedError("yeah")
    # line_string = game_row["{}LineScore".format(side)]
    # score_list = []
    # for char in line_string:
    #     score_list.append(char)


class GameLog(declarative_base()):
    """
    Game summaries from retrosheet.org

    The information used here was obtained free of
    charge from and is copyrighted by Retrosheet.  Interested
    parties may contact Retrosheet at "www.retrosheet.org".

    """

    __tablename__ = "rs_gamelog"
    # TODO: Need a better primary key for game log
    id = Column(Integer, Sequence("game_id_seq"), primary_key=True)
    Date = Column(DateTime)
    GameSeriesNumber = Column(Integer)
    DOW = Column(String(3))
    VisitingTeam = Column(String(3))
    VisitingLeague = Column(String(2))
    VisitingTeamGameNumber = Column(Integer)
    HomeTeam = Column(String(3))
    HomeLeague = Column(String(2))
    HomeTeamGameNumber = Column(Integer)
    VistingScore = Column(Integer)
    HomeScore = Column(Integer)
    NumberOuts = Column(Integer)
    DayNight = Column(String(1))
    CompletionInfo = Column(String(100))
    ForfeitInfo = Column(String(100))
    ProtestInfo = Column(String(100))
    ParkID = Column(String(5))
    Attendance = Column(Integer)
    TimeOfGame = Column(Integer)
    # The line scores need to be parsed in a special way
    VistingLineScore = Column(String(50))
    HomeLineScore = Column(String(50))
    Visiting_AB = Column(Integer)
    Visiting_B1 = Column(Integer)
    Visiting_B2 = Column(Integer)
    Visiting_B3 = Column(Integer)
    Visiting_HR = Column(Integer)
    Visiting_RBI = Column(Integer)
    Visiting_SH = Column(Integer)
    Visiting_SF = Column(Integer)
    Visiting_HBP = Column(Integer)
    Visiting_BB = Column(Integer)
    Visiting_IBB = Column(Integer)
    Visiting_K = Column(Integer)
    Visiting_SB = Column(Integer)
    Visiting_CS = Column(Integer)
    Visiting_GDP = Column(Integer)
    Visiting_INT = Column(Integer)
    Visiting_LOB = Column(Integer)
    Visiting_PitchersUsed = Column(Integer)
    Visiting_IndividualER = Column(Integer)
    Visiting_TeamER = Column(Integer)
    Visiting_WP = Column(Integer)
    Visiting_BK = Column(Integer)
    Visiting_PO = Column(Integer)
    Visiting_A = Column(Integer)
    Visiting_E = Column(Integer)
    Visiting_PassedBall = Column(Integer)
    Visiting_DP = Column(Integer)
    Visiting_TP = Column(Integer)
    Home_AB = Column(Integer)
    Home_B1 = Column(Integer)
    Home_B2 = Column(Integer)
    Home_B3 = Column(Integer)
    Home_HR = Column(Integer)
    Home_RBI = Column(Integer)
    Home_SH = Column(Integer)
    Home_SF = Column(Integer)
    Home_HBP = Column(Integer)
    Home_BB = Column(Integer)
    Home_IBB = Column(Integer)
    Home_K = Column(Integer)
    Home_SB = Column(Integer)
    Home_CS = Column(Integer)
    Home_GDP = Column(Integer)
    Home_INT = Column(Integer)
    Home_LOB = Column(Integer)
    Home_PitchersUsed = Column(Integer)
    Home_IndividualER = Column(Integer)
    Home_TeamER = Column(Integer)
    Home_WP = Column(Integer)
    Home_BK = Column(Integer)
    Home_PO = Column(Integer)
    Home_A = Column(Integer)
    Home_E = Column(Integer)
    Home_PassedBall = Column(Integer)
    Home_DP = Column(Integer)
    Home_TP = Column(Integer)
    HP_UmpireID = Column(String(8))
    HP_UmpireName = Column(String(100))
    B1_UmpireID = Column(String(8))
    B1_UmpireName = Column(String(100))
    B2_UmpireID = Column(String(8))
    B2_UmpireName = Column(String(100))
    B3_UmpireID = Column(String(8))
    B3_UmpireName = Column(String(100))
    LF_UmpireID = Column(String(8))
    LF_UmpireName = Column(String(100))
    RF_UmpireID = Column(String(8))
    RF_UmpireName = Column(String(100))
    Visiting_ManagerID = Column(String(8))
    Visiting_ManagerName = Column(String(100))
    Home_ManagerID = Column(String(8))
    Home_ManagerName = Column(String(100))
    WinningPitcherID = Column(String(8))
    WinningPitcherName = Column(String(100))
    LosingPitcherID = Column(String(8))
    LosingPitcherName = Column(String(100))
    SavingPitcherID = Column(String(8))
    SavingPitcherName = Column(String(100))
    GameWinRBIID = Column(String(8))
    GameWinRBIName = Column(String(100))
    Visiting_StartingPID = Column(String(8))
    Visiting_StartingPName = Column(String(100))
    Home_StartingPID = Column(String(8))
    Home_StartingPName = Column(String(100))
    Visiting_Batter1ID = Column(String(8))
    Visiting_Batter1Name = Column(String(100))
    Visiting_Batter1Pos = Column(Integer)
    Visiting_Batter2ID = Column(String(8))
    Visiting_Batter2Name = Column(String(100))
    Visiting_Batter2Pos = Column(Integer)
    Visiting_Batter3ID = Column(String(8))
    Visiting_Batter3Name = Column(String(100))
    Visiting_Batter3Pos = Column(Integer)
    Visiting_Batter4ID = Column(String(8))
    Visiting_Batter4Name = Column(String(100))
    Visiting_Batter4Pos = Column(Integer)
    Visiting_Batter5ID = Column(String(8))
    Visiting_Batter5Name = Column(String(100))
    Visiting_Batter5Pos = Column(Integer)
    Visiting_Batter6ID = Column(String(8))
    Visiting_Batter6Name = Column(String(100))
    Visiting_Batter6Pos = Column(Integer)
    Visiting_Batter7ID = Column(String(8))
    Visiting_Batter7Name = Column(String(100))
    Visiting_Batter7Pos = Column(Integer)
    Visiting_Batter8ID = Column(String(8))
    Visiting_Batter8Name = Column(String(100))
    Visiting_Batter8Pos = Column(Integer)
    Visiting_Batter9ID = Column(String(8))
    Visiting_Batter9Name = Column(String(100))
    Visiting_Batter9Pos = Column(Integer)
    Home_Batter1ID = Column(String(8))
    Home_Batter1Name = Column(String(100))
    Home_Batter1Pos = Column(Integer)
    Home_Batter2ID = Column(String(8))
    Home_Batter2Name = Column(String(100))
    Home_Batter2Pos = Column(Integer)
    Home_Batter3ID = Column(String(8))
    Home_Batter3Name = Column(String(100))
    Home_Batter3Pos = Column(Integer)
    Home_Batter4ID = Column(String(8))
    Home_Batter4Name = Column(String(100))
    Home_Batter4Pos = Column(Integer)
    Home_Batter5ID = Column(String(8))
    Home_Batter5Name = Column(String(100))
    Home_Batter5Pos = Column(Integer)
    Home_Batter6ID = Column(String(8))
    Home_Batter6Name = Column(String(100))
    Home_Batter6Pos = Column(Integer)
    Home_Batter7ID = Column(String(8))
    Home_Batter7Name = Column(String(100))
    Home_Batter7Pos = Column(Integer)
    Home_Batter8ID = Column(String(8))
    Home_Batter8Name = Column(String(100))
    Home_Batter8Pos = Column(Integer)
    Home_Batter9ID = Column(String(8))
    Home_Batter9Name = Column(String(100))
    Home_Batter9Pos = Column(Integer)
    AdditionalInformation = Column(String(100))
    AcquisitionInformation = Column(String(1))

    def __init__(self, single_game_er):
        for key, value in single_game_er.items():
            if key is not None and value is not None:
                setattr(self, clean_db_col_names(key), native_dtype(value))


class TeamLineup(declarative_base()):
    """Derivative table to store only linuep data.
    Relies on GameLog to properly function

    UID is md5 hash of GameLog.Date and GameLog.GameSeriesNumber and self.team
    """

    __tablename__ = "rs_team_lineups"
    UID = Column(String(32), index=True, primary_key=True, unique=True)
    parkid = Column(String(5))
    team = String(3)
    StartingPID = Column(String(8))
    Batter1ID = Column(String(8))
    Batter1Pos = Column(Integer)
    Batter2ID = Column(String(8))
    Batter2Pos = Column(Integer)
    Batter3ID = Column(String(8))
    Batter3Pos = Column(Integer)
    Batter4ID = Column(String(8))
    Batter4Pos = Column(Integer)
    Batter5ID = Column(String(8))
    Batter5Pos = Column(Integer)
    Batter6ID = Column(String(8))
    Batter6Pos = Column(Integer)
    Batter7ID = Column(String(8))
    Batter7Pos = Column(Integer)
    Batter8ID = Column(String(8))
    Batter8Pos = Column(Integer)
    Batter9ID = Column(String(8))
    Batter9Pos = Column(Integer)

    def __init__(self, glog: GameLog, side: str):

        self._glog = glog
        if side not in ["Home", "Visiting"]:
            raise ValueError(f"{side} not recognized as a valid parameter")
        else:
            self.side = side

        # just brute force this for now
        self.parkid = glog.ParkID
        self.team = glog.__dict__["{}Team".format(self.side)]

        self.StartingPID = self._get_prop("StartingPID")
        self.Batter1ID = self._get_prop("Batter1ID")
        self.Batter1Pos = self._get_prop("Batter1Pos")
        self.Batter2ID = self._get_prop("Batter2ID")
        self.Batter2Pos = self._get_prop("Batter2Pos")
        self.Batter3ID = self._get_prop("Batter9Pos")
        self.Batter3Pos = self._get_prop("Batter9Pos")
        self.Batter4ID = self._get_prop("Batter4ID")
        self.Batter4Pos = self._get_prop("Batter4Pos")
        self.Batter5ID = self._get_prop("Batter5ID")
        self.Batter5Pos = self._get_prop("Batter5Pos")
        self.Batter6ID = self._get_prop("Batter6ID")
        self.Batter6Pos = self._get_prop("Batter9Pos")
        self.Batter7ID = self._get_prop("Batter7ID")
        self.Batter7Pos = self._get_prop("Batter7Pos")
        self.Batter8ID = self._get_prop("Batter8ID")
        self.Batter8Pos = self._get_prop("Batter8Pos")
        self.Batter9ID = self._get_prop("Batter9ID")
        self.Batter9Pos = self._get_prop("Batter9Pos")

        self.UID = self.get_uid()

    def get_uid(self):

        hash_str = "".join(
            [
                str(x)
                for x in [
                    self._glog.Date,
                    self._glog.GameSeriesNumber,
                    self.team,
                ]
            ]
        ).encode("utf-8")

        return hashlib.md5(hash_str).hexdigest()

    def _get_prop(self, prop_string):
        return self._glog.__dict__["{}_{}".format(self.side, prop_string)]


class TeamRoster(declarative_base()):
    """
    Table for storing current and historic roster data
    """

    __tablename__ = "rs_team_rosters"
    UID = Column(String(32), index=True, primary_key=True, unique=True)
    team = Column(String(3))
    year = Column(Integer)
    rs_id = Column(String(8))
    name_first = Column(String(50))
    name_last = Column(String(50))
    position = Column(String(3))
    bats = Column(String(1))
    throws = Column(String(1))

    def __init__(self, roster_row):
        for key, value in roster_row.items():
            if key is not None and value is not None:
                setattr(self, clean_db_col_names(key), native_dtype(value))
        self.UID = self._get_uid()

    def _get_uid(self):
        hash_str = (
            "".join(
                [
                    str(x)
                    for x in [
                        self.team,
                        self.year,
                        self.name_first,
                        self.name_last,
                        self.bats,
                        self.throws,
                    ]
                ]
            )
            .replace(".", "")
            .encode("utf-8")
        )
        return hashlib.md5(hash_str).hexdigest()
