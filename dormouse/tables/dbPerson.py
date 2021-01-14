import hashlib
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

import requests

from sqlalchemy import Column, DateTime, Float, Integer, Sequence, String
from sqlalchemy.ext.declarative import declarative_base

from pybaseball import statcast
from pybaseball.playerid_lookup import get_lookup_table


from dormouse.extras.pybb import retro_day_stats, single_player_batting_stats
from dormouse.extras.utils import (
    clean_db_col_names,
    get_col_min_max,
    native_dtype,
    space_out_req,
    cast_fiel_dtypes,
)

_BASE = declarative_base()

def populate_statcast(
    start_dt: datetime, end_date: datetime, session, auto_commit=True
):
    """
    Populates the statcast_pitching table with values ranging from start date to end date, inclusively.
    # TODO: Make this work with a lst of supplied teams instead of all teams
    # TODO: Check to see what the earliest and latest dates are in the db. Fill in dates only where needed
    """

    @space_out_req
    def _single_day_sc(d):
        return statcast(start_dt=d)

    date = start_dt

    # Get list of UIDs in db
    query = (
        session.query(StatcastPitching.UID)
        .filter(
            StatcastPitching.game_date >= start_dt,
            StatcastPitching.game_date <= end_date,
        )
        .all()
    )
    UIDs = [x[0] for x in query]
    while date <= end_date:
        try:
            df = _single_day_sc(date.strftime("%Y-%m-%d"))
            df = df.fillna(0)
            df = cast_fiel_dtypes(df, StatcastPitching)
            for _, row in df.iterrows():
                entry = StatcastPitching(row)
                if entry.UID not in UIDs:
                    session.add(entry)
            print(date)
        except ValueError:
            print(f"error @ {date}")

        date += timedelta(days=1)

        """
        Since the datasets are so large (25 MB / 3 days), we need to commit
        rows after every query. If not, we may wind up trying to add multiple
        GB of data in one INSERT statement.
        """
        if auto_commit:
            session.commit()

    return


def populate_player_lu(session, auto_commit=True):
    """
    Can only do the entire table or no table at all
    """
    # From pybaseball
    lu_df = get_lookup_table()
    # covnert to correct dtypes
    lu_df["mlb_played_last"] = (
        pd.to_numeric(lu_df["mlb_played_last"], errors="coerce")
        .fillna(0)
        .astype(np.int64)
    )
    lu_df["mlb_played_first"] = (
        pd.to_numeric(lu_df["mlb_played_first"], errors="coerce")
        .fillna(0)
        .astype(np.int64)
    )

    query = session.query(PlayerLookup.key_mlbam).all()
    UIDs = [x[0] for x in query]
    for _, row in lu_df.iterrows():
        # Only add if there is advanced data for a given player
        if row["key_mlbam"] != -1:
            player = PlayerLookup(row)
            if player.key_mlbam not in UIDs:
                session.add(player)

    if auto_commit:
        session.commit()

    return


def populate_player_game_stats(
    start_season, end_season, session, auto_commit=True
):
    """
    Populates the player single game stats with data from Retrosheets Day-by-day events for an entire season
    """

    data = retro_day_stats(start_season, end_season)

    data = data.fillna(0)
    data = cast_fiel_dtypes(data, PlayerGameStats)

    query = session.query(PlayerGameStats.UID).all()
    UIDs = [x[0] for x in query]
    for _, row in data.iterrows():
        # Only add if there is advanced data for a given player
        player = PlayerGameStats(row)
        if player.UID not in UIDs:
            UIDs.append(player.UID)
            session.add(player)

    if auto_commit:
        session.commit()


class StatcastPitching(_BASE):
    """
    Statcast data for a single pitch
    """

    __tablename__ = "tblStatcastPitching"
    # id = Column(Integer, Sequence("event_id_seq"), primary_key=True)
    UID = Column(String(32), index=True, primary_key=True, unique=True)
    pitch_type = Column(String(2))
    game_date = Column(DateTime)
    release_speed = Column(Float)
    release_pos_x = Column(Float)
    release_pos_z = Column(Float)
    player_name = Column(String(100))
    batter = Column(Integer)
    pitcher = Column(Integer)
    events = Column(String(50))
    description = Column(String(50))
    # spin_dir = Column(Float) # I think this is deprecated????
    zone = Column(Integer)
    des = Column(String(500))
    game_type = Column(String(1))
    stand = Column(String(1))
    p_throws = Column(String(1))
    home_team = Column(String(3))
    away_team = Column(String(3))
    result_type = Column(String(1))
    hit_location = Column(Integer)
    bb_type = Column(String(50))
    balls = Column(Integer)
    strikes = Column(Integer)
    game_year = Column(Integer)
    pfx_x = Column(Float)
    pfx_z = Column(Float)
    plate_x = Column(Float)
    plate_z = Column(Float)
    on_3b = Column(Integer)
    on_2b = Column(Integer)
    on_1b = Column(Integer)
    outs_when_up = Column(Integer)
    inning = Column(Integer)
    inning_topbot = Column(String(3))
    hc_x = Column(Float)
    hc_y = Column(Float)
    fielder_2 = Column(Integer)
    umpire = Column(Integer)
    sv_id = Column(String(15))
    vx0 = Column(Float)
    vy0 = Column(Float)
    vz0 = Column(Float)
    ax = Column(Float)
    ay = Column(Float)
    az = Column(Float)
    sz_top = Column(Float)
    sz_bot = Column(Float)
    hit_distance_sc = Column(Integer)
    launch_speed = Column(Float)
    launch_angle = Column(Float)
    effective_speed = Column(Float)
    release_spin_rate = Column(Integer)
    release_extension = Column(Float)
    game_pk = Column(Integer)
    pitcher_1 = Column(Integer)
    fielder_2_1 = Column(Integer)
    fielder_3 = Column(Integer)
    fielder_4 = Column(Integer)
    fielder_5 = Column(Integer)
    fielder_6 = Column(Integer)
    fielder_7 = Column(Integer)
    fielder_8 = Column(Integer)
    fielder_9 = Column(Integer)
    release_pos_y = Column(Float)
    estimated_ba_using_speedangle = Column(Float)
    estimated_woba_using_speedangle = Column(Float)
    woba_value = Column(Float)
    woba_denom = Column(Integer)
    babip_value = Column(Integer)
    iso_value = Column(Integer)
    launch_speed_angle = Column(Integer)
    at_bat_number = Column(Integer)
    pitch_number = Column(Integer)
    pitch_name = Column(String(50))
    home_score = Column(Integer)
    away_score = Column(Integer)
    bat_score = Column(Integer)
    fld_score = Column(Integer)
    post_away_score = Column(Integer)
    post_home_score = Column(Integer)
    post_bat_score = Column(Integer)
    post_fld_score = Column(Integer)
    if_fielding_alignment = Column(String(25))
    of_fielding_alignment = Column(String(25))

    def __init__(self, statcast_series: pd.Series):
        for key, value in statcast_series.items():
            if key is not None and value is not None:
                setattr(
                    self,
                    clean_db_col_names(
                        key, replace_set={"type": "result_type"}
                    ),
                    native_dtype(value),
                )
        self.UID = self._get_uid()

    def _get_uid(self):
        hash_str = (
            "".join(
                [
                    str(x)
                    for x in [
                        self.game_pk,
                        self.pitcher,
                        self.at_bat_number,
                        self.pitch_number,
                        self.release_speed,
                    ]
                ]
            )
            .replace(".", "")
            .encode("utf-8")
        )
        return hashlib.md5(hash_str).hexdigest()


class PlayerLookup(_BASE):
    """
    Player lookup table provided by chadwick b.
    """

    __tablename__ = "tblPlayerLookup"
    # Even though key_person is unique, and is part our our UID hashing, we will
    # create our own UID to keep tables' schema consitent with eachother
    id = Column(
        Integer,
        Sequence("id_auto_seq", start=1, increment=1),
        index=True,
        primary_key=True,
        unique=True,
    )
    name_last = Column(String(100))
    name_first = Column(String(100))
    key_mlbam = Column(Integer)
    key_retro = Column(String(8))
    key_bbref = Column(String(9))
    key_fangraphs = Column(Integer)
    mlb_played_first = Column(Integer)
    mlb_played_last = Column(Integer)

    def __init__(self, player_meta: pd.Series):
        for key, value in player_meta.items():
            if key is not None and value is not None:
                setattr(self, clean_db_col_names(key), native_dtype(value))


class PlayerGameStats(_BASE):
    # From https://github.com/chadwickbureau/retrosplits/tree/master/daybyday
    __tablename__ = "tblSingleGamePlayerStats"
    UID = Column(String(21), primary_key=True, unique=True, index=True)
    game_key = Column(String(12))
    game_source = Column(String(3))
    game_date = Column(DateTime)
    game_number = Column(Integer)
    appear_date = Column(DateTime)
    site_key = Column(String(5))
    season_phase = Column(String(1))
    team_alignment = Column(Integer)
    team_key = Column(String(3))
    opponent_key = Column(String(3))
    person_key = Column(String(8))
    slot = Column(Integer)
    seq = Column(Integer)
    B_G = Column(Integer)
    B_PA = Column(Integer)
    B_AB = Column(Integer)
    B_R = Column(Integer)
    B_H = Column(Integer)
    B_TB = Column(Integer)
    B_2B = Column(Integer)
    B_3B = Column(Integer)
    B_HR = Column(Integer)
    B_HR4 = Column(Integer)
    B_RBI = Column(Integer)
    B_GW = Column(Integer)
    B_BB = Column(Integer)
    B_IBB = Column(Integer)
    B_SO = Column(Integer)
    B_GDP = Column(Integer)
    B_HP = Column(Integer)
    B_SH = Column(Integer)
    B_SF = Column(Integer)
    B_SB = Column(Integer)
    B_CS = Column(Integer)
    B_XI = Column(Integer)
    B_G_DH = Column(Integer)
    B_G_PH = Column(Integer)
    B_G_PR = Column(Integer)
    P_G = Column(Integer)
    P_GS = Column(Integer)
    P_CG = Column(Integer)
    P_SHO = Column(Integer)
    P_GF = Column(Integer)
    P_W = Column(Integer)
    P_L = Column(Integer)
    P_SV = Column(Integer)
    P_OUT = Column(Integer)
    P_TBF = Column(Integer)
    P_AB = Column(Integer)
    P_R = Column(Integer)
    P_ER = Column(Integer)
    P_H = Column(Integer)
    P_TB = Column(Integer)
    P_2B = Column(Integer)
    P_3B = Column(Integer)
    P_HR = Column(Integer)
    P_HR4 = Column(Integer)
    P_BB = Column(Integer)
    P_IBB = Column(Integer)
    P_SO = Column(Integer)
    P_GDP = Column(Integer)
    P_HP = Column(Integer)
    P_SH = Column(Integer)
    P_SF = Column(Integer)
    P_XI = Column(Integer)
    P_WP = Column(Integer)
    P_BK = Column(Integer)
    P_IR = Column(Integer)
    P_IRS = Column(Integer)
    P_GO = Column(Integer)
    P_AO = Column(Integer)
    P_PITCH = Column(Integer)
    P_STRIKE = Column(Integer)
    F_1B_POS = Column(Integer)
    F_1B_G = Column(Integer)
    F_1B_GS = Column(Integer)
    F_1B_OUT = Column(Integer)
    F_1B_TC = Column(Integer)
    F_1B_PO = Column(Integer)
    F_1B_A = Column(Integer)
    F_1B_E = Column(Integer)
    F_1B_DP = Column(Integer)
    F_1B_TP = Column(Integer)
    F_2B_POS = Column(Integer)
    F_2B_G = Column(Integer)
    F_2B_GS = Column(Integer)
    F_2B_OUT = Column(Integer)
    F_2B_TC = Column(Integer)
    F_2B_PO = Column(Integer)
    F_2B_A = Column(Integer)
    F_2B_E = Column(Integer)
    F_2B_DP = Column(Integer)
    F_2B_TP = Column(Integer)
    F_3B_POS = Column(Integer)
    F_3B_G = Column(Integer)
    F_3B_GS = Column(Integer)
    F_3B_OUT = Column(Integer)
    F_3B_TC = Column(Integer)
    F_3B_PO = Column(Integer)
    F_3B_A = Column(Integer)
    F_3B_E = Column(Integer)
    F_3B_DP = Column(Integer)
    F_3B_TP = Column(Integer)
    F_SS_POS = Column(Integer)
    F_SS_G = Column(Integer)
    F_SS_GS = Column(Integer)
    F_SS_OUT = Column(Integer)
    F_SS_TC = Column(Integer)
    F_SS_PO = Column(Integer)
    F_SS_A = Column(Integer)
    F_SS_E = Column(Integer)
    F_SS_DP = Column(Integer)
    F_SS_TP = Column(Integer)
    F_OF_POS = Column(Integer)
    F_OF_G = Column(Integer)
    F_OF_GS = Column(Integer)
    F_OF_OUT = Column(Integer)
    F_OF_TC = Column(Integer)
    F_OF_PO = Column(Integer)
    F_OF_A = Column(Integer)
    F_OF_E = Column(Integer)
    F_OF_DP = Column(Integer)
    F_OF_TP = Column(Integer)
    F_LF_POS = Column(Integer)
    F_LF_G = Column(Integer)
    F_LF_GS = Column(Integer)
    F_LF_OUT = Column(Integer)
    F_LF_TC = Column(Integer)
    F_LF_PO = Column(Integer)
    F_LF_A = Column(Integer)
    F_LF_E = Column(Integer)
    F_LF_DP = Column(Integer)
    F_LF_TP = Column(Integer)
    F_CF_POS = Column(Integer)
    F_CF_G = Column(Integer)
    F_CF_GS = Column(Integer)
    F_CF_OUT = Column(Integer)
    F_CF_TC = Column(Integer)
    F_CF_PO = Column(Integer)
    F_CF_A = Column(Integer)
    F_CF_E = Column(Integer)
    F_CF_DP = Column(Integer)
    F_CF_TP = Column(Integer)
    F_RF_POS = Column(Integer)
    F_RF_G = Column(Integer)
    F_RF_GS = Column(Integer)
    F_RF_OUT = Column(Integer)
    F_RF_TC = Column(Integer)
    F_RF_PO = Column(Integer)
    F_RF_A = Column(Integer)
    F_RF_E = Column(Integer)
    F_RF_DP = Column(Integer)
    F_RF_TP = Column(Integer)
    F_C_POS = Column(Integer)
    F_C_G = Column(Integer)
    F_C_GS = Column(Integer)
    F_C_OUT = Column(Integer)
    F_C_TC = Column(Integer)
    F_C_PO = Column(Integer)
    F_C_A = Column(Integer)
    F_C_E = Column(Integer)
    F_C_DP = Column(Integer)
    F_C_TP = Column(Integer)
    F_C_PB = Column(Integer)
    F_C_XI = Column(Integer)
    F_P_POS = Column(Integer)
    F_P_G = Column(Integer)
    F_P_GS = Column(Integer)
    F_P_OUT = Column(Integer)
    F_P_TC = Column(Integer)
    F_P_PO = Column(Integer)
    F_P_A = Column(Integer)
    F_P_E = Column(Integer)
    F_P_DP = Column(Integer)
    F_P_TP = Column(Integer)

    def __init__(self, player_data: pd.Series):
        for key, value in player_data.items():
            if key is not None and value is not None:
                setattr(self, clean_db_col_names(key), native_dtype(value))

        self.UID = self._get_uid()

    def _get_uid(self):
        return "{}_{}".format(self.game_key, self.person_key)

class AsOfDatePlayerGameStats(_BASE):
    """"Calculate as of date player stats for quick retrieval
    """"

    __tablename__ = "tblAsOfDateStats"
    UID= Column(Integer)
    season = Column(Integer)
    asof_date = Column(DateTime)
    person_key = Column(String(8))
    B_G = Column(Integer)
    B_PA = Column(Integer)
    B_AB = Column(Integer)
    B_R = Column(Integer)
    B_H = Column(Integer)
    B_TB = Column(Integer)
    B_2B = Column(Integer)
    B_3B = Column(Integer)
    B_HR = Column(Integer)
    B_HR4 = Column(Integer)
    B_RBI = Column(Integer)
    B_GW = Column(Integer)
    B_BB = Column(Integer)
    B_IBB = Column(Integer)
    B_SO = Column(Integer)
    B_GDP = Column(Integer)
    B_HP = Column(Integer)
    B_SH = Column(Integer)
    B_SF = Column(Integer)
    B_SB = Column(Integer)
    B_CS = Column(Integer)
    B_XI = Column(Integer)
    B_G_DH = Column(Integer)
    B_G_PH = Column(Integer)
    B_G_PR = Column(Integer)
    P_G = Column(Integer)
    P_GS = Column(Integer)
    P_CG = Column(Integer)
    P_SHO = Column(Integer)
    P_GF = Column(Integer)
    P_W = Column(Integer)
    P_L = Column(Integer)
    P_SV = Column(Integer)
    P_OUT = Column(Integer)
    P_TBF = Column(Integer)
    P_AB = Column(Integer)
    P_R = Column(Integer)
    P_ER = Column(Integer)
    P_H = Column(Integer)
    P_TB = Column(Integer)
    P_2B = Column(Integer)
    P_3B = Column(Integer)
    P_HR = Column(Integer)
    P_HR4 = Column(Integer)
    P_BB = Column(Integer)
    P_IBB = Column(Integer)
    P_SO = Column(Integer)
    P_GDP = Column(Integer)
    P_HP = Column(Integer)
    P_SH = Column(Integer)
    P_SF = Column(Integer)
    P_XI = Column(Integer)
    P_WP = Column(Integer)
    P_BK = Column(Integer)
    P_IR = Column(Integer)
    P_IRS = Column(Integer)
    P_GO = Column(Integer)
    P_AO = Column(Integer)
    P_PITCH = Column(Integer)
    P_STRIKE = Column(Integer)
    F_1B_POS = Column(Integer)
    F_1B_G = Column(Integer)
    F_1B_GS = Column(Integer)
    F_1B_OUT = Column(Integer)
    F_1B_TC = Column(Integer)
    F_1B_PO = Column(Integer)
    F_1B_A = Column(Integer)
    F_1B_E = Column(Integer)
    F_1B_DP = Column(Integer)
    F_1B_TP = Column(Integer)
    F_2B_POS = Column(Integer)
    F_2B_G = Column(Integer)
    F_2B_GS = Column(Integer)
    F_2B_OUT = Column(Integer)
    F_2B_TC = Column(Integer)
    F_2B_PO = Column(Integer)
    F_2B_A = Column(Integer)
    F_2B_E = Column(Integer)
    F_2B_DP = Column(Integer)
    F_2B_TP = Column(Integer)
    F_3B_POS = Column(Integer)
    F_3B_G = Column(Integer)
    F_3B_GS = Column(Integer)
    F_3B_OUT = Column(Integer)
    F_3B_TC = Column(Integer)
    F_3B_PO = Column(Integer)
    F_3B_A = Column(Integer)
    F_3B_E = Column(Integer)
    F_3B_DP = Column(Integer)
    F_3B_TP = Column(Integer)
    F_SS_POS = Column(Integer)
    F_SS_G = Column(Integer)
    F_SS_GS = Column(Integer)
    F_SS_OUT = Column(Integer)
    F_SS_TC = Column(Integer)
    F_SS_PO = Column(Integer)
    F_SS_A = Column(Integer)
    F_SS_E = Column(Integer)
    F_SS_DP = Column(Integer)
    F_SS_TP = Column(Integer)
    F_OF_POS = Column(Integer)
    F_OF_G = Column(Integer)
    F_OF_GS = Column(Integer)
    F_OF_OUT = Column(Integer)
    F_OF_TC = Column(Integer)
    F_OF_PO = Column(Integer)
    F_OF_A = Column(Integer)
    F_OF_E = Column(Integer)
    F_OF_DP = Column(Integer)
    F_OF_TP = Column(Integer)
    F_LF_POS = Column(Integer)
    F_LF_G = Column(Integer)
    F_LF_GS = Column(Integer)
    F_LF_OUT = Column(Integer)
    F_LF_TC = Column(Integer)
    F_LF_PO = Column(Integer)
    F_LF_A = Column(Integer)
    F_LF_E = Column(Integer)
    F_LF_DP = Column(Integer)
    F_LF_TP = Column(Integer)
    F_CF_POS = Column(Integer)
    F_CF_G = Column(Integer)
    F_CF_GS = Column(Integer)
    F_CF_OUT = Column(Integer)
    F_CF_TC = Column(Integer)
    F_CF_PO = Column(Integer)
    F_CF_A = Column(Integer)
    F_CF_E = Column(Integer)
    F_CF_DP = Column(Integer)
    F_CF_TP = Column(Integer)
    F_RF_POS = Column(Integer)
    F_RF_G = Column(Integer)
    F_RF_GS = Column(Integer)
    F_RF_OUT = Column(Integer)
    F_RF_TC = Column(Integer)
    F_RF_PO = Column(Integer)
    F_RF_A = Column(Integer)
    F_RF_E = Column(Integer)
    F_RF_DP = Column(Integer)
    F_RF_TP = Column(Integer)
    F_C_POS = Column(Integer)
    F_C_G = Column(Integer)
    F_C_GS = Column(Integer)
    F_C_OUT = Column(Integer)
    F_C_TC = Column(Integer)
    F_C_PO = Column(Integer)
    F_C_A = Column(Integer)
    F_C_E = Column(Integer)
    F_C_DP = Column(Integer)
    F_C_TP = Column(Integer)
    F_C_PB = Column(Integer)
    F_C_XI = Column(Integer)
    F_P_POS = Column(Integer)
    F_P_G = Column(Integer)
    F_P_GS = Column(Integer)
    F_P_OUT = Column(Integer)
    F_P_TC = Column(Integer)
    F_P_PO = Column(Integer)
    F_P_A = Column(Integer)
    F_P_E = Column(Integer)
    F_P_DP = Column(Integer)
    F_P_TP = Column(Integer)

    def __init__(self, player_data: pd.Series):
        for key, value in player_data.items():
            if key is not None and value is not None:
                setattr(self, clean_db_col_names(key), native_dtype(value))

        self.UID = self._get_uid()

    def _get_uid(self):
        return "{}_{}".format(self.game_key, self.person_key)
