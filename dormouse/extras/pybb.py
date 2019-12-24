"""
Extra functions that build on pybaseball. 
This should be removed if these functions are included in a future release of pybaseball
"""
import pandas as pd
from bs4 import BeautifulSoup
import requests
import numpy as np
import io
from datetime import datetime


def _single_player_soup(
    player_id, start_season, end_season, league, qual, ind
):
    url_base = "https://www.fangraphs.com/leaders.aspx?pos=all&stats=bat&lg={}&qual={}&type=c,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,-1&season={}&month=0&season1={}&ind={}&team=0&rost=0&age=0&filter=&players={}"
    # season={}&month=0&season1={}&ind=1&team=0&rost=0&age=0&filter=&players={}&startdate={}&enddate={}'
    url = url_base.format(
        league, qual, end_season, start_season, ind, player_id
    )
    # print(url)
    s = requests.get(url).content
    return BeautifulSoup(s, "lxml")


def _get_table(soup, ind):
    table = soup.find("table", {"class": "rgMasterTable"})
    data = []
    # pull heading names from the fangraphs tables
    headings = [row.text.strip() for row in table.find_all("th")[1:]]
    # rename the second occurrence of 'FB%' to 'FB% (Pitch)'
    FBperc_indices = [i for i, j in enumerate(headings) if j == "FB%"]
    headings[FBperc_indices[1]] = "FB% (Pitch)"

    table_body = table.find("tbody")
    rows = table_body.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols[1:]])

    data = pd.DataFrame(data=data, columns=headings)

    # replace empty strings with NaN
    data.replace(r"^\s*$", np.nan, regex=True, inplace=True)

    # convert percentage strings to floats   FB% duplicated
    percentages = [
        "Zone% (pi)",
        "Contact% (pi)",
        "Z-Contact% (pi)",
        "O-Contact% (pi)",
        "Swing% (pi)",
        "Z-Swing% (pi)",
        "O-Swing% (pi)",
        "XX% (pi)",
        "SL% (pi)",
        "SI% (pi)",
        "SB% (pi)",
        "KN% (pi)",
        "FS% (pi)",
        "FC% (pi)",
        "FA% (pi)",
        "CU% (pi)",
        "CS% (pi)",
        "CH% (pi)",
        "TTO%",
        "Hard%",
        "Med%",
        "Soft%",
        "Oppo%",
        "Cent%",
        "Pull%",
        "Zone% (pfx)",
        "Contact% (pfx)",
        "Z-Contact% (pfx)",
        "O-Contact% (pfx)",
        "Swing% (pfx)",
        "Z-Swing% (pfx)",
        "O-Swing% (pfx)",
        "UN% (pfx)",
        "KN% (pfx)",
        "SC% (pfx)",
        "CH% (pfx)",
        "EP% (pfx)",
        "KC% (pfx)",
        "CU% (pfx)",
        "SL% (pfx)",
        "SI% (pfx)",
        "FO% (pfx)",
        "FS% (pfx)",
        "FC% (pfx)",
        "FT% (pfx)",
        "FA% (pfx)",
        "SwStr%",
        "F-Strike%",
        "Zone%",
        "Contact%",
        "Z-Contact%",
        "O-Contact%",
        "Swing%",
        "Z-Swing%",
        "O-Swing%",
        "PO%",
        "XX%",
        "KN%",
        "SF%",
        "CH%",
        "CB%",
        "CT%",
        "SL%",
        "FB%",
        "BUH%",
        "IFH%",
        "HR/FB",
        "IFFB%",
        "FB% (Pitch)",
        "GB%",
        "LD%",
        "GB/FB",
        "K%",
        "BB%",
    ]
    for col in percentages:
        # skip if column is all NA (happens for some of the more obscure stats + in older seasons)
        if not data[col].empty:
            if pd.api.types.is_string_dtype(data[col]):
                data[col] = data[col].str.strip(" %")
                data[col] = data[col].str.strip("%")
                data[col] = data[col].astype(float) / 100.0
        else:
            # print(col)
            pass

    # convert everything except name and team to numeric (Dol?)
    cols_to_numeric = [
        col
        for col in data.columns
        if col not in ["Name", "Team", "Age Rng", "Dol"]
    ]
    data[cols_to_numeric] = data[cols_to_numeric].astype(float)

    # sort by WAR and OPS so best players float to the top
    data = data.sort_values(["WAR", "OPS"], ascending=False)
    return data


def single_player_batting_stats(
    player_id, start_season, end_season=None, league="all", qual=1, ind=1
):
    if start_season is None:
        raise ValueError(
            "You need to provide at least one season to collect data for. Try pitching_leaders(season) or pitching_leaders(start_season, end_season)."
        )
    if end_season is None:
        end_season = start_season
    soup = _single_player_soup(
        start_season=start_season,
        end_season=end_season,
        player_id=player_id,
        league=league,
        qual=qual,
        ind=ind,
    )
    table = _get_table(soup, ind)
    return table


def retro_day_stats(start_season, end_season=None, agg_type="playing"):
    """
    Pull day by data stats from the chadwick repository on github. The data orignates from retrosheets event files
    :param start_season: The first season to pull
    :type int, required
    :param end_season: The last season to pull
    :type int, optional
    :param agg_type: The aggregation type, either "playing" or "team"
    :type str, optional
    """

    def _fix_date(dt_string):
        """
        Convert YYYY-MM-DD integer to datetime object
        """
        dt_string = str(dt_string)
        dt_string = dt_string.split("-")
        return datetime(
            year=int(dt_string[0]),
            month=int(dt_string[1]),
            day=int(dt_string[2]),
        )

    def _pull_rs_github(season, agg_type):
        base_url = "https://raw.githubusercontent.com/chadwickbureau/retrosplits/master/daybyday/{}-{}.csv"
        r = requests.get(base_url.format(agg_type, season))
        return pd.read_csv(io.StringIO(r.text))

    if agg_type not in ["playing", "team"]:
        raise ValueError(f"{agg_type} not recognized")

    if end_season == None:
        end_season = start_season

    [start_season, end_season] = [int(x) for x in [start_season, end_season]]
    df = pd.DataFrame()

    for season in range(start_season, end_season + 1):
        df = df.append(_pull_rs_github(season, agg_type), ignore_index=True)

    df["game.date"] = df["game.date"].apply(_fix_date)
    df["appear.date"] = df["appear.date"].apply(_fix_date)
    return df
