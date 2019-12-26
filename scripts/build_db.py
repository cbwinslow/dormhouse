"""
Builds a jaberwocky compliant database from scratch. 
The input arguments only included the start and end season that should be included. Defaults to 2017-2019
"""
import os
import sys

this_file = os.path.realpath(__file__)
sys.path.insert(1, os.path.realpath(os.path.join(this_file, "../..")))

from dormouse.tables.dbGame import populate_game_log, GameLog
from dormouse.tables.dbPerson import (
    populate_player_lu,
    populate_player_game_stats,
    populate_statcast,
    PlayerGameStats,
    PlayerLookup,
    StatcastPitching,
)

from sqlalchemy import create_engine, distinct, func
from sqlalchemy.orm import sessionmaker
import datetime


def _main(args):
    # TODO: Progress updates

    _start = args.start
    _end = args.end
    connecion = args.connection

    print(f"{_start}, {_end}")
    engine = create_engine(connecion)
    Session = sessionmaker(bind=engine)
    Session.configure(bind=engine)
    session = Session()

    # Create tables
    StatcastPitching.__table__.create(bind=engine, checkfirst=True)
    PlayerLookup.__table__.create(bind=engine, checkfirst=True)
    PlayerGameStats.__table__.create(bind=engine, checkfirst=True)
    GameLog.__table__.create(bind=engine, checkfirst=True)

    # populate player lookup table first
    # this is an 'all or nothing' deal

    if args.all or args.lookup:
        print("Populating player lookup")
        populate_player_lu(session)

    # Statcast pitching/hitting data
    if args.all or args.statcast:
        print("Populating statcast data")
        populate_statcast(
            datetime.datetime(day=1, month=3, year=_start),
            datetime.datetime(day=1, month=11, year=_end),
            session,
        )

    if args.all or args.gamelog:
        print("Populating game logs")
        for year in range(_start, _end + 1):
            print(f"season = {year}")
            populate_game_log(year, "rs", session)

    if args.all or args.retrosplits:
        print("Populating individual player game by game stats")
        populate_player_game_stats(_start, _end, session)
    # Commit and close
    session.commit()
    session.close()


if __name__ == "__main__":
    import argparse
    import datetime

    # TODO: The optional arguments are kinda broken. I need to fix them

    parser = argparse.ArgumentParser(
        description="Build a databse to be used by the bandersnatch simulation engine"
    )

    parser.add_argument(
        "connection",
        metavar="connection",
        type=str,
        help="The sqlalchemy connection string to use",
    )

    parser.add_argument(
        "--start",
        metavar="start_season",
        type=int,
        help="The first season to include in the database",
        default=2017,
    )

    parser.add_argument(
        "--end",
        metavar="end_season",
        type=int,
        help="The last season to include in the database",
        default=2019,
    )

    parser.add_argument(
        "--all",
        metavar="all_sources",
        type=bool,
        help="Fetch data from all sources",
        default=False,
    )

    parser.add_argument(
        "--lookup",
        metavar="lookup",
        type=bool,
        help="Fetch player lookup data",
        default=False,
    )

    parser.add_argument(
        "--statcast",
        metavar="statcast",
        type=bool,
        help="Fetch statcast data",
        default=False,
    )

    parser.add_argument(
        "--gamelog",
        metavar="gamelog",
        type=bool,
        help="Fetch gamelog data",
        default=False,
    )

    parser.add_argument(
        "--retrosplits",
        metavar="retrosplits",
        type=bool,
        help="Fetch retrosplits day-by-day data",
        default=False,
    )

    args = parser.parse_args()
    _main(args)
