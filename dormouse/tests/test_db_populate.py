import datetime
import os
import sys

# from sqlalchemy.ext.declarative import declarative_base
import unittest

import pandas as pd
from sqlalchemy import create_engine, distinct, func
from sqlalchemy.orm import sessionmaker

this_file = os.path.realpath(__file__)
sys.path.insert(1, os.path.realpath(os.path.join(this_file, "../../..")))

from dormouse.tables.dbPerson import (
    StatcastPitching,
    populate_statcast,
    PlayerLookup,
    populate_player_lu,
    PlayerGameStats,
    populate_player_game_stats,
)
from dormouse.tables.dbGame import GameLog, populate_game_log


class TestPlayerBPopulation(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        try:
            os.remove("test.db")
        except:  # This is a hacky way to do this
            pass

        engine = create_engine("sqlite:///test.db", echo=False)
        Session = sessionmaker(bind=engine)
        Session.configure(bind=engine)
        # Base = declarative_base()
        StatcastPitching.__table__.create(bind=engine, checkfirst=True)
        PlayerLookup.__table__.create(bind=engine, checkfirst=True)
        PlayerGameStats.__table__.create(bind=engine, checkfirst=True)
        self.session = Session()
        self.file_dir = os.path.realpath(__file__)

    @classmethod
    def tearDownClass(self):
        self.session.commit()
        self.session.close()

    # @unittest.skip("")
    def test_range_commit(self):
        with self.subTest("Initial test"):
            # TODO Check to make sure range overlap conditions are handled properly
            date_begin = datetime.datetime(day=1, month=4, year=2019)
            date_end = datetime.datetime(day=5, month=4, year=2019)
            populate_statcast(date_begin, date_end, self.session)

            self.assertEqual(
                self.session.query(StatcastPitching.home_team).count(), 17536
            )
        with self.subTest("Testing that overlapping ranges dont clash"):
            # This shouldnt result in any new additions to the db
            date_begin = datetime.datetime(day=2, month=4, year=2019)
            date_end = datetime.datetime(day=5, month=4, year=2019)
            populate_statcast(date_begin, date_end, self.session)

            self.assertEqual(
                self.session.query(StatcastPitching.home_team).count(), 17536
            )

    # @unittest.skip("")
    def test_plu_commit(self):
        populate_player_lu(self.session)
        self.assertTrue(
            self.session.query(PlayerLookup.key_mlbam).count(), 79130
        )

    def test_player_day_stats(self):
        populate_player_game_stats(2019, 2019, session=self.session)
        self.assertTrue(self.session.query(PlayerGameStats.UID).count(), 72879)
        # populate_player_game_stats(2017, 2018, session=self.session)


# @unittest.skip("")
class TestGameDBPopulate(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        try:
            os.remove("test2.db")
        except:  # This is a hacky way to do this
            pass

        engine = create_engine("sqlite:///test2.db", echo=False)
        Session = sessionmaker(bind=engine)
        Session.configure(bind=engine)
        # Base = declarative_base()
        GameLog.__table__.create(bind=engine, checkfirst=True)
        self.session = Session()
        self.file_dir = os.path.realpath(__file__)

    @classmethod
    def tearDownClass(self):
        self.session.commit()
        self.session.close()

    def test_rs_game_log(self):
        populate_game_log(2019, "rs", self.session)
        pass

    def test_line_score_parse(self):
        # TODO: Game log line score parsing
        pass


if __name__ == "__main__":
    unittest.main()
