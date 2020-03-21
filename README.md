**

# Dormouse Baseball Database

Dormouse is a consolidated baseball database specifically designed for simulating baseball games at the pitch-by-pitch level. Dormouse contains table definitions and web-scraping functionality for the following data sources: 

- Statcast pitch data
- Chadwick Bureau player information
- Retrosheets game logs
- Retrosheets single game level player statistics
- Retrosheets team rosters
- Team metadata (Custom)

## Getting Started

To populate a database with data simply call `build_db.py`with the date range and subset of data sources you would like to include. A full help menu can be found via the *--help* argument.

For example, if you'd like to build a local sqlite database with all the relevant baseball data from the 2017 through 2019 seasons use the following arguments.

`python scripts/build_db.py "sqlite:///master.db" --start 2017 --end 2019 --all True`

The connection string for the build_db function follows standard SQLAlchemy conventions.

Relevant population functions can be found in the *tables/* directory. The documentation for these functions is very incomplete but I will make every attempt to update it as I find the time. All functions rely on SQLAlchemy sessions. The most helpful examples of how to use all the population functions can be found in the tests module.

## Schema Documentation

Documentation for all column data can be acquired from the original data sources.

- [Statcast Pitching](https://baseballsavant.mlb.com/csv-docs)
- [Chadwick Bereau Player Data](https://github.com/chadwickbureau/register)
- [Retrosheets Game Logs](https://www.retrosheet.org/gamelogs/index.html)
- [Retroheets Individual Player Single Game Stats](https://github.com/chadwickbureau/retrosplits)
- [Retrosheets Team Rosters](https://www.retrosheet.org/game.htm)

## Acknowledgements

The folks over at Chadwick Bureau and Retrosheets are doing to Lord's work. You should consider supporting them with either donations or resolving some outstanding issues with their data. The Retrosheets data that this library depends on is released under the following license:

> Recipients of Retrosheet data are free to make any desired use of the 
> information, including (but not limited to) selling it, giving it
> away, or  producing a commercial product based upon the data.
> Retrosheet has one requirement for any such transfer of data or
> product development, which is that the following statement must appear
> prominently
> 
> The information used here was obtained free of  charge from and is
> copyrighted by Retrosheet.  Interested  parties may contact Retrosheet
> at 20 Sunset Rd.,  Newark, DE 19711.
