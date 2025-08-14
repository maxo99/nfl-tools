from enum import Enum


class NFLTeam(Enum):
    AZ = ("AZ", "Arizona", "Cardinals")
    ATL = ("ATL", "Atlanta", "Falcons")
    BAL = ("BAL", "Baltimore", "Ravens")
    BUF = ("BUF", "Buffalo", "Bills")
    CAR = ("CAR", "Carolina", "Panthers")
    CHI = ("CHI", "Chicago", "Bears")
    CIN = ("CIN", "Cincinnati", "Bengals")
    CLE = ("CLE", "Cleveland", "Browns")
    DAL = ("DAL", "Dallas", "Cowboys")
    DEN = ("DEN", "Denver", "Broncos")
    DET = ("DET", "Detroit", "Lions")
    GB = ("GB", "Green Bay", "Packers")
    HOU = ("HOU", "Houston", "Texans")
    IND = ("IND", "Indianapolis", "Colts")
    JAC = ("JAC", "JAX","Jacksonville", "Jaguars")
    KC = ("KC", "Kansas City", "Chiefs")
    LV = ("LV", "OAK", "Las Vegas", "Raiders")
    LAC = ("LAC", "SD", "Los Angeles", "Chargers")
    LAR = ("LAR", "STL", "Los Angeles", "Rams")
    MIA = ("MIA", "Miami", "Dolphins")
    MIN = ("MIN", "Minnesota", "Vikings")
    NE = ("NE", "New England", "Patriots")
    NO = ("NO", "New Orleans", "Saints")
    NYG = ("NYG", "New York", "Giants")
    NYJ = ("NYJ", "New York", "Jets")
    PHI = ("PHI", "Philadelphia", "Eagles")
    PIT = ("PIT", "Pittsburgh", "Steelers")
    SF = ("SF", "San Francisco", "49ers")
    SEA = ("SEA", "Seattle", "Seahawks")
    TB = ("TB", "Tampa Bay", "Buccaneers")
    TEN = ("TEN", "Tennessee", "Titans")
    WAS = ("WAS", "Washington", "Commanders")



NEWS_RSS_FEEDS = {
    "ESPN_FEED": "https://www.espn.com/espn/rss/nfl/news",
    "CBS_FEED": "https://www.cbssports.com/rss/headlines/nfl/",
    "YAHOO_FEED": "https://sports.yahoo.com/nfl/rss.xml",
    "SB_NATION_FEED": "https://www.sbnation.com/rss/nfl/index.xml",
}


# TEAMS = {
#     "AZ": ["Arizona", "Cardinals"],
#     "ATL": ["Atlanta", "Falcons"],
#     "BAL": ["Baltimore", "Ravens"],
#     "BUF": ["Buffalo", "Bills"],
#     "CAR": ["Carolina", "Panthers"],
#     "CHI": ["Chicago", "Bears"],
#     "CIN": ["Cincinnati", "Bengals"],
#     "CLE": ["Cleveland", "Browns"],
#     "DAL": ["Dallas", "Cowboys"],
#     "DEN": ["Denver", "Broncos"],
#     "DET": ["Detroit", "Lions"],
#     "GB": ["Green Bay", "Packers"],
#     "HOU": ["Houston", "Texans"],
#     "IND": ["Indianapolis", "Colts"],
#     "JAC": ["Jacksonville", "Jaguars"],
#     "KC": ["Kansas City", "Chiefs"],
#     "LV": ["Las Vegas", "Raiders"],
#     "LAC": ["Los Angeles", "Chargers"],
#     "LAR": ["Los Angeles", "Rams"],
#     "MIA": ["Miami", "Dolphins"],
#     "MIN": ["Minnesota", "Vikings"],
#     "NE": ["New England", "Patriots"],
#     "NO": ["New Orleans", "Saints"],
#     "NYG": ["New York", "Giants"],
#     "NYJ": ["New York", "Jets"],
#     "PHI": ["Philadelphia", "Eagles"],
#     "PIT": ["Pittsburgh", "Steelers"],
#     "SF": ["San Francisco", "49ers"],
#     "SEA": ["Seattle", "Seahawks"],
#     "TB": ["Tampa Bay", "Buccaneers"],
#     "TEN": ["Tennessee", "Titans"],
#     "WAS": ["Washington", "Commanders"],
# }
