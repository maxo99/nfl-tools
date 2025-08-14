from collections import defaultdict

import feedparser
import pydantic

from nfltools.constants import NEWS_RSS_FEEDS, NFLTeam


class FeedEntry(pydantic.BaseModel):
    feed_id: str
    id: str
    title: str
    summary: str
    published: str
    author: str
    link: str
    teams: list[NFLTeam] = []
    players: list[NFLTeam] = []


class FeedData(pydantic.BaseModel):
    entries: list[FeedEntry] = pydantic.Field(default_factory=list)
    _team_cache: dict[NFLTeam, list[FeedEntry]] = defaultdict(list)

    class Config:
        arbitrary_types_allowed = True

    def add_entry(self, entry: FeedEntry):
        if entry not in self.entries and isinstance(self.entries, list):
            self.entries.append(entry)
        if not entry.teams:
            print(f"Entry {entry.id} has no teams")
        for t in entry.teams:
            self._team_cache[t].append(entry)

    def get_entries_by_team(self, team: str) -> list[FeedEntry]:
        if team not in self._team_cache:
            return []
        return self._team_cache[team]


def process_feed(feed_url, count=5):
    try:
        feed = feedparser.parse(feed_url)
        if feed.bozo:
            raise ValueError(f"Error fetching feed: {feed.bozo_exception}")
        print(f"Feed channel: {feed.channel}")
        for entry in feed.entries[:count]:
            print(f"{entry.published}: {entry.title} ({entry.link})")
    except Exception as e:
        print(f"Failed to process feed {feed_url}: {e}")


def collect_feed_data() -> FeedData:
    fd = FeedData()
    for name, url in NEWS_RSS_FEEDS.items():
        print(f"Fetching {name} from {url}")
        feed = feedparser.parse(url)
        if feed.bozo:
            print(f"Error fetching {name}: {feed.bozo_exception}")
            continue

        for entry in feed.entries:
            if isinstance(entry, dict):
                # Ensure all required keys exist before creating FeedEntry
                required_keys = [
                    "id",
                    "title",
                    "summary",
                    "published",
                    # "author",
                    "link",
                ]
                if all(k in entry for k in required_keys):
                    entry = FeedEntry(
                        feed_id=name,
                        id=str(entry.get("id", "")),
                        title=str(entry.get("title", "")),
                        summary=str(entry.get("summary", "")),
                        published=str(entry.get("published", "")),
                        author=str(entry.get("author", "Unknown")),
                        link=str(entry.get("link", "")),
                    )



                    entry.teams = [
                        team
                        for team in NFLTeam
                        if any(name in entry.title for name in team.value)
                        or any(name in entry.summary for name in team.value)
                    ]

                    fd.add_entry(entry)
                else:
                    print(f"Skipping entry missing required keys: {entry}")
                # FeedEntry(
                #     feed_id=name,
                #     id=entry.get("id",""),
                #     title=entry.title,
                #     summary=entry.summary,
                #     published=entry.published,
                #     author=entry.author if "author" in entry else "Unknown",
                #     link=entry.link,
                # )
                # d = {
                #     "id": entry.id,
                #     "title": entry.title,
                #     "summary": entry.summary,
                #     "published": entry.published,
                #     "author": entry.author if "author" in entry else "Unknown",
                #     "link": entry.link,
                # }
                print(entry)
        print(f"Collected {len(fd.entries)} entries from {name}")
    return fd
