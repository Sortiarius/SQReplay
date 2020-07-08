import mpyq, json, os, sys, glob, re, urllib.request, sqlite3, argparse, platform
from s2protocol import versions
from event import Event
from tracker import TrackedUnits
from pathlib import Path
from tqdm import tqdm

req = urllib.request.urlopen("https://gist.github.com/Sortiarius/31b6399ec0c9c5a5be46f9c80b9d19a5/raw").read().decode()
PATCHES = json.loads(req)

ATTRIBUTES_GAMEMODE = {
    "0001": "Select",
    "0002": "Dynamic",
    "0003": "Classic",
    "0004": "Full",
    "0005": "Arena",
    "0006": "Draft"
}

ATTRIBUTES_CREEPMULT = {
    "0002": "1x",
    "0003": "3x"
}

db = sqlite3.connect("./squadron.db")
c = db.cursor()


def patch(timestamp) -> str:
    max = PATCHES[len(PATCHES) - 1]
    max_date = max['time']

    min = PATCHES[0]

    if max_date <= timestamp:
        return max['patch']

    if timestamp < min['time']:
        return "_INVALID"

    index = len(PATCHES) - 2
    min_patch = PATCHES[index + 1]

    while index >= 0:
        if PATCHES[index + 1]['time'] > timestamp:
            min_patch = PATCHES[index]
        index -= 1

    return min_patch['patch']


def closest_version(n, versions) -> (int, int):
    seq = list(map(int, re.findall('\\d+', ''.join(versions.list_all()))))
    lst = sorted(seq + [n])
    min_index = lst.index(n) - 1
    max_index = min(min_index + 2, len(lst) - 1)
    return seq[min_index], lst[max_index]


def team(tid) -> str:
    if tid == 0:
        return "East"
    if tid == 1:
        return "West"


class Replay:

    def __init__(self, path, debug):
        self.DEBUG = debug

        self.invalid = False
        try:
            self.archive = mpyq.MPQArchive(path)
        except:
            self.invalid = True
            return

        self.archive_contents = self.archive.header['user_data_header']['content']

        _header = versions.latest().decode_replay_header(self.archive_contents)
        self.build = _header['m_version']['m_baseBuild']

        try:
            self.protocol = versions.build(self.build)
        except:
            next_lower = closest_version(self.build, versions)[0]
            self.protocol = versions.build(next_lower)

        try:
            self.meta = json.loads(self.archive.read_file('replay.gamemetadata.json').decode('utf-8'))
        except:
            self.invalid = True
            return

        self._events = None
        self._tracker_events = None
        self._init_data = None
        self._details = None
        self._attribute_events = None

        self.tracked_units = TrackedUnits()
        self.game_ended = False
        self.winner = None

        self.players = []

        self._game = {"wave": 0, "builders": {}}
        self.towers = []
        self.sends = []
        self.buildersByWave = []
        self.buildersOnWave = {}
        self.workers = []
        self.speedUps = []

        self.gasNumber = {}
        self.upgradeNumber = {}

        self.filename = path

        self._db = None

    @property
    def game_events(self):
        if self._events is None:
            contents = self.archive.read_file('replay.game.events')
            for event in self.protocol.decode_replay_game_events(contents):
                event = Event(**event)
                yield event

        return self._events

    @property
    def tracker_events(self):
        if self._tracker_events is None:
            contents = self.archive.read_file('replay.tracker.events')
            for event in self.protocol.decode_replay_tracker_events(contents):
                yield Event(**event)

    @property
    def init_data(self):
        if self._init_data is None:
            contents = self.archive.read_file('replay.initData')
            self._init_data = self.protocol.decode_replay_initdata(contents)
        return self._init_data

    @property
    def details(self):
        if self._details is None:
            contents = self.archive.read_file('replay.details')
            self._details = self.protocol.decode_replay_details(contents)
        return self._details

    @property
    def game_id(self):
        for e in self.game_events:
            if e.get('_event') == 'NNet.Game.SSetSyncLoadingTimeEvent':
                return e.get('m_syncTime')

    @property
    def attribute_events(self):
        if self._attribute_events is None:
            contents = self.archive.read_file('replay.attributes.events')
            self._attribute_events = self.protocol.decode_replay_attributes_events(contents)
        return self._attribute_events

    def game_type(self, mode) -> str:

        b = self._game['builders']

        if mode == "Select":
            return "Select"

        if mode == "Draft":
            return "Draft"

        if mode == "Dynamic":

            if "RandomCustomBuilder" in b:
                return "Random Refined"

            cr = True
            for wave in self.buildersOnWave:
                if len(set(self.buildersOnWave[wave])) != 1:
                    cr = False

            if cr:
                return "Chaos Refined"
            else:
                return "Chaos"

        return "_INVALID"

    def create_uid_pid_mapping(self):
        """
        Make this a bit faster and terminate when we have enough
        Returns
        -------
        """
        # Set this flag to true when we start hitting the PlayerSetupEvents
        last_index = None
        uid_pid_mapping = {}
        for index, event in enumerate(self.tracker_events):
            if event.player_setup:
                last_index = index
                uid_pid_mapping[event['m_userId']] = event['m_playerId']
            if last_index is not None and last_index != index:
                # A new Non Player setup event. We've looped through all.
                # break out so we don't consume the entire generator.
                break
        return uid_pid_mapping

    def load_players(self):
        player_container = {}
        team_container = {}
        name_pattern = re.compile(r'&lt;.*<sp/>')

        slot_uid_mapping = {
            e['m_workingSetSlotId']: e['m_userId']
            for e in self.init_data['m_syncLobbyState']['m_lobbyState']['m_slots']
        }
        uid_pid_mapping = self.create_uid_pid_mapping()

        for p in self.details.get('m_playerList', []):
            profile_id = "{m_region}-S2-{m_realm}-{m_id}".format(**p['m_toon'])
            color = (
                p['m_color']['m_r'],
                p['m_color']['m_g'],
                p['m_color']['m_b'],
            )
            name = p['m_name'].decode('utf-8')
            name = re.sub(name_pattern, "", name)

            slot_id = p['m_workingSetSlotId']
            user_id = slot_uid_mapping.get(slot_id)
            player_id = uid_pid_mapping.get(slot_uid_mapping.get(slot_id))

            player = {
                "name": name,
                "handle": profile_id,
                "user_id": user_id,
                "player_id": player_id,
                "color": (p['m_color']['m_r'], p['m_color']['m_g'], p['m_color']['m_b']),
                "team": team(p['m_teamId'])
            }

            self.players.append(player)

    def player(self, pid: int) -> any:
        for player in self.players:
            if player['player_id'] == pid:
                return player

    def handle_upgrade(self, event):
        if event['m_upgradeTypeName'] == "RefinerySpeed":
            player = self.player(event['m_playerId'])
            handle = player['handle']
            if handle not in self.upgradeNumber:
                self.upgradeNumber[handle] = 1

            upgrade = {
                "player": player['player_id'],
                "wave": self._game['wave'],
                "number": self.upgradeNumber[handle]
            }
            self.speedUps.append(upgrade)
            self.upgradeNumber[handle] += 1

    def handle_death(self, event: Event, init_event: Event):
        owner = self.player(init_event['m_controlPlayerId'])
        killer = self.player(event.get('m_killerPlayerId'))

        if init_event.is_unit("SecuritySystem"):
            if event.position == 77:
                self.winner = "East"
                self.game_ended = True

            if event.position == 67:
                self.winner = "West"
                self.game_ended = True

    def handle_unit(self, event):
        unit_type = event['m_unitTypeName']

        # Calculate the wave the game ends on.
        if unit_type[0:4] == "Wave":
            self._game['wave'] = int(unit_type[4:])

        # Workers
        if unit_type == "SquadronWorker":
            if event["m_controlPlayerId"] not in self.gasNumber:
                self.gasNumber[event["m_controlPlayerId"]] = 1

            gas_event = {
                "player": event["m_controlPlayerId"],
                "wave": self._game['wave'],
                "number": self.gasNumber[event["m_controlPlayerId"]]
            }
            self.workers.append(gas_event)
            self.gasNumber[event["m_controlPlayerId"]] += 1

        # Builders
        if unit_type[-7:] == "Builder":
            if unit_type not in self._game['builders']:
                self._game['builders'][unit_type] = 1
            else:
                self._game['builders'][unit_type] += 1

            builder = {
                "player": event["m_controlPlayerId"],
                "type": unit_type,
                "wave": self._game['wave']
            }
            self.buildersByWave.append(builder)

            if self._game['gamemode'] == "Dynamic":
                wave = str(self._game['wave'])
                if wave not in self.buildersOnWave:
                    self.buildersOnWave[wave] = []
                self.buildersOnWave[wave].append(unit_type)

        if event["m_controlPlayerId"] in [13, 14, 0]:
            return

        # Tower
        if unit_type[0] == "f":
            tower = {
                "builder": event["m_controlPlayerId"],
                "type": unit_type,
                "wave": self._game['wave']
            }
            self.towers.append(tower)

        # Send
        if unit_type[0:5] == "Send_":
            send = {
                "player": event["m_controlPlayerId"],
                "type": unit_type,
                "wave": self._game['wave'] + 1
            }
            self.sends.append(send)

    def read(self) -> (bool, any):

        if self.invalid:
            return True, "Invalid MPQ"

        if self.meta['Title'] not in ['Squadron TD', 'Squadron TD Beta']:
            return True, "Unsupported Game"

        if self.DEBUG:
            print(f"Parsing game {self.game_id} | {self.filename}")

        if patch(self.game_id) == "_INVALID":
            return True, "Outdated Patch"

        self.load_players()

        if len(self.players) < 4:
            return True, "Not Enough Players"

        gamemode = self.attribute_events['scopes'][16][6][0]['value'].decode()
        if gamemode not in ATTRIBUTES_GAMEMODE:
            return True, "Unsupported Gamemode"
        self._game['gamemode'] = ATTRIBUTES_GAMEMODE[gamemode]

        creeps = self.attribute_events['scopes'][16][2][0]['value'].decode()

        if creeps not in ATTRIBUTES_CREEPMULT:
            return True, "Unsupported Creep Mode"

        for event in self.tracker_events:

            init_event = self.tracked_units.fetch(event)

            if event.unit_init or event.unit_born:
                self.tracked_units.add(event)
                self.handle_unit(event)
            if event.upgrade_event:
                self.handle_upgrade(event)
            if event.unit_died and init_event is not None:
                self.handle_death(event, init_event)

        if not self.game_ended:
            return True, "Game did not end or was Cooperative."

        gamemode = self.game_type(ATTRIBUTES_GAMEMODE[gamemode])
        if gamemode == "_INVALID":
            return True, "Unable to Parse Gamemode"

        for player in self.players:
            if player['team'] == self.winner:
                player['won'] = True
            else:
                player['won'] = False

        game = {
            "id": self.game_id,
            "patch": patch(self.game_id),
            "creeps": ATTRIBUTES_CREEPMULT[creeps],
            "gamemode": gamemode,
            "end_wave": self._game['wave'],
            "players": self.players,
            "towers": self.towers,
            "sends": self.sends,
            "buildersByWave": self.buildersByWave,
            "workers": self.workers,
            "upgrades": self.speedUps
        }

        self._db = game

        return False, game

    def insert(self):
        game_id = self.game_id

        for row in c.execute("SELECT * FROM Games WHERE id = ?", (game_id,)):
            if row[0] == game_id:
                if self.DEBUG:
                    print(f"Game {game_id} Already Found in Database.")
                return

        c.execute(
            "INSERT INTO Games(ID, PATCH, CREEPS, GAMEMODE, END) VALUES(?,?,?,?,?)",
            (self.game_id, patch(self.game_id), self._db['creeps'], self._db['gamemode'], self._db['end_wave'])
        )

        for player in self._db['players']:
            add_player = True

            for row in c.execute("SELECT * FROM Players WHERE HANDLE = ?", (player['handle'],)):
                if row[0] == player['handle']:
                    add_player = False
                    if game_id > row[2]:
                        c.execute(
                            "UPDATE Players SET NAME = ?, LATEST_GAME = ? WHERE HANDLE = ?",
                            (player['name'], game_id, player['handle'])
                        )

            if add_player:
                c.execute(
                    "INSERT INTO Players(HANDLE, NAME, LATEST_GAME) VALUES(?,?,?)",
                    (player['handle'], player['name'], game_id)
                )

            won = 0
            if player['won']:
                won = 1

            c.execute(
                "INSERT INTO GamePlayers(PLAYER_ID,TEAM,WON,PLAYER_HANDLE,GAME_ID) VALUES(?,?,?,?,?)",
                (player['player_id'], player['team'], won, player['handle'], game_id)
            )

        for tower in self.towers:
            player_handle = self.player(tower['builder'])['handle']
            c.execute(
                "INSERT INTO Towers(WAVE, TOWER_TYPE, GAME_ID, HANDLE, PLAYER_ID) VALUES(?,?,?,?,?)",
                (tower['wave'], tower['type'], game_id, player_handle, tower['builder'])
            )

        for send in self.sends:
            player_handle = self.player(send['player'])['handle']
            c.execute(
                "INSERT INTO Sends(WAVE, SEND_TYPE, GAME_ID, HANDLE, PLAYER_ID) VALUES(?,?,?,?,?)",
                (send['wave'], send['type'], game_id, player_handle, send['player'])
            )

        for builder in self.buildersByWave:
            player_handle = self.player(builder['player'])['handle']
            c.execute(
                "INSERT INTO Builders(WAVE, BUILDER, GAME_ID, HANDLE, PLAYER_ID) VALUES (?,?,?,?,?)",
                (builder['wave'], builder['type'], game_id, player_handle, builder['player'])
            )

        for worker in self.workers:
            player_handle = self.player(worker['player'])['handle']
            c.execute(
                "INSERT INTO Workers(WORKER_NUMBER, WORKER_WAVE, GAME_ID, HANDLE, PLAYER_ID) VALUES (?,?,?,?,?)",
                (worker['number'], worker['wave'], game_id, player_handle, worker['player'])
            )

        for upgrade in self.speedUps:
            player_handle = self.player(upgrade['player'])['handle']
            c.execute(
                "INSERT INTO Upgrades(UPGRADE_NUMBER, UPGRADE_WAVE, GAME_ID, HANDLE, PLAYER_ID) VALUES (?,?,?,?,?)",
                (upgrade['number'], upgrade['wave'], game_id, player_handle, upgrade['player'])
            )

        db.commit()


def main():

    parser = argparse.ArgumentParser(description="Squadron TD Game Parser - Processes .SC2Replay files.")
    parser.add_argument('--debug', metavar="d", type=bool, help="Activates Debug Mode.", required=False)
    parser.add_argument("--path", metavar="p", type=str, help="Path to Starcraft 2 Folder", required=False)

    args = parser.parse_args()
    DEBUG = False
    if args.debug:
        DEBUG = True

    with open("schema.sql", "r") as f:
        schema = f.read()

    c.executescript(schema)

    if not args.path:
        user = os.path.expanduser("~")
        if platform.system() == "Windows":
            file_path = user + "\\Documents\\Starcraft II"
        elif platform.system() == "Darwin":
            file_path = user + "/Library/Application Support/Blizzard/Starcraft II"
        else:
            print("Cannot find Starcraft II folder. Run program again with --path set.")
            return
    else:
        file_path = args.path

    total = len(list(Path(file_path).rglob('*.SC2Replay')))

    for file in tqdm(Path(file_path).rglob('*.SC2Replay'), total=total):
        replay = Replay(file, DEBUG)

        if replay.invalid:
            if DEBUG:
                print(f"Invalid MPQ: {file}".encode('utf-8'))
            continue

        try:
            replay_id = replay.game_id
        except:
            if DEBUG:
                print(f"Invalid MPQ Events: {file}".encode('utf-8'))

        for row in c.execute("SELECT * FROM Games WHERE ID = ?", (replay_id,)):
            if row[0] == replay_id:
                if DEBUG:
                    print(f"Replay already in Database: {file}")
                continue

        _err, data = replay.read()
        if _err:
            if DEBUG:
                print(f"Invalid Replay: {file}".encode('utf-8'))
            continue

        replay.insert()

    db.close()


if __name__ == "__main__":
    main()