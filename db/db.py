import sqlite3
import threading
from datetime import datetime
import json
from typing import List, Tuple, Optional

class Database:
    """
    싱글톤 패턴을 사용하여 SQLite 데이터베이스에 접근하는 클래스입니다.
    """

    _instance: Optional['Database'] = None
    _lock: threading.Lock = threading.Lock()
    _dcl: bool = False

    def __new__(cls) -> 'Database':
        """
        싱글톤 인스턴스를 생성합니다.
        인스턴스가 없으면 새로운 인스턴스를 생성하고 데이터베이스 연결을 초기화합니다.
        """
        if cls._instance is None and not cls._dcl:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(Database, cls).__new__(cls)
                    cls._instance.connection = sqlite3.connect('db.sqlite3', check_same_thread=False)
                    cls._instance.cursor = cls._instance.connection.cursor()
                    cls._instance._initialize_database()
                    cls._dcl = True
        return cls._instance
    
    def __del__(self) -> None:
        """데이터베이스 연결을 닫습니다."""
        try:
            self.connection.close()
        except AttributeError:
            pass

    def __enter__(self) -> 'Database':
        """컨텍스트 매니저 진입 시 호출됩니다."""
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[object]) -> None:
        """컨텍스트 매니저 종료 시 호출됩니다."""
        try:
            self.connection.close()
        except AttributeError:
            pass

    def _initialize_database(self) -> None:
        """데이터베이스 테이블을 초기화합니다."""
        self.cursor.executescript('''
            CREATE TABLE IF NOT EXISTS Users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                handle TEXT UNIQUE NOT NULL,
                solved_count INTEGER NOT NULL,
                solved_problems TEXT NOT NULL,
                last_check_time TIMESTAMP NOT NULL,
                last_update_time TIMESTAMP NOT NULL
            );

            CREATE TABLE IF NOT EXISTS Servers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS Channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                server_id INTEGER NOT NULL,
                FOREIGN KEY (server_id) REFERENCES Servers(id)
            );

            CREATE TABLE IF NOT EXISTS UserChannelMapping (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                server_id INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES Users(id),
                FOREIGN KEY (channel_id) REFERENCES Channels(id),
                FOREIGN KEY (server_id) REFERENCES Servers(id)
            );
        ''')
        self.connection.commit()

    def add_user(self, handle: str) -> None:
        """
        새로운 유저를 데이터베이스에 추가합니다.

        Args:
            handle (str): 유저 핸들
        """
        current_time = datetime.now()
        self.cursor.execute('''
            INSERT INTO Users (handle, solved_count, solved_problems, last_check_time, last_update_time)
            VALUES (?, 0, ?, ?, ?)
        ''', (handle, json.dumps([]), current_time, current_time))
        self.connection.commit()

    def add_server(self, name: str) -> None:
        """
        새로운 서버를 데이터베이스에 추가합니다.

        Args:
            name (str): 서버 이름
        """
        self.cursor.execute('''
            INSERT INTO Servers (name)
            VALUES (?)
        ''', (name,))
        self.connection.commit()

    def add_channel(self, name: str, server_id: int) -> None:
        """
        새로운 채널을 데이터베이스에 추가합니다.

        Args:
            name (str): 채널 이름
            server_id (int): 해당 채널이 속한 서버의 ID
        """
        self.cursor.execute('''
            INSERT INTO Channels (name, server_id)
            VALUES (?, ?)
        ''', (name, server_id))
        self.connection.commit()

    def map_user_to_channel(self, handle: str, channel_id: int, server_id: int) -> None:
        """
        유저를 특정 채널과 서버에 매핑합니다.

        Args:
            handle (str): 유저 핸들
            channel_id (int): 채널의 ID
            server_id (int): 서버의 ID
        """
        user_id = self.get_user_id_by_handle(handle)
        if user_id is not None:
            self.cursor.execute('''
                INSERT INTO UserChannelMapping (user_id, channel_id, server_id)
                VALUES (?, ?, ?)
            ''', (user_id, channel_id, server_id))
            self.connection.commit()
        else:
            raise ValueError(f"유저 핸들 '{handle}'에 해당하는 유저를 찾을 수 없습니다.")

    def update_user_solved_problems(self, handle: str, solved_problems: List[int]) -> None:
        """
        유저의 문제 풀이 정보를 업데이트합니다.

        Args:
            handle (str): 유저 핸들
            solved_problems (list): 유저가 푼 문제 번호 목록
        """
        current_time = datetime.now()
        solved_count = len(solved_problems)
        self.cursor.execute('''
            UPDATE Users
            SET solved_count = ?, solved_problems = ?, last_update_time = ?
            WHERE handle = ?
        ''', (solved_count, json.dumps(solved_problems), current_time, handle))
        self.connection.commit()

    def get_user_channels(self, handle: str) -> List[Tuple[int, str, int, str]]:
        """
        유저가 등록된 채널 목록을 가져옵니다.

        Args:
            handle (str): 유저 핸들

        Returns:
            list: 유저가 등록된 채널 목록 (채널 ID, 채널 이름, 서버 ID, 서버 이름)
        """
        self.cursor.execute('''
            SELECT Channels.id, Channels.name, Servers.id, Servers.name
            FROM UserChannelMapping
            INNER JOIN Users ON UserChannelMapping.user_id = Users.id
            INNER JOIN Channels ON UserChannelMapping.channel_id = Channels.id
            INNER JOIN Servers ON UserChannelMapping.server_id = Servers.id
            WHERE Users.handle = ?
        ''', (handle,))
        return self.cursor.fetchall()

    def get_user_id_by_handle(self, handle: str) -> Optional[int]:
        """
        유저 핸들을 이용해 유저 ID를 조회합니다.

        Args:
            handle (str): 유저 핸들

        Returns:
            Optional[int]: 유저 ID
        """
        self.cursor.execute('''
            SELECT id FROM Users WHERE handle = ?
        ''', (handle,))
        result = self.cursor.fetchone()
        return result[0] if result else None

if __name__ == '__main__':
    with Database() as db:
        # 예제 데이터 추가 및 메소드 테스트
        db.add_user('user1')
        db.add_server('Server1')
        db.add_server('Server2')

        db.add_channel('Channel1', 1)
        db.add_channel('Channel2', 1)
        db.add_channel('Channel3', 2)

        db.map_user_to_channel('user1', 1, 1)
        db.map_user_to_channel('user1', 2, 1)
        db.map_user_to_channel('user1', 3, 2)

        db.update_user_solved_problems('user1', [1001, 1002, 1003])

        print(db.get_user_channels('user1'))
