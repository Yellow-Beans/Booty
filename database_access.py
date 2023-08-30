import aiosqlite

# INFO:
# FILE NAME -> "database.db"
# TABLE NAME -> "activity"
# COLUMN NAMES -> "GuildID": int, "UserID": int, "timestamp": int, "whitelisted": bool

# Connects to the database file "database.db", creates it if not existent


class Database:


    async def create_db(self) -> None:
        """
        Creates the table 'activity' if it doesn't exist.
        :return: None
        """
        connection, cursor = await self._get_con()
        # Creating the table if needed
        await cursor.execute(""" CREATE TABLE IF NOT EXISTS activity
        (
            GuildID INTEGER NOT NULL, 
            UserID INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            whitelisted BOOLEAN NOT NULL,
            PRIMARY KEY (GuildID, UserID)
        );
                        """)
        await self._commit_n_close(connection)


    @staticmethod
    async def _get_con() -> tuple[aiosqlite.Connection, aiosqlite.Cursor]:
        """
        Connects to the database file "database.db", and generates a cursor.
        :returns: connection and cursor
        """
        connection = await aiosqlite.connect("database.db")
        cursor = await connection.cursor()
        return connection, cursor


    @staticmethod
    async def _commit_n_close(connection: aiosqlite.Connection) -> None:
        """
        commits the changes and closes the connection.
        :param connection:
        :return:
        """
        await connection.commit()
        await connection.close()


    async def make_or_update_entry(self, serverid: int, userid: int, timestamp: int, white=False) -> None:
        """
        updates the stored timestamp for a stored user if the user exists.
        creates a new entry if the user does not exist.
        if a stored user is whitelisted this status will be kept.
        :param serverid:
        :param userid:
        :param timestamp: int: represents the latest moment a user was active
        :param white: bool, default = False, will turn True if the user is whitelisted. MUST be True to whitelist sb.
        :return: None
        """
        connection, cursor = await self._get_con()
        # checks if the user is whitelisted
        if not white:
            await cursor.execute(
                """
                SELECT whitelisted FROM activity WHERE GuildID = ? AND UserID = ?
                """,
                (serverid, userid, )
            )
            if await cursor.fetchone() == (1,):
                white = True
        await cursor.execute(
            """
            INSERT OR REPLACE INTO activity 
            (GuildID, UserID, timestamp, whitelisted) VALUES(?, ?, ?, ?)
            """,
            (serverid, userid, timestamp, white)
        )
        await self._commit_n_close(connection)


    async def make_needed_entry(self, serverid: int, userid: int, timestamp: int) -> None:
        """
        Creates a new line if the user is not stored. Does nothing if the user exists in the DB.
        :arg serverid: int
        :arg userid: int
        :arg timestamp: int
        """
        connection, cursor = await self._get_con()

        # Create a new entry if the user is not stored in the database. else: continue
        await cursor.execute(
            """
            INSERT OR IGNORE INTO activity 
            (GuildID, UserID, timestamp, whitelisted) VALUES(?, ?, ?, ?)
            """,
            (serverid, userid, timestamp, False, )
        )
        if cursor.rowcount > 0:
            await self._commit_n_close(connection)
        else:
            await connection.close()


    async def call_memberids_one_server(self, serverid: int) -> list[tuple[int]]:
        """
        Gathers all stored UserIDs for a specific server and returns them as a collection of tuples,
        with one entry in each tuple, in a list.
        :param serverid: ID of the server to receive user IDs from
        :return: List of tuples containing UserIDs
        """
        connection, cursor = await self._get_con()
        await cursor.execute(""" 
        SELECT UserID FROM activity WHERE GuildID = ?
        """, (serverid, )
        )

        x = await cursor.fetchall()
        await connection.close()
        return x


    async def delete_single_user(self, serverid: int, userid: int) -> None:
        """
        Delete a single user from one specific server.
        :param serverid: integer
        :param userid: integer
        :return: None
        """
        connection, cursor = await self._get_con()
        await cursor.execute("""
        DELETE FROM activity WHERE GuildID = ? AND UserID = ?
        """, (serverid, userid)
        )
        await self._commit_n_close(connection)


    async def delete_single_server(self, serverid: int) -> None:
        """
        Wipe an entire server off the database
        :param: serverid: int
        :return None
        """
        connection, cursor = await self._get_con()
        await cursor.execute("""
        DELETE FROM activity WHERE GuildID = ?
        """, (serverid, )
        )
        await self._commit_n_close(connection)


    async def call_memberids_inactive_users(self, serverid: int, min_activity: int) -> list[tuple[int]]:
        """
        Gathers all stored userIDs of inactive users for a specific Server, excluding whitelisted users,
        and returns their IDs as a collection of tuples in a list.
        :param serverid: int
        :param min_activity: Lowest timestamp, int, a user must have to be considered active
        :return: List with tuples containing UserIDs
        """
        connection, cursor = await self._get_con()
        await cursor.execute(""" 
        SELECT UserID FROM activity WHERE GuildID = ? AND timestamp < ? AND NOT whitelisted
        """, (serverid, min_activity)
        )
        ids = await cursor.fetchall()
        await connection.close()
        return ids


    async def call_whitelisted_ids(self, serverid: int) -> list[tuple[int]]:
        """
        Gathers all stored userIDs of whitelisted users, for one specific server.
        :param serverid: int
        :return: list with tuples, containing UserIDs
        """
        connection, cursor = await self._get_con()
        await cursor.execute(""" 
        SELECT UserID FROM activity WHERE GuildID = ? AND whitelisted
        """, (serverid, )
        )
        ids = await cursor.fetchall()
        await connection.close()
        return ids


    async def remove_whitelist_status(self, serverid: int, userid: int) -> None:
        """
        Set whitelisted to False for one user on a specific server.
        :param serverid: integer
        :param userid: int
        :return: None
        """
        connection, cursor = await self._get_con()
        await cursor.execute(""" 
        UPDATE activity 
        SET whitelisted = False 
        WHERE GuildID = ? AND UserID =?
        """, (serverid, userid, )
        )
        count = cursor.rowcount
        if count > 0:
            await self._commit_n_close(connection)
        else:
            await connection.close()


    async def call_all_server_ids_once(self, ) -> list[tuple[int]]:
        """
        Gathers one sample of each stored server ID.
        :return: List of tuples containing server IDs
        """
        connection, cursor = await self._get_con()
        await cursor.execute(""" 
        SELECT DISTINCT GuildID FROM activity
        """)
        server_ids = await cursor.fetchall()
        await connection.close()
        return server_ids


    async def delete_many_servers(self, server_ids: list[tuple[int]]) -> None:
        """
        Wipe entire servers off the database
        :param server_ids: List with tuples containing the server IDs
        :return: None
        """
        connection, cursor = await self._get_con()
        await cursor.executemany("""
        DELETE FROM activity WHERE GuildID = ?
        """, server_ids)

        if cursor.rowcount > 0:
            await self._commit_n_close(connection)
        else:
            await connection.close()


    async def make_needed_entries(self, s_and_u_ids: list[tuple[int, int, int, bool]]) -> None:
        """
        Creates a new line if the user is not stored. Does nothing if the user exists in the DB.
        :param s_and_u_ids: a list with tuples, with a guild id(int), user ID(int), int(timestamp) and a whitelist bool
        in each tuple.
        :return: None
        """
        connection, cursor = await self._get_con()
        # Create a new entry if the user is not stored in the database. else: pass
        await cursor.executemany("""
            INSERT OR IGNORE INTO activity 
            (GuildID, UserID, timestamp, whitelisted) VALUES(?, ?, ?, ?)
            """, s_and_u_ids)
        if cursor.rowcount > 0:
            await self._commit_n_close(connection)
        else:
            await connection.close()


    async def delete_many_members(self, delete_members: list) -> None:
        """
        Deletes member entries from db
        :param delete_members: List with tuples. each tuple should contain (guild_id, user_id)
        :return: None
        """
        connection, cursor = await self._get_con()
        await cursor.executemany(
            """DELETE FROM activity WHERE GuildID = ? AND UserID = ?
        """, delete_members)
        if cursor.rowcount > 0:
            await self._commit_n_close(connection)
        else:
            await connection.close()


    async def get_inactive_userids_and_timestamps(self, serverid: int, min_activity: int) -> list[tuple[int, int]]:
        """
        Gathers all stored userIDs of inactive users for a specific Server, excluding whitelisted users
        the arguments 'server_id' and min_activity have to be integer.
        min_activity represents the latest moment (timestamp) someone had to be active to be considered active
        :param serverid: Integer
        :param min_activity: Int, timestamp, representing the last time someone had to be active to be considered active
        :return: Tuples in a list. Each tuple contains a userID and a timestamp.
        """
        connection, cursor = await self._get_con()
        await cursor.execute(""" 
        SELECT UserID, timestamp FROM activity 
        WHERE GuildID = ? AND timestamp < ? AND NOT whitelisted 
        ORDER BY timestamp DESC
        """, (serverid, min_activity))
        ids_n_time = await cursor.fetchall()
        await connection.close()
        return ids_n_time
