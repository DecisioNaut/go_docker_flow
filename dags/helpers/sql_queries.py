class SqlQueries:
    """Bunch of SQL queries to work with Redshift"""

    # Udacity's provided queries

    songplay_table_insert = """
        INSERT INTO songplays (
            session_id,
            songplay_id,
            start_time,
            artist_id,
            song_id,
            user_id,
            level,
            location,
            user_agent
        )
        WITH
            raw_staging_events AS (
                SELECT
                    sessionId AS session_id,
                    itemInSession AS item_in_session,
                    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                    artist,
                    song,
                    userId AS user_id,
                    level,
                    location,
                    userAgent AS user_agent
                FROM
                    staging_events
                WHERE
                    auth = 'Logged In' AND
                    length > 0
            ),
            raw_artist_data AS (
                SELECT
                    artist_id,
                    name
                FROM
                    artists
            ),
            raw_staging_songs AS (
                SELECT
                    song_id,
                    title,
                    artist_id
                FROM
                    songs
            )
        SELECT
            raw_staging_events.session_id,
            raw_staging_events.item_in_session,
            raw_staging_events.start_time,
            raw_artist_data.artist_id,
            raw_staging_songs.song_id,
            raw_staging_events.user_id,
            raw_staging_events.level,
            raw_staging_events.location,
            raw_staging_events.user_agent
        FROM
            raw_staging_events
        LEFT JOIN
            raw_artist_data
        ON
            raw_staging_events.artist = raw_artist_data.name
        LEFT JOIN
            raw_staging_songs
        ON
            raw_staging_events.song = raw_staging_songs.title AND
            raw_artist_data.artist_id = raw_staging_songs.artist_id;
    """

    artist_table_insert = """
        WITH
        artist_names AS (
            SELECT DISTINCT
                artist AS name
            FROM
                staging_events
            WHERE
                auth = 'Logged In' AND
                length > 0
            ORDER BY
                name DESC
        ),
        artist_names_and_ids as (
            SELECT
                ROW_NUMBER() OVER() AS artist_id,
                name
            FROM
                artist_names
        ),
        staging_songs_artists AS (
            SELECT DISTINCT
                artist_name,
                MAX(artist_location) AS location,
                MAX(artist_latitude) AS latitude,
                MAX(artist_longitude) AS longitude
            FROM
                staging_songs
            GROUP BY
                artist_name
            ORDER BY
                artist_name DESC
        )
        SELECT DISTINCT
            artist_id,
            name,
            location,
            latitude,
            longitude
        FROM
            artist_names_and_ids
        LEFT JOIN
            staging_songs_artists
        ON
            artist_names_and_ids.name = staging_songs_artists.artist_name
        ORDER BY
            artist_id;
    """

    song_table_insert = """
        WITH staging_events_songs AS (
            SELECT
                song AS title,
                artist
            FROM
                staging_events
            WHERE
                auth = 'Logged In' AND
                length > 0
            GROUP BY
                title,
                artist
        ),
        artist_ids AS (
            SELECT
                name,
                artist_id
            FROM
                artists
        ),
        staging_events_songs_with_artist_id AS (
            SELECT
                staging_events_songs.title,
                staging_events_songs.artist,
                artist_ids.artist_id
            FROM
                staging_events_songs
            LEFT JOIN
                artist_ids
            ON staging_events_songs.artist = artist_ids.name
        ),
        staging_songs_ranked AS (
            SELECT
                title,
                artist_name,
                CASE
                    WHEN year = 0 THEN NULL
                    ELSE year
                END AS year,
                duration,
                DENSE_RANK() OVER (PARTITION BY title, artist_name ORDER BY duration DESC) AS rank
            FROM
                staging_songs
        )
        SELECT
            ROW_NUMBER() OVER () AS song_id,
            staging_events_songs_with_artist_id.title,
            staging_events_songs_with_artist_id.artist_id,
            staging_songs_ranked.year,
            staging_songs_ranked.duration
        FROM
            staging_events_songs_with_artist_id
        LEFT JOIN
            staging_songs_ranked
        ON
            staging_events_songs_with_artist_id.title = staging_songs_ranked.title AND
            staging_events_songs_with_artist_id.artist = staging_songs_ranked.artist_name;
    """

    time_table_insert = """
        SELECT
            TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
            EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS year,
            EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS month,
            EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS day,
            EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS hour,
            EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS week,
            EXTRACT(dow FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS weekday
        FROM
            staging_events
        WHERE
            auth = 'Logged In' AND
            length > 0
        GROUP BY
            start_time
        ORDER BY
            start_time
        ;
    """

    user_table_insert = """
        WITH
            user_staging_events AS (
                SELECT
                    userId AS user_id,
                    firstName AS first_name,
                    lastName AS last_name,
                    gender,
                    level,
                    TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS time
                FROM
                    staging_events
                WHERE
                    auth = 'Logged In' AND
                    length > 0
            ),
            max_user_times AS (
                SELECT
                    user_id,
                    MAX(time) AS max_time
                FROM
                    user_staging_events
                GROUP BY
                    user_id
            )
        SELECT
            user_staging_events.user_id,
            user_staging_events.first_name,
            user_staging_events.last_name,
            user_staging_events.gender,
            user_staging_events.level
        FROM
            user_staging_events
        JOIN
            max_user_times
        ON
            user_staging_events.user_id = max_user_times.user_id AND
            user_staging_events.time = max_user_times.max_time;
    """

    # Table creation queries

    create_staging_events_table = """
        CREATE TABLE IF NOT EXISTS staging_events (
            artist          VARCHAR(254),
            auth            VARCHAR(254),
            firstName       VARCHAR(254),
            gender          CHAR(1),
            itemInSession   INTEGER,
            lastName        VARCHAR(254),
            length          FLOAT,
            level           CHAR(4),
            location        VARCHAR(254),
            method          VARCHAR(254),
            page            VARCHAR(254),
            registration    FLOAT,
            sessionId       INTEGER,
            song            VARCHAR(254),
            status          INTEGER,
            ts              BIGINT,
            userAgent       VARCHAR(254),
            userId          INTEGER
        );
    """

    create_staging_songs_table = """
        CREATE TABLE IF NOT EXISTS staging_songs (
            artist_id       VARCHAR(254),
            artist_latitude FLOAT,
            artist_location VARCHAR(254),
            artist_longitude FLOAT,
            artist_name     VARCHAR(254),
            duration        FLOAT,
            num_songs       INTEGER,
            song_id         VARCHAR(254),
            title           VARCHAR(254),
            year            INTEGER
        );
    """

    create_songplays_table = """
        CREATE TABLE IF NOT EXISTS songplays (
            session_id      INTEGER         NOT NULL,
            songplay_id     INTEGER         NOT NULL,
            start_time      TIMESTAMP       NOT NULL,
            artist_id       INTEGER         NOT NULL,
            song_id         INTEGER         NOT NULL,
            user_id         INTEGER         NOT NULL,
            level           CHAR(4)         NOT NULL,
            location        VARCHAR(254)    NOT NULL,
            user_agent      VARCHAR(254)    NOT NULL,
            PRIMARY KEY (session_id, songplay_id),
            UNIQUE (session_id, songplay_id),
            FOREIGN KEY (start_time)        REFERENCES  time (start_time),
            FOREIGN KEY (user_id)           REFERENCES  users (user_id),
            FOREIGN KEY (artist_id)         REFERENCES  artists (artist_id),
            FOREIGN KEY (song_id)           REFERENCES  songs (song_id)
        )
        DISTSTYLE EVEN
        SORTKEY (start_time);
    """

    create_artists_table = """
        CREATE TABLE IF NOT EXISTS artists (
            artist_id       INTEGER         NOT NULL,
            name            VARCHAR(254)    NOT NULL,
            location        VARCHAR(254)    NULL,
            latitude        FLOAT           NULL,
            longitude       FLOAT           NULL,
            UNIQUE          (artist_id),
            PRIMARY KEY     (artist_id)
        )
        DISTSTYLE ALL
        SORTKEY (name);
    """

    create_songs_table = """
        CREATE TABLE IF NOT EXISTS songs (
            song_id         INTEGER         NOT NULL,
            title           VARCHAR(254)    NOT NULL,
            artist_id       INTEGER         NOT NULL,
            year            INTEGER         NULL,
            duration        FLOAT           NULL,
            PRIMARY KEY     (song_id),
            UNIQUE          (song_id),
            FOREIGN KEY     (artist_id)     REFERENCES  artists (artist_id)
        )
        DISTSTYLE ALL
        SORTKEY (title, year);
    """

    create_time_table = """
        CREATE TABLE IF NOT EXISTS time (
            start_time      TIMESTAMP       NOT NULL,
            year            INTEGER         NOT NULL,
            month           INTEGER         NOT NULL,
            day             INTEGER         NOT NULL,
            hour            INTEGER         NOT NULL,
            week            INTEGER         NOT NULL,
            weekday         INTEGER         NOT NULL,
            UNIQUE          (start_time),
            PRIMARY KEY     (start_time)
        )
        DISTSTYLE EVEN
        SORTKEY (start_time);
    """

    create_users_table = """
        CREATE TABLE IF NOT EXISTS users (
            user_id         INTEGER         NOT NULL,
            first_name      VARCHAR(254)     NOT NULL,
            last_name       VARCHAR(254)     NOT NULL,
            gender          CHAR(1)         NOT NULL,
            level           CHAR(4)         NOT NULL,
            UNIQUE          (user_id),
            PRIMARY KEY     (user_id)
        )
        DISTSTYLE ALL
        SORTKEY (last_name, first_name, gender, level);
    """

    # Table drop queries

    drop_staging_events_table = """
        DROP TABLE IF EXISTS staging_events CASCADE
    """

    drop_staging_songs_table = """
        DROP TABLE IF EXISTS staging_songs CASCADE
    """

    drop_songplays_table = """
        DROP TABLE IF EXISTS songplays CASCADE
    """

    drop_artists_table = """
        DROP TABLE IF EXISTS artists CASCADE
    """

    drop_songs_table = """
        DROP TABLE IF EXISTS songs CASCADE
    """

    drop_time_table = """
        DROP TABLE IF EXISTS time CASCADE
    """

    drop_users_table = """
        DROP TABLE IF EXISTS users CASCADE
    """
