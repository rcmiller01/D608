class SqlQueries:
    songplay_table_insert = """
    SELECT
        md5(events.sessionid::varchar || events.ts::varchar) AS playid,
        TIMESTAMP 'epoch' + events.ts/1000 * INTERVAL '1 second' AS start_time,
        events.userid::int          AS userid,
        events.level                AS level,
        songs.song_id               AS songid,
        songs.artist_id             AS artistid,
        events.sessionid            AS sessionid,
        events.location             AS location,
        events.useragent            AS user_agent
    FROM staging_events events
    LEFT JOIN staging_songs songs
      ON events.song = songs.title
     AND events.artist = songs.artist_name
     AND abs(events.length - songs.duration) < 2
    WHERE events.page = 'NextSong';
    """

    user_table_insert = """
    SELECT DISTINCT
        userid::int     AS userid,
        firstname       AS first_name,
        lastname        AS last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong' AND userid IS NOT NULL;
    """

    song_table_insert = """
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
    """

    artist_table_insert = """
    SELECT DISTINCT
        artist_id,
        artist_name     AS name,
        artist_location AS location,
        artist_latitude  AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
    """

    time_table_insert = """
    WITH t AS (
      SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
      FROM staging_events
      WHERE page = 'NextSong'
    )
    SELECT DISTINCT
      start_time,
      EXTRACT(hour from start_time)  AS hour,
      EXTRACT(day  from start_time)  AS day,
      EXTRACT(week from start_time)  AS week,
      EXTRACT(month from start_time) AS month,
      EXTRACT(year from start_time)  AS year,
      EXTRACT(dow  from start_time)  AS weekday
    FROM t;
    """
