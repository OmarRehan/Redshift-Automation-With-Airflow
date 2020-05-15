class SparkifyQueries:
    dict_DDL_schemas = {
        'schema_staging_name': 'STAGING_SCHEMA',
        'schema_sparkify_name': 'SPARKIFY_SCHEMA'
    }

    dict_DDL_tables = {
        'stg_log_table': 'STG_LOG',
        'stg_song_table': 'STG_SONG',
        'songplay_table': 'SONG_PLAY_TBL',
        'user_table': 'USER_TBL',
        'song_table': 'SONG_TBL',
        'artist_table': 'ARTIST_TBL',
        'time_table': 'TIME_TBL'
    }

    # FINAL TABLES
    insert_songplay_table = """
        BEGIN TRANSACTION;

        DELETE FROM {schema_sparkify_name}.{songplay_table}
        USING {schema_staging_name}.{stg_log_table} AS stg_log
        WHERE SONG_PLAY_TBL.UNIX_TS = stg_log.ts
        AND SONG_PLAY_TBL.SESSION_ID = stg_log.sessionid
        AND stg_log.page = 'NextSong';

        INSERT INTO {schema_sparkify_name}.{songplay_table}
        SELECT DISTINCT
        CAST(ts AS BIGINT) AS UNIX_TS,
        CAST(sessionid AS BIGINT) AS SESSION_ID,
        ARTIST_OUTPUT.ARTIST_ID,
        SONG_OUTPUT.SONG_ID,
        CAST(userid AS INTEGER) AS USER_ID,
        level,
        location AS USER_LOCATION,
        useragent AS USER_AGENT,
        CAST(auth AS VARCHAR(25)) AS USER_LOGGED,
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS CREATION_TIMESTAMP
        FROM {schema_staging_name}.{stg_log_table} AS stg_log
        LEFT JOIN
        (
            SELECT ARTIST_ID,ARTIST_NAME,
            ROW_NUMBER() OVER (PARTITION BY ARTIST_NAME ORDER BY ARTIST_LATITUDE DESC, ARTIST_LONGITUTE DESC,ARTIST_LOCATION ASC) AS ROW_NUM
            FROM {schema_sparkify_name}.{artist_table}
        ) AS ARTIST_OUTPUT
        ON STG_LOG.artist = ARTIST_OUTPUT.ARTIST_NAME
        AND ARTIST_OUTPUT.ROW_NUM = 1
        LEFT JOIN
        (
            SELECT SONG_ID,SONG_TITLE,
            ROW_NUMBER() OVER (PARTITION BY SONG_TITLE ORDER BY SONG_YEAR DESC) AS ROW_NUM
            FROM {schema_sparkify_name}.{song_table}
        ) AS SONG_OUTPUT
        ON TRIM(STG_LOG.song) = SONG_OUTPUT.SONG_TITLE
        AND SONG_OUTPUT.ROW_NUM = 1
        WHERE stg_log.page = 'NextSong';

    """

    insert_user_table = """
        BEGIN TRANSACTION;

        DELETE FROM {schema_sparkify_name}.{user_table}
        USING {schema_staging_name}.{stg_log_table} AS stg_log
        WHERE USER_TBL.USER_ID = stg_log.userid;

        INSERT INTO {schema_sparkify_name}.{user_table}
        SELECT USER_ID,FIRST_NAME,LAST_NAME,gender,level
        FROM
        (
            SELECT
            CAST(userid AS INTEGER) AS USER_ID,
            firstname AS FIRST_NAME,
            lastname AS LAST_NAME,
            gender,
            level,
            ROW_NUMBER() OVER (PARTITION BY userid order BY ts DESC) AS ROW_NUM
            FROM {schema_staging_name}.{stg_log_table}
            WHERE auth='Logged In'
        )
        WHERE ROW_NUM = 1;

    """

    insert_song_table = """
        BEGIN TRANSACTION;

        DELETE FROM {schema_sparkify_name}.{song_table}
        USING {schema_staging_name}.{stg_song_table} AS STG_SONG
        WHERE SONG_TBL.song_id = STG_SONG.SONG_ID
        AND SONG_TBL.artist_id = STG_SONG.ARTIST_ID
        AND SONG_TBL.duration = CAST(STG_SONG.DURATION AS DOUBLE PRECISION);

        INSERT INTO {schema_sparkify_name}.{song_table}
        w init


    """

    insert_artist_table = """
        BEGIN TRANSACTION;

        DELETE FROM {schema_sparkify_name}.{artist_table}
        USING {schema_staging_name}.{stg_song_table} AS STG_SONG
        WHERE ARTIST_TBL.ARTIST_ID = STG_SONG.artist_id;

        INSERT INTO {schema_sparkify_name}.{artist_table}
        SELECT
        ARTIST_ID,
        ARTIST_NAME,
        ARTIST_LOCATION,
        CAST(ARTIST_LATITUDE AS DOUBLE PRECISION),
        CAST(ARTIST_LONGITUTE AS DOUBLE PRECISION)
        FROM
        (
            SELECT DISTINCT
            artist_id AS ARTIST_ID,
            artist_name AS ARTIST_NAME,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY LEN(artist_name)) AS ROW_NUM,
            CASE WHEN trim(artist_location) = '' OR artist_location IS NULL THEN 'Unknown' ELSE artist_location END AS ARTIST_LOCATION,
            CASE WHEN trim(artist_latitude) = '' OR artist_latitude IS NULL THEN '-9999' ELSE artist_latitude END AS ARTIST_LATITUDE,
            CASE WHEN trim(artist_longitude) = '' OR artist_longitude IS NULL THEN '-9999' ELSE artist_longitude END AS ARTIST_LONGITUTE
            FROM {schema_staging_name}.{stg_song_table} AS STG_SONG
        )
        WHERE ROW_NUM = 1;

    """

    insert_time_table = """
        INSERT INTO {schema_sparkify_name}.{time_table}
        SELECT DISTINCT
        CAST(ts AS BIGINT) AS TIME_ID
        ,TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS TS_FORMATTED
        ,EXTRACT(year FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS YEAR_COL
        ,EXTRACT(month FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS MONTH_COL
        ,EXTRACT(day FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS DAY_COL
        ,EXTRACT(hour FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS HOUR_COL
        ,CASE
        WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 0 THEN 'Sunday'
        WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 1 THEN 'Monday'
        WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 2 THEN 'Tuesday'
        WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 3 THEN 'Wednesday'
        WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 4 THEN 'Thursday'
        WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 5 THEN 'Friday'
        WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 6 THEN 'Saturday'
        END AS DAY_NAME
        FROM {schema_staging_name}.{stg_log_table} AS stg_log
        LEFT OUTER JOIN {schema_sparkify_name}.{time_table} AS TIME_TBL
        ON CAST(stg_log.ts AS BIGINT) = TIME_TBL.TIME_ID
        WHERE TIME_TBL.TIME_ID IS NULL;
    """

    data_quality_checks = [
        {
            'schema_name': dict_DDL_schemas.get('schema_sparkify_name')
            ,'table_name': dict_DDL_tables.get('song_table')
            ,'quality_rule':'COUNT_OF_RECORDS'
            ,'quality_check_query': 'SELECT COUNT(*) FROM {}.{};'.format(dict_DDL_schemas.get('schema_sparkify_name'),
                                                                          dict_DDL_tables.get('song_table'))
            ,'min_count': 0
            ,'result': None
        },
        {
            'schema_name': dict_DDL_schemas.get('schema_sparkify_name')
            ,'table_name': dict_DDL_tables.get('artist_table')
            , 'quality_rule': 'COUNT_OF_RECORDS'
            ,'quality_check_query': 'SELECT COUNT(*) FROM {}.{};'.format(dict_DDL_schemas.get('schema_sparkify_name'),
                                                                          dict_DDL_tables.get('artist_table'))
            ,'min_count': 0
            ,'result': None
        },
        {
            'schema_name': dict_DDL_schemas.get('schema_sparkify_name')
            ,'table_name': dict_DDL_tables.get('artist_table')
            , 'quality_rule': 'COUNT_OF_RECORDS'
            ,'quality_check_query': 'SELECT COUNT(*) FROM {}.{};'.format(dict_DDL_schemas.get('schema_sparkify_name'),
                                                                          dict_DDL_tables.get('artist_table'))
            ,'min_count': 0
            ,'result': None
        },
        {
            'schema_name': dict_DDL_schemas.get('schema_sparkify_name')
            ,'table_name': dict_DDL_tables.get('time_table')
            , 'quality_rule': 'COUNT_OF_RECORDS'
            ,'quality_check_query': 'SELECT COUNT(*) FROM {}.{};'.format(dict_DDL_schemas.get('schema_sparkify_name'),
                                                                          dict_DDL_tables.get('time_table'))
            ,'min_count': 0
            ,'result':None
        },
        {
            'schema_name': dict_DDL_schemas.get('schema_sparkify_name')
            , 'table_name': dict_DDL_tables.get('songplay_table')
            , 'quality_rule': 'COUNT_OF_RECORDS'
            , 'quality_check_query': 'SELECT COUNT(*) FROM {}.{};'.format(dict_DDL_schemas.get('schema_sparkify_name'),
                                                                          dict_DDL_tables.get('songplay_table'))
            , 'min_count': 0
            , 'result': None
        },
    ]
