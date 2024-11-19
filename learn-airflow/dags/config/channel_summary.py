{
    'table': 'channel_summary',
    'schema': 'sko99',
    'main_sql': """
        SELECT
            channel,
            COUNT(1) AS session_count,
            COUNT(DISTINCT userid) AS user_count
        FROM
            raw_data.user_session_channel
        GROUP BY 1
        ORDER BY 2 DESC;
        """,
    'input_check':
    [
        {
        'sql': 'SELECT COUNT(1) FROM sko99.raw_data',
        'count': 150000
        },
    ],
    'output_check':
    [
        {
        'sql': 'SELECT COUNT(1) FROM {schema}.temp_{table}',
        'count': 12
        }
    ],
}