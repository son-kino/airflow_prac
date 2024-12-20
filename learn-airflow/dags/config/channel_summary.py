{
    'table': 'channel_summary',
    'schema': 'sko99',
    'main_sql': """
        SELECT DISTINCT A.userid,
        FIRST_VALUE(A.channel) over(partition by A.userid order by B.ts rows between unbounded preceding and unbounded following) AS First_Channel,
        LAST_VALUE(A.channel) over(partition by A.userid order by B.ts rows between unbounded preceding and unbounded following) AS Last_Channel
        FROM 
            raw_data.user_session_channel A
        LEFT JOIN raw_data.session_timestamp B 
            ON A.sessionid = B.sessionid;
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