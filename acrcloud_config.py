config = {
    "server":{
        "port":3005, #Local Monitor Server Port
    },
    "user":{
        #ACRCloud Broadcast Monitoring, Local Project Access_Key
        "access_key":"XXXXXXXX",
        #API URL To Get The Local Monitor Streams Info
        "api_url":"https://api.acrcloud.com/v1/local-monitor-streams?access_key={0}",
    },
    #if you only want to monitor some streams of your local project, you can fill ids in the field of stream_ids:
    #like: stream_ids:["stream_id1", "stream_id2"], if empty, it will monitor all the streams.
    "stream_ids":[],
    "recognize": {},
    "database":{
        'type': 'sqlite3', # mysql, psql or sqlite3, if psql is selected, make sure to install the python package 'psycopg2'
        #Local Database Config
        "enabled": 1, #1-enabled(default), 0-disabled
        "host":"127.0.0.1",
        "port":3306,
        "user":"root",
        "passwd":"XXXXXXXXX",
        "db":"monitor_result",
        "sqlite3_db": "./sqlite3_db/results.db"
    },
    "log":{
        #Local Monitor Server Log Config
        "dir":"./radioLog",
    }
}
