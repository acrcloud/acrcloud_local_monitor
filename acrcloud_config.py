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
        #Local Database Config
        "enabled": 1, #1-enabled(default), 0-disabled
        "host":"127.0.0.1",
        "port":3306,
        "user":"root",
        "passwd":"XXXXXXXXX",
        "db":"monitor_result",
    },
    "log":{
        #Local Monitor Server Log Config
        "dir":"./radioLog",
    },
    "record":{
        #if record=3, Monitor Server will record the recognize audio file.
        #if record=0, Monitor Server will not record audio file.
        "record":0,
        "record_before":5, #seconds
        "record_after":5,  #seconds
        #if record_save_days = N and record_save_days>0, Monitor Server will keep current N days record files,
        #the record files of N days will be auto deleted.
        #if record_save_days=0, it will keep all record files all time.
        "record_save_days": 5, #default keep 5 days record files
        "record_dir":"./record_file"
    }
}
