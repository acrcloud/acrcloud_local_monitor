config = {
    "server":{
        "port":3005, #Local Monitor Server Port
    },
    "user":{
        #ACRCloud Broadcast Monitoring, Local Project Access_Key And Access_Secret
        "access_key":"48ba195edd0107061f2062f0cd2bf5a3",
        "access_secret":"KxTGNBiZOM0nS2TmY22ApFjsHeYGjnVyYm9Bh5Uc",
        #API URL To Get The Local Monitor Streams Info
        "api_url":"https://api.acrcloud.com/v1/local-monitor-streams?access_key={0}",
    },
    "database":{
        #Local Database Config
        "host":"127.0.0.1",
        "port":3306,
        "user":"root",
        "passwd":"",
        "db":"monitor_result",
    },
    "log":{
        #Local Monitor Server Log Config
        "dir":"./radioLog",
    }
}
