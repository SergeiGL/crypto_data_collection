# parsing_exchanges_to_sql
This repo is for code that parses data from different exchanges and stores it in the SQL database.

## Requirements
`sql_config.py` file with the following structure:
```
DB_CONFIG = {
    "user": "",
    "port": ,
    "password": "",
    "host": "",
    "db": "",
}

```

and also the `tg_bot_config.py` file with the following structure:
```
TG_TOKEN = ''                 
TG_CHAT_ID_ERRORS = ''
TG_TOKEN_MESSAGES = ''
TG_CHAT_ID_MESSAGES = ''
```
