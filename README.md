# parsing_exchanges_to_sql
This repo is for code that parses data from different exchanges and stores it in the SQL database.

## BINANCE Parsing
BINANCE parsing needs the `sql_config.py` file with the following structure:
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
TG_TOKEN = ''                   # Токен телеграм бота для отправки ошибок
TG_CHAT_ID_ERRORS = ''          # Идентификатор чата для отправки ошибок
TG_TOKEN_MESSAGES = ''          # Токен телеграм бота для отправки уведомлений
TG_CHAT_ID_MESSAGES = ''        # Идентификатор чата для отправки уведомлений об интересных ставках
```

# Docker

## Build image

```
docker build . -t parse
```

## Run docker Containers
List required scripts in [script_list.txt](script_list.txt) and then run
```
./parse_all.sh
```
