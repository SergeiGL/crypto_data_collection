sudo docker run -dit --name $1 \
--restart unless-stopped \
-v $(pwd)/sql_config.py:/root/parsing_exchanges_to_sql/sql_config.py \
-v $(pwd)/tg_bot_config.py:/root/parsing_exchanges_to_sql/tg_bot_config.py \
-v $(pwd)/local_settings.py:/root/parsing_exchanges_to_sql/local_settings.py \
parse $2
