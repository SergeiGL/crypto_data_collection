FROM python:3.11.4-slim-buster


# Выставляем часовой пояс
RUN echo "Europe/Moscow" > /etc/timezone \
    && ln -fs /usr/share/zoneinfo/Europe/Moscow /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata


# Обновляем pip и устанавливаем зависимости
COPY requirements.txt /root/
RUN python -m pip install --upgrade pip \
    && pip install -r /root/requirements.txt


WORKDIR /root/parsing_exchanges_to_sql
ENTRYPOINT ["python"]


# Копируем исходные коды
COPY . ./
