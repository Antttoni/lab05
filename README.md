Реализуемый функционал (актуальная на момент написания документации версия контейнера antttoni/lab05:0.0.8 )

Обработка метрик по покупкам клиента согласно БТ: https://github.com/newprolab/dataengineer11/blob/main/labs/lab05/lab05-pipeline-personalize.md
Архитектурная схема решения: https://github.com/newprolab/dataengineer11/tree/main/labs/lab05#readme

Для корректной работы приложения на своем сервере требуется:

1) Развернуть ClickHouse согласно инструкции https://github.com/newprolab/dataengineer11/blob/main/labs/lab02/Clickhouse.md, параметры подключения к этой БД потребуются в п.2

2) Скопировать в каталог на своем сервере файлы 
https://github.com/Antttoni/lab05_git/blob/main/.env
https://github.com/Antttoni/lab05_git/blob/main/docker-compose.yml
https://github.com/Antttoni/lab05_git/blob/main/variables.txt

3) Настроить необходимые сетевые доступы к указанным в файле серверам CLICK_HOST и POSTGRE_HOST по соответствующим портам из https://github.com/Antttoni/lab05_git/blob/main/.env 

4) При необходимости скорректировать параметры доступов в файлах из п.2

5) Перейти в созданный в п.2 каталог и запустить приложение командой docker-compose up

6) Создать пользователя в Airflow:
docker-compose exec airflow-webserver bash
airflow users create -u admin -f Ad -l Min -r Admin -e admin@adm.in

7) Через интерфейс Airflow Admin -> Variables загрузить файл variables.txt из п.2

8) Перейти в DAGs и убедиться, что DAG anton_dolgikh_lab05 успешно запускается за текущую дату

9) Запустить выполнение обработчика за даты из БТ:
airflow dags backfill --start-date 2020-11-20  --end-date 2020-11-30 anton_dolgikh_lab05


