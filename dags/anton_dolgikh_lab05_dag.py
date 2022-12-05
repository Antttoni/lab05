import contextlib
import psycopg2
import pandas as pd

import pendulum
import clickhouse_driver
import redis as redis_driver

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {"owner": "dolgikh"}

@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2020, 11, 22),
    catchup=False,
)
def anton_dolgikh_lab05():
    
    # подключаемся к БД
    click = clickhouse_driver.Client(host=Variable.get("CLICK_HOST"), port=Variable.get("CLICK_PORT"))
    redis = redis_driver.Redis(host=Variable.get("REDIS_HOST"), port=Variable.get("REDIS_PORT"), db=Variable.get("REDIS_DB"))
    
    
    def backfill_check():
        context = get_current_context()
        if context["dag_run"].run_type == "backfill":
            return "join"
        else:
            return "load_dict"
    
    backfill_check = BranchPythonOperator(task_id="backfill_check", python_callable=backfill_check)
    
    # идем дальше, если хотя бы один предыдущий узел успешно выполнился, а остальные были также или успешно выполнены, или пропущены
    join = DummyOperator(task_id="join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    
    # актуализация справочника категорий
    @task
    def load_dict(click):
        query_create_dict_full = """
                select t3.sku_id, 
                    case when t5.cat < 10 then t4.cat else t5.cat end as cat2, 
                    case when t2.cat between 100 and 999 then t2.cat else t4.cat end as cat3,
                    t1.id as id
                from (
                    select max(id) as id, cat from category_tree group by cat
                )t1
                inner join category_tree t2
                on t1.id = t2.id
                inner join sku_cat t3
                on t2.cat = t3.cat
                inner join category_tree t4
                on t2.parent_id = t4.id
                inner join category_tree t5
                on t4.parent_id = t5.id
            """ 
        # используем with, чтобы разорвать соединение после окончния выполнения
        with contextlib.closing(
            psycopg2.connect(
               database=Variable.get("POSTGRE_DB"),
               user=Variable.get("POSTGRE_LOGIN"),
               password=Variable.get("POSTGRE_PASS"),
               host=Variable.get("POSTGRE_HOST"),
            )
        ) as conn:
        # подтягиваем актуальные данные из справочника    
            lab05_dict_full = pd.read_sql(query_create_dict_full, conn)


        df_dict_full = pd.DataFrame(lab05_dict_full, columns = ['sku_id', 'cat2', 'cat3', 'id'])

        # очищаем старые данные из справочника
        click.execute("TRUNCATE TABLE lab05_dict_full")    

        # перезаписываем данные справочника
        print("lab05_dict_full COUNT(*):", click.execute("INSERT INTO lab05_dict_full VALUES", df_dict_full.to_dict('records')))

 
    @task
    def main_task(click, redis):
        
        # получаем параметры запуска
        context = get_current_context()
        start, end = context["data_interval_start"], context["data_interval_end"]
        print("start:", start, "; end:", end)
        query_res = f"""
                select year, month, day, hour, cat from (
                    select d.cat2 as cat, r.year, r.month, r.day, r.hour, timestamp  from (
                        select itemId, year(timestamp) as year, month(timestamp) as month, day(timestamp) as day,hour(timestamp) as hour, timestamp 
                        from lab05_raw_data 
                        where action = 'favAdd' and timestamp between {start.timestamp()} and {end.timestamp()}
                    )r
                    inner join lab05_dict_full d
                    on r.itemId = d.sku_id
                )
                group by year, month, day, hour, cat
                order by year, month, day, hour, count(*) desc, min(timestamp), cat
                LIMIT 5 BY year, month, day, hour
            """
        print("Query main_task:", query_res)
        
        # собираем DataFrame
        df = pd.DataFrame(click.execute(query_res), columns = ['year', 'month', 'day', 'hour', 'cat'])  
        df = df.reset_index()  # чтобы точно сортировка была правильная

        print("Records count:", df.count())

        for index, row in df.iterrows():
            year = str(row['year']).zfill(4)
            month = str(row['month']).zfill(2)
            day = str(row['day']).zfill(2)
            hour = str(row['hour']).zfill(2)
             
        # Формат "fav:level2:2020-11-21:23h:top5"       
            redis.sadd(f"fav:level2:{year}-{month}-{day}:{hour}h:top5",str(row['cat']).zfill(2))
        
        # проверка
        print("fav:level2:2020-11-21:23h:top5:",redis.smembers("fav:level2:2020-11-21:23h:top5"))

    @task
    def add_task1(click, redis):
        # получаем параметры запуска
        context = get_current_context()
        start, end = context["data_interval_start"], context["data_interval_end"]
        print("start:", start, "; end:", end)
       
        query_res_full1 = f"""
                 select year, month, day, hour, userId, cat3, row_number()  OVER (PARTITION BY year, month, day, hour, userId ORDER BY count(*) desc, min(timestamp), max(r_num)) AS rank
                 from (
                    select r.userId, d.cat3, r.year, r.month, r.day, r.hour, timestamp, r.action, r.r_num  from (
                        select action, userId, itemId, year(timestamp) as year, month(timestamp) as month, day(timestamp) as day,hour(timestamp) as hour, timestamp, r_num 
                        from lab05_raw_data 
                        where action = 'itemView' and timestamp between {start.timestamp()} and {end.timestamp()}
                    )r
                    inner join lab05_dict_full d
                    on r.itemId = d.sku_id
                )
                group by year, month, day, hour, userId, cat3
                LIMIT 10 BY year, month, day, hour, userId	
            """
        print("Query add_task1:", query_res_full1)        
        
        # собираем DataFrame
        df_full1 = pd.DataFrame(click.execute(query_res_full1), columns = ['year', 'month', 'day', 'hour', 'userId', 'cat3', 'rank' ])  
        df_full1 = df_full1.reset_index() # чтобы точно сортировка была правильная

        print("Records count:", df_full1.count())

        for index, row in df_full1.iterrows():
            year = str(row['year']).zfill(4)
            month = str(row['month']).zfill(2)
            day = str(row['day']).zfill(2)
            hour = str(row['hour']).zfill(2)
            
             
        # Формат "fav:level2:2020-11-21:23h:top5"       
            redis.zadd(f"user:{row['userId']}:view:level3:{year}-{month}-{day}:{hour}h:top10",{row['cat3']:row['rank']})


    @task
    def add_task2(click, redis):
        # получаем параметры запуска
        context = get_current_context()
        start, end = context["data_interval_start"], context["data_interval_end"]
        print("start:", start, "; end:", end)
       
        query_res_full2 = f"""
            select year, month, day, hour, userId, cat3 from (
                select year, month, day, hour, userId, cat3, timestamp, action, r_num, score,
                first_value(action) OVER w1 as first_action,
                first_value(r_num) OVER w2 as first_Add_r_num	
                from ( 
                    select r.userId, d.cat3, r.year, r.month, r.day, r.hour, timestamp, r.action, r.r_num, r.score  from (
                        select action, userId, itemId, year(timestamp) as year, month(timestamp) as month, day(timestamp) as day,hour(timestamp) as hour, timestamp, r_num, 
                        case when action = 'favRemove' then -1 else 1 end as score   
                    from lab05_raw_data 
                    where action in ('favAdd','favRemove') and timestamp between {start.timestamp()} and {end.timestamp()}
                        )r
                    inner join lab05_dict_full d
                    on r.itemId = d.sku_id               
                )
                WINDOW
                    w1 AS (PARTITION BY year, month, day, hour, userId, cat3 ORDER BY r_num ASC),
                    w2 AS (PARTITION BY year, month, day, hour, userId, cat3 ORDER BY action ASC)
            )
            group by year, month, day, hour, userId, cat3 having (sum(score) >= 0)
            ORDER BY year, month, day, hour, userId, (case when min(first_action) = 'favRemove' then sum(score)+ 1 else sum(score) end) desc, min(first_Add_r_num)		
            LIMIT 5 BY year, month, day, hour, userId	
            """
        
        print("Query add_task2:", query_res_full2)        
        
        # собираем DataFrame
        df_full2 = pd.DataFrame(click.execute(query_res_full2), columns = ['year', 'month', 'day', 'hour', 'userId', 'cat3' ])  
        df_full2 = df_full2.reset_index()  # make sure indexes pair with number of rows

        print("Records count:", df_full2.count())

        for index, row in df_full2.iterrows():
            year = str(row['year']).zfill(4)
            month = str(row['month']).zfill(2)
            day = str(row['day']).zfill(2)
            hour = str(row['hour']).zfill(2)
            val = str(row['cat3']).zfill(3)
             
        # Формат "fav:level2:2020-11-21:23h:top5"       
            redis.zadd(f"user:{row['userId']}:fav:level3:{year}-{month}-{day}:{hour}h:top5",{row['cat3']:index})

    # Очередность запуска dag'ов: 

    backfill_check >> join
    
    # если тип запуска не backfill, обновляем данные справочника категорий
    backfill_check >> load_dict(click) >> join
    
    # поэтапно запускаем выполнение заданий лабы 5
    join >> main_task(click, redis) >> add_task1(click, redis) >> add_task2(click, redis)
    # join >> aggregate_counts(download_tree(), click, redis)

    # все операции с БД выполнены, отключаемся
    click.disconnect()
    # для Redis точно этот оператор для отключения используется?.. Почему в Jupiter после вызова этого оператора всё равно отрабатывает оператор redis.smembers например? 
    redis.quit()

actual_dag = anton_dolgikh_lab05()