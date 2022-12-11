# ETL задача

## Краткое описание

Сэк: 

- JupiterHub
- Clickhouse
- Python
- SQL
- Airflow

## Настройка связи

Настроили подключение к clickhouse и задали параметры для выполнения дага. После чего с помощью SQL-запроса получим нужные данные, которые будут ежедневно выгружаться в таблицу.

![image](https://user-images.githubusercontent.com/100629361/206928842-7f2a4da0-14b2-473a-a88e-40ea750d0796.png)

## Создание тасков

Всего создали 8 дагов. Двое из них выгружают данные по действиям новостной ленты и по данным с сообщениямию.

![image](https://user-images.githubusercontent.com/100629361/206929127-3a737873-9fe1-4295-a1a5-59bd8ec23e38.png)
![image](https://user-images.githubusercontent.com/100629361/206929137-f80c5b17-7410-4aeb-9a08-49d9b8afcc9f.png)

Один даг после выгрузки данных объединяет таблицы.

![image](https://user-images.githubusercontent.com/100629361/206929015-6bbb0955-273f-4679-915b-59eb8a3b4f01.png)

Трое отвечают за получение срезов по полу, возрасту и опарационной системе.

![image](https://user-images.githubusercontent.com/100629361/206929061-7662e762-4d4c-4bcb-b678-63d66adfbcce.png)

Затем полученные срезы объединяются. И последний таск объединяет все в одну таблицу.

## Ежедневная выгрузка

В airflow настроена ежедневная выгрузка. Граф выглядит следующим образом.

![image](https://user-images.githubusercontent.com/100629361/206929210-b2a83a59-060a-4869-b99a-d0d3ca67b556.png)

![image](https://user-images.githubusercontent.com/100629361/206929266-40053df6-c5ce-4145-8436-669047590402.png)

Результирующая таблица:

![image](https://user-images.githubusercontent.com/100629361/206929634-4ff2ce08-d4b1-4f0a-975d-782bfc99bcb2.png)

