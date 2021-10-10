## SISU SOLUTIONS CODING ASSESSMENT – DATA ENGINEERING ##
***
### Instructions:
- Set up PySpark locally.
- Use PyCharm to run main.py or from the CommandLine type:
```
python main.py
```
***
### Specifications:
- PyCharm IDE
- Apache Spark 3.1.2
- Python 3.8
***
### Diagram:
The implemented approach was a hybrid Delta Lake as the below diagram:
![alt text](https://github.com/arturogonzalezm/transactions/blob/main/images/delta_lake.png?raw=true)
***
### Results:
```
+-------+--------+---------+
|AgentID|PostCode|MaxAmount|
+-------+--------+---------+
|307511 |2081    |5378.03  |
|307507 |2081    |4216.0   |
|307564 |2081    |2770.24  |
|307662 |2722    |1274.57  |
|307509 |2587    |1130.5   |
|307510 |2587    |920.93   |
|307508 |2587    |755.6    |
|307312 |2581    |742.36   |
|307510 |2586    |613.75   |
|307509 |2586    |613.75   |
|307662 |2581    |443.23   |
|307508 |2586    |333.61   |
|307562 |2722    |241.82   |
|307561 |2722    |132.51   |
|307664 |2581    |104.11   |
|307664 |2586    |84.72    |
|307662 |2586    |84.72    |
|307506 |2587    |70.36    |
|307323 |2581    |68.7     |
|307561 |2581    |62.1     |
|307562 |2581    |37.99    |
|307561 |2626    |22.18    |
|307396 |2581    |9.95     |
|307563 |2581    |9.9      |
|307663 |2581    |6.05     |
|307396 |2582    |0.09     |
|307312 |2582    |0.0      |
+-------+--------+---------+
```