Deploy AIRFLOW


You should use version Python 3.6 or Python 2.7 of Python Interpreter.
Version 1.10 of Airflow doesn't not support Python 3.7 and higher
 

1. Create virtual environment. 

2. You need download the project from GitHub on your computer.

3. Open project in some IDEA. 

4. Start your virtualenv by command: 
    ```
    source virtualvenv/bin/activate
 
5. Download apache airflow open terminal window and type the following command:
    ```
   pip install apache-airflow

6. Create the file in your source directory:
    ```
   run.txt

7. Open terminal in virtual environment and initial database command: 
    ````
    airflow initdb
   
8. When database initializing ended in terminal type the following command for start web server:
    ```
   airflow webserver -p 8080
   
   Notice: your pid is saved in 'airflow-webserver.pid'
   
9 In terminal you can see your local server. It looks like:

    [2020-04-13 20:32:33 +0300] [10569] [INFO] Listening at: http://0.0.0.0:3422 

10. Open new terminal window and type the following command for schedule the your dag(s):
    ```
    airflow scheduler
    
11. Go to localhost and start working