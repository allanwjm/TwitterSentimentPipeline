# Twitter Sentiment Analysis Pipeline
The *Twitter Sentiment Pipeline* is the **pipeline** version of the [Twitter Analysis Project](https://github.com/allanwjm/TwitterAnalysis).

This program will fetch data from the **CouchDB** database, process them and store in the local **MySQL** database for generating the sentiment analysis results as **CSV** files throught Restful APIs.

## Installation
This program requires **Python 3** and **MySQL**. Any other dependencies are listed in `requirements.txt`.

1. Install and start your MySQL database.

1. Create a new virtualenv (recommanded).
    ```bash
    virtualenv venv -p python3
    source venv/bin/activate
    ```
    
1. Install the dependencies.
    ```bash
    pip install -r requirements.txt
    ```
    
1. Config the MySQL connection parameters in the `consts.py`
    ```python
    DATABASE_HOST = ...
    DATABASE_PORT = ...
    DATABASE_USER = ...
    DATABASE_PASSWORD = ...
    DATABASE_SCHEMA = ...
    ```
    
1. Initialize the database for the pipeline program (important!).
    ```bash
    python pipeline.py -i
    ```
    
1. Start running the pipeline!
    ```bash
    python pipeline.py
    ```
    
## APIs
The analysis result can be accessed through RESTful APIs. They are in `CSV` format so can be imported into **AURIN**.

Start the API program using this:
```bash
python api.py -p <PORT>
```

Then request the **CSV** file with this URL: `/csv/<CITY>`. Try the API hosted on our *Nectar* node:
```http request
http://115.146.92.26:8000/csv/<CITY>
```

Available Cities:
* [Adelaide](http://115.146.92.26:8000/csv/adelaide)
* [Brisbane](http://115.146.92.26:8000/csv/brisbane)
* [Canberra](http://115.146.92.26:8000/csv/canberra)
* [Hobart](http://115.146.92.26:8000/csv/hobart)
* [Melbourne](http://115.146.92.26:8000/csv/melbourne)
* [Perth](http://115.146.92.26:8000/csv/perth)
* [Sydney](http://115.146.92.26:8000/csv/sydney)

It may take a while to process the data... You can see a generated file `melbourne.csv` directly in this repository.

![CSV Screenshot](screenshot-csv.png)

---
## TODO...

The API parameters like *Year*, *Weekday* and so on is still under construction!
I will continue this after finish the final exam...
