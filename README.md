# BigDataAnalytics-MINI

## How to start
1. Install docker
2. Build and run containers (commands below)
3. Check what is on kafka (command below)
4. Check files on the HDFS


## Project architecture
```
.
├── Dockerfile.consumer_batch
├── Dockerfile.datanode
├── Dockerfile.scraper_news_xtb
├── Dockerfile.scraper_newsapi
├── Dockerfile.scraper_stock_xtb
├── Dockerfile.scraper_worldnewsapi
├── README.md
├── docker-compose.yml
└── src
    ├── avro_schemas                <- schemas of avro files for HDFS
    ├── data_processing
    │   ├── scraping                <- code for scraping
    │   └── transformations
    ├── models
    ├── services
    │   ├── kafka_consumers         <- kafka consumers (for batch preparing)
    │   ├── kafka_producers         <- kafka producers (scrappers)
    │   └── producers_mock_data     <- mock data for producers testing
    ├── tests
    └── requirements.txt
```


## Commands
### Docker
Stopping every running container

```sh
sudo docker stop $(sudo docker ps -q)
```


Deleting every existing container

```sh
sudo docker rm $(sudo docker ps -aq)
```


Build and run containers

```sh
sudo docker compose build --no-cache
sudo docker compose up -d
```

Or:
```sh
sudo docker compose down; sudo docker compose up --build -d
```


Getting info from kafka

```sh
docker exec -it bigdataanalytics-mini-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic scraped_data --from-beginning
```


Checking docker pod logs:
```sh
sudo docker logs \<IMAGE NAME\>
```


Build custom Docker Image with torch:
```sh
sudo docker build -t python_3_10_torch -f Dockerfile.torch .
```

### HDFS commands
Enter to the container
```sh
sudo docker exec -it namenode bash
```

List files:
```sh
hdfs dfs -ls /
```

### Mongo DB
http://localhost:8081


### Model API
Training:
http://localhost:8000/train/

Training with real-time view:
http://localhost:8000/train_stream/?epochs=100

Sample prediction
http://localhost:8000/predict/?open=11500.0&close=11450.0&high=11600.0&low=11300.0&vol=7500.0&temperature=20.5&rain=0.2&sun=4500.0&sentiment=1.0&language=0.0

Regenerate historical predictions
http://localhost:8000/predict_history/?start_date=2024-1-17&end_date=2025-01-14
http://localhost:8000/train_and_predict/?start_date=2024-1-17&end_date=2025-01-14&epochs=1000
