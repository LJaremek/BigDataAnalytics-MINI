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


Getting info from kafka

```sh
docker exec -it bigdataanalytics-mini-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic scraped_data --from-beginning
```


Checking docker pod logs:
```sh
docker logs \<IMAGE NAME\>
```
