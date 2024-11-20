# BigDataAnalytics-MINI

## How to start
1. Install docker
2. Build and run containers (commands below)
3. Check what is on kafka (command below)


## Project architecture
```
.
├── Dockerfile.scraper_news_xtb
├── Dockerfile.scraper_newsapi
├── Dockerfile.scraper_worldnewsapi
├── README.md
├── docker-compose.yml
└── src
    ├── data_processing
    │   ├── scraping
    │   └── transformations
    ├── models
    ├── services
    │   ├── kafka_consumers
    │   └── kafka_producers
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
