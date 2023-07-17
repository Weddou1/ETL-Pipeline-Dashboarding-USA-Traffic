# Presentation
An ETL (Extract - Transform - Load) data pipeline to store data that can later be analyzed! 

To do this, we're going to use the [Mage](https://github.com/mage-ai/mage-ai) Framework (with Python) to facilitate the process.

## Dataset 

[Dataset Link](Dataset link)

```
Kaggle Description : 
This is a countrywide car accident dataset that covers 49 states of the USA. The accident data were collected from February 2016 to March 2023, using multiple APIs that provide streaming traffic incident (or event) data. These APIs broadcast traffic data captured by various entities, including the US and state departments of transportation, law enforcement agencies, traffic cameras, and traffic sensors within the road networks. The dataset currently contains approximately 7.7 million accident records.
```

## Data Processing Methods

### Data Normalization

This consists in modeling our database in a way that breaks it down into several small logical units, in order to avoid data redundancy and make it easier to store afterwards.

Data before Normalization
![image](https://github.com/Weddou1/ETL-Pipeline-Dashboarding-USA-Traffic/assets/86536874/872311d5-8508-47be-b413-15d15592b4ac)

Data After Normalization (The diagram is available if you want to see it in detail)
![image](https://github.com/Weddou1/ETL-Pipeline-Dashboarding-USA-Traffic/assets/86536874/b14e8964-0f8e-4c41-a7af-e3079a062777)

### Encoding

The fewer columns, the better! That's why you can use basic encoding techniques like this, which can save you a lot of space.

![image](https://github.com/Weddou1/ETL-Pipeline-Dashboarding-USA-Traffic/assets/86536874/bb28c501-ecb9-4fa4-aceb-c9ae983ef411)

## Loading

Data can be loaded into a PostgreSQL database using the psycopg2 library.
