# 주식 가격과 종목토로방
주식 가격과 네이버 종목토론방의 내용을 일자 별로 수집 및 시각화/분석

## Purpose
네이버 종목토론방의 여론과 주식 가격의 연관성 분석      
   
주식을 할 때, 종목토로방에서 정보를 얻는 사람들이 많다.    
그런데 과연 그 정보들은 믿을만한 정보일까? 종목토론방에 긍정적인 말이 많다면 주가도 긍정적일까?    
들리는 바로는 종목토론방에 일부로 부정적인 여론을 흘려 주식을 팔도록 유도하는 수법도 있다고 한다. 이 프로젝트를 통해서 이러한 호기심을 해결하고자 한다.
   
   
---
## Solution
1. 네이버 증권에서 주식 가격을 추출하여 PostgreSQL에 저장한다.
2. 네이버 종목토론방의 정보를 추출하여 MongoDB에 저장한다.
3. Spark로 Database에서 정보를 불러와 데이터를 처리한다.
4. 처리된 데이터를 Hive에 저장한다.
5. 위 과정을 Airflow를 통해서 워크플로우 스케줄링을 한다. (진행중)
6. Hive에 저장된 데이터를 tableau를 통해 시각화/분석한다. (예정)

## Work Flow
<img width="80%" src="https://user-images.githubusercontent.com/40620421/154842090-05aec276-49ef-43a7-9722-ea27fcc9ca88.png"/>
   
   
## Environment
GCP(Google Cloud Platform)   
Ubuntu:20.04   
- Python
- Hadoop
- Apache Spark
- Apache Hive
- Apache Airflow
- PostgreSQL
- MongoDB
