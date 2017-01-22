#!/usr/bin/env bash

sbt assembly

curl -XDELETE http://localhost:9200/geonames

curl -XPUT http://localhost:9200/geonames -d @settings.json

spark-submit --class GeonameIngester target/scala-2.11/elasticsearch-geonames-locator-assembly-1.0.jar
