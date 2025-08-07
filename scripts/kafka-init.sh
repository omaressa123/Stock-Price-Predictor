#!/bin/bash

# تأكد إن Kafka جاهز قبل إنشاء التوبيكس
echo "Waiting for Kafka to be ready..."
sleep 10

# تعريف التوبيكس اللي محتاجهم النظام
TOPICS=("stock_prices" "stock_predictions")

for topic in "${TOPICS[@]}"
do
  echo "Creating topic: $topic"
  /opt/kafka/bin/kafka-topics.sh \
    --create \
    --topic "$topic" \
    --bootstrap-server kafka:9092 \
    --replication-factor 1 \
    --partitions 1 || echo "Topic $topic may already exist"
done

echo "Kafka topics created successfully."
