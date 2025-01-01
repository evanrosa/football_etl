FROM confluentinc/cp-kafka:latest

# Add any custom configuration or scripts if needed
COPY configs/server.properties /etc/kafka/server.properties

EXPOSE 9092

CMD ["/etc/confluent/docker/run"]
