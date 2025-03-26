from confluent_kafka import Consumer

def main():
    conf = {'bootstrap.servers': 'localhost:29092',
            'group.id': 'financial_transaction_consumer',
            'auto.offset.reset': 'earliest'}
    # earliest | smallest
    consumer = Consumer(conf)

    # Subscribe to topic
    topic = "partition_tutorial"
    consumer.subscribe([topic])

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')[:20]))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

if __name__ == "__main__":
    main()