import logging
import argparse
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PubSubSubscriber:
    def __init__(self, project_id: str):
        self.subscriber = pubsub_v1.SubscriberClient()
        self.project_id = project_id

    def create_subscription(self, topic_name: str, subscriber_name: str) -> None:
        topic = f"projects/{self.project_id}/topics/{topic_name}"
        subscription = f"projects/{self.project_id}/subscriptions/{subscriber_name}"

        try:
            self.subscriber.create_subscription(name=subscription, topic=topic)
            logging.info(f"Subscription '{subscriber_name}' created successfully.")
        except AlreadyExists:
            logging.info(f"Subscription '{subscriber_name}' already exists.")

    ## Testing purpose
    def listen_for_messages(self, subscriber_name: str) -> None:
        subscription = f"projects/{self.project_id}/subscriptions/{subscriber_name}"

        def callback(message):
            logging.info(f"Received message: {message.data.decode('utf-8')}")
            message.ack()
            logging.info("Message acknowledged.")

        logging.info(f"Listening for messages on subscription '{subscriber_name}'...")
        future = self.subscriber.subscribe(subscription, callback)

        try:
            future.result()
        except KeyboardInterrupt:
            logging.info("Subscription listener stopped.")
            future.cancel()

    def pull_messages(self, subscriber_name: str) -> None:
        subscription = f"projects/{self.project_id}/subscriptions/{subscriber_name}"
        logging.info("Pulling messages manually...")
        try:
            response = self.subscriber.pull(subscription=subscription, max_messages=10)
            if not response.received_messages:
                logging.info("No messages in the subscription.")
            else:
                for msg in response.received_messages:
                    logging.info(f"Pulled message: {msg.message.data.decode('utf-8')}")
                    self.subscriber.acknowledge(subscription=subscription, ack_ids=[msg.ack_id])
                    logging.info("Message acknowledged.")
        except Exception as e:
            logging.error(f"Error pulling messages: {e}")

def main():
    parser = argparse.ArgumentParser(description="Subscriber: Creating subscription and listening for messages.")
    parser.add_argument("--project_id", type=str, required=True, help="Project ID of GCP project")
    parser.add_argument("--topic_name", type=str, required=True, help="Topic Name")
    parser.add_argument("--subscriber_name", type=str, required=True, help="Subscriber Name")
    parser.add_argument("--manual_pull", action='store_true', help="Use manual pull instead of real-time listener")

    args = parser.parse_args()
    project_id = args.project_id
    topic_name = args.topic_name
    subscriber_name = args.subscriber_name

    subscriber = PubSubSubscriber(project_id=project_id)
    subscriber.create_subscription(topic_name=topic_name, subscriber_name=subscriber_name)

    # if args.manual_pull:
    #     subscriber.pull_messages(subscriber_name=subscriber_name)
    # else:
    #     subscriber.listen_for_messages(subscriber_name=subscriber_name)

if __name__ == "__main__":
    main()
