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

    def listen_for_messages(self, subscriber_name: str) -> None:
        subscription = f"projects/{self.project_id}/subscriptions/{subscriber_name}"

        def callback(message):
            logging.info(f"Received message: {message.data.decode('utf-8')}")
            message.ack()

        future = self.subscriber.subscribe(subscription, callback)
        logging.info(f"Listening for messages on '{subscriber_name}'...")

        try:
            future.result()  
        except KeyboardInterrupt:
            logging.info("Subscription listener stopped.")
            future.cancel()

def main():
    parser = argparse.ArgumentParser(description="Subscriber: Creating subscription and listening for messages.")
    parser.add_argument("--project_id", type=str, required=True, help="Project ID of GCP project")
    parser.add_argument("--topic_name", type=str, required=True, help="Topic Name")
    parser.add_argument("--subscriber_name", type=str, required=True, help="Subscriber Name")

    args = parser.parse_args()
    project_id = args.project_id
    topic_name = args.topic_name
    subscriber_name = args.subscriber_name

    subscriber = PubSubSubscriber(project_id=project_id)
    
    subscriber.create_subscription(topic_name=topic_name, subscriber_name=subscriber_name)
    
    subscriber.listen_for_messages(subscriber_name=subscriber_name)

if __name__ == "__main__":
    main()
