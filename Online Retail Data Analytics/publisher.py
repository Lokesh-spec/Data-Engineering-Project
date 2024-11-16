import logging
import argparse
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PubSubPublisher:
    def __init__(self, project_id: str):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id

    def create_topic(self, topic_name: str) -> None:
        topic = f"projects/{self.project_id}/topics/{topic_name}"
        try:
            self.publisher.create_topic(name=topic)
            logging.info(f"Topic '{topic_name}' created successfully.")
        except AlreadyExists:
            logging.info(f"Topic '{topic_name}' already exists.")

    def publish_message(self, topic_name: str, message: str) -> None:
        topic = f"projects/{self.project_id}/topics/{topic_name}"
        future = self.publisher.publish(topic, message.encode('utf-8'))
        logging.info(f"Message '{message}' published to topic '{topic_name}'.")

def main():
    parser = argparse.ArgumentParser(description="Publisher: Creating topic and publishing messages.")
    parser.add_argument("--project_id", type=str, required=True, help="Project ID of GCP project")
    parser.add_argument("--topic_name", type=str, required=True, help="Topic Name")
    parser.add_argument("--message", type=str, required=True, help="Message to publish")

    args = parser.parse_args()
    project_id = args.project_id
    topic_name = args.topic_name
    message = args.message

    publisher = PubSubPublisher(project_id=project_id)
    
    # Create the topic if it doesn't exist
    publisher.create_topic(topic_name=topic_name)
    
    # Publish the message to the topic
    publisher.publish_message(topic_name=topic_name, message=message)

if __name__ == "__main__":
    main()
