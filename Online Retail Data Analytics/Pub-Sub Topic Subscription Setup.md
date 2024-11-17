
This document provides a detailed breakdown of the two Python scripts for **Pub/Sub Publisher** and **Pub/Sub Subscriber** using Google Cloud Pub/Sub, and their purpose is to create a topic and a subscription, respectively. The testing part involves publishing a message to the topic and listening for messages through the subscription.

## 1. Pub/Sub Publisher

### Objective:

- **Create a Pub/Sub Topic**: The `PubSubPublisher` class handles the creation of a Pub/Sub topic.
- **Publish a Message**: This part allows testing by sending a message to the created topic.

### Code Breakdown:

#### **Imports:**

``` python
import logging 
import argparse 
from google.cloud import pubsub_v1 
from google.api_core.exceptions import AlreadyExists
```


- **logging**: Used for logging the execution details, which helps in debugging and keeping track of events.
- **argparse**: Provides command-line interface (CLI) support, allowing users to pass project ID, topic name, and message as arguments.
- **pubsub_v1**: Google Cloud Pub/Sub client library for publishing messages and creating topics.
- **AlreadyExists**: Handles the exception that occurs if the topic already exists in the project.

#### **PubSubPublisher Class:**

``` python
class PubSubSubscriber:
def __init__(self, project_id: str):
	self.subscriber = pubsub_v1.SubscriberClient()
	self.project_id = project_id
```

- The class **`PubSubPublisher`** initializes the `PublisherClient` to interact with Pub/Sub services.
- `project_id` is required to interact with the Pub/Sub topic in the specific GCP project.

#### **Method: `create_topic`**

``` python
def create_subscription(self, topic_name: str, subscriber_name: str) -> None:
    topic = f"projects/{self.project_id}/topics/{topic_name}"
    subscription = f"projects/{self.project_id}/subscriptions/{subscriber_name}"

    try:
        self.subscriber.create_subscription(name=subscription, topic=topic)
        logging.info(f"Subscription '{subscriber_name}' created successfully.")
    except AlreadyExists:
        logging.info(f"Subscription '{subscriber_name}' already exists.")
```

- This method constructs the full topic path and attempts to create the topic.
- If the topic already exists, it logs the message instead of raising an error.

#### **Method: `publish_message`**

``` python
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

```

- This method publishes the given message to the specified topic.
- The message is encoded into UTF-8 format, then sent to the topic.
- The future result is obtained to ensure the message is successfully published.

#### **Main Function:**

``` python
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

```

- The script accepts arguments for `project_id`, `topic_name`, and `message`.
- After initializing the `PubSubPublisher` class, it creates the topic and, if needed, publishes a test message.

### Running the Publisher:

- The publisher script can be run by passing the `project_id`, `topic_name`, and `message` through the command line:

``` bash
python publisher.py --project_id YOUR_PROJECT_ID --topic_name YOUR_TOPIC_NAME --message "Test Message"
```

---

## 2. Pub/Sub Subscriber

### Objective:

- **Create a Pub/Sub Subscription**: The `PubSubSubscriber` class is responsible for creating a subscription to a Pub/Sub topic.
- **Listen for Messages**: It listens to messages in real-time, processes them as they arrive, and acknowledges receipt.

### Code Breakdown:

#### **Imports:**

``` python
import logging
import argparse
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists
```

- Similar to the publisher script, these imports are used for logging, argument parsing, and interacting with Google Cloud Pub/Sub.

#### **PubSubSubscriber Class:**

``` python
class PubSubSubscriber:
    def __init__(self, project_id: str):
        self.subscriber = pubsub_v1.SubscriberClient()
        self.project_id = project_id
```

#### **Method: `create_subscription`**

``` python
def create_subscription(self, topic_name: str, subscriber_name: str) -> None:
    topic = f"projects/{self.project_id}/topics/{topic_name}"
    subscription = f"projects/{self.project_id}/subscriptions/{subscriber_name}"

    try:
        self.subscriber.create_subscription(name=subscription, topic=topic)
        logging.info(f"Subscription '{subscriber_name}' created successfully.")
    except AlreadyExists:
        logging.info(f"Subscription '{subscriber_name}' already exists.")
```

- This method constructs the full paths for the subscription and topic, then tries to create the subscription.
- If the subscription already exists, it logs a message indicating this.

#### **Method: `listen_for_messages`**

``` python
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
```

- This method listens for incoming messages on the specified subscription.
- The `callback` function processes each received message by logging its content and acknowledging it.

#### **Method: `pull_messages`**

``` python
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

```

- This method manually pulls messages from the subscription, instead of listening in real-time.
- It retrieves up to 10 messages and acknowledges each one after processing.

#### **Main Function:**

``` python
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
```

- This script accepts arguments for `project_id`, `topic_name`, and `subscriber_name`, then creates the subscription.
- It can either listen for messages in real-time or pull messages manually.

### Running the Subscriber:

- The subscriber script can be run by passing the `project_id`, `topic_name`, and `subscriber_name` through the command line:

``` bash
python subscriber.py --project_id YOUR_PROJECT_ID --topic_name YOUR_TOPIC_NAME --subscriber_name YOUR_SUBSCRIBER_NAME
```

---

## Create and List Notification using GCP CLI

``` bash
gsutil notification create -t projects/kinetic-guild-441706-k8/topics/retail-data-by-day-region -f json -e OBJECT_FINALIZE gs://online-retail-invoice
```

``` bash
gsutil notification list gs://online-retail-invoice
```

## Conclusion:

- The **publisher script** is responsible for creating a topic and sending test messages to it.
- The **subscriber script** creates a subscription and listens for messages (or pulls them manually for testing).
- These scripts form the foundation for interacting with Pub/Sub, creating topics and subscriptions, and processing messages. They can be expanded to handle different message processing workflows in real-time streaming applications.


