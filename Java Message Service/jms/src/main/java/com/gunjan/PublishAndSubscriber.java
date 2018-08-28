package com.gunjan;

import java.util.Properties;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * @author Gunjan
 * 
 *         This class illustrate Point to Point JMS messaging. <br/>
 *         ActiveMQTextMessage {commandId = 5, responseRequired = true,
 *         messageId = ID:Gunjan-HP-49659-1390023190339-3:1:1:1:1,
 *         originalDestination = null, originalTransactionId = null, producerId
 *         = ID:Gunjan-HP-49659-1390023190339-3:1:1:1, destination =
 *         queue://GunjanQ, transactionId = null, expiration = 0, timestamp =
 *         1390023190620, arrival = 0, brokerInTime = 1390023190620,
 *         brokerOutTime = 1390023190635, correlationId = helloCorrId, replyTo =
 *         null, persistent = true, type = null, priority = 4, groupID = null,
 *         groupSequence = 0, targetConsumerId = null, compressed = false,
 *         userID = null, content =
 *         org.apache.activemq.util.ByteSequence@528acf6e, marshalledProperties
 *         = null, dataStructure = null, redeliveryCounter = 0, size = 0,
 *         properties = null, readOnlyProperties = true, readOnlyBody = true,
 *         droppable = false, jmsXGroupFirstForConsumer = false, text = Hello..,
 *         Plz process me}
 */
public class PublishAndSubscriber {

	public static void main(String[] args) throws Exception {
		thread(new Subscriber("Subscriber 1"), false);
		// thread(new Subscriber("Subscriber 2"), false);
		/**
		 * Here Publisher 1, broadcast a message to Subscriber 1 and Subscriber
		 * 2 as these Subscriber subscribes to GunjanT topic.
		 */
		// thread(new Publisher("Publisher 1"), false);
	}

	public static void thread(Runnable runnable, boolean daemon) {
		Thread brokerThread = new Thread(runnable);
		brokerThread.setDaemon(daemon);
		brokerThread.start();
	}

	public static class Publisher implements Runnable {
		String senderId;

		public void run() {
			try {
				Properties jndiProps = new Properties();
				jndiProps
						.setProperty(Context.INITIAL_CONTEXT_FACTORY,
								"org.apache.activemq.jndi.ActiveMQInitialContextFactory");
				jndiProps.setProperty(Context.PROVIDER_URL,
						"tcp://localhost:61616");
				javax.naming.Context jndiContext = new InitialContext(jndiProps);
				ActiveMQConnectionFactory activeMQConnectionFactory = (ActiveMQConnectionFactory) jndiContext
						.lookup("ConnectionFactory");

				/**
				 * Place below xml inside apache-activemq/conf/activemq.xml file
				 * to create the queue and topic at active mq startup
				 * 
				 * <broker xmlns="http://activemq.apache.org/schema/core"
				 * brokerName="localhost" dataDirectory="${activemq.data}">
				 * <destinations> <queue physicalName="GunjanQ" /> <topic
				 * physicalName="GunjanT" /> </destinations> <broker>
				 */

				Topic topic = (Topic) jndiContext
						.lookup("dynamicTopics/GunjanT");
				TopicConnection topicConnection = activeMQConnectionFactory
						.createTopicConnection();
				topicConnection.start();

				/**
				 * AUTO_ACKNOWLEDGE, NO need to call message.acknowledge() to
				 * ACK, client runtime will auto ACK the JMS Server. And after
				 * getting the ACK the JMS Server delete the NON-PERSISTENT MSG
				 * and PERSISTENT MSG from memory and database/file respectively
				 */
				TopicSession topicSession = topicConnection.createTopicSession(
						false, Session.AUTO_ACKNOWLEDGE);

				/**
				 * TemporaryTopic is a temporary topic which lasts only as long
				 * as its associated client connection is active. Guaranteed to
				 * be unique across each connection. Here in this example this
				 * TemporaryTopic is used by the Consumer to reply back to
				 * Producer
				 */
				TemporaryTopic temporaryTopic = topicSession
						.createTemporaryTopic();
				/**
				 * DUPS_OK_ACKNOWLEDGE, instructs JMS Server its OK to send
				 * duplicate message to receive, so extra overhead can be remove
				 * to achieve at-once delivery, Here receive are tolerant to
				 * receive duplicate message
				 */
				// QueueSession queueSession =
				// queueConnection.createQueueSession(false,
				// Session.DUPS_OK_ACKNOWLEDGE);
				/**
				 * CLIENT_ACKNOWLEDGE, need to call message.acknowledge() to
				 * ACK, client runtime will not auto ACK the JMS Server. And
				 * after getting the ACK the JMS Server delete the
				 * NON-PERSISTENT MSG and PERSISTENT MSG from memory and
				 * database/file respectively. If its not called the message
				 * will be there in memory/db and re-deliver to the receiver
				 * with re-delivery flag as true, msg may expire and can be lost
				 * if its non-persistent msg
				 */
				// QueueSession queueSession =
				// queueConnection.createQueueSession(false,
				// Session.CLIENT_ACKNOWLEDGE);
				TextMessage textMessage = topicSession.createTextMessage();
				textMessage.setJMSCorrelationID("helloCorrId");
				textMessage.setText("Hello.., Plz process me");
				textMessage.setJMSReplyTo(temporaryTopic);
				/**
				 * PERSISTENT message is made persistent by JMS Server after
				 * receiving message from sender and once the ACK received by
				 * JMS Server from Receiver the message is deleted from the
				 * persistent store, If there is no ACK from the Receiver then
				 * JMS provide sends the message again to the receiver after
				 * making the re-delivered flag as true
				 */
				// textMessage.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
				textMessage.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
				TopicPublisher topicPublisher = topicSession
						.createPublisher(topic);
				TopicSubscriber topicSubscriber = topicSession
						.createSubscriber(temporaryTopic);
				topicSubscriber.setMessageListener(new MessageListener() {
					public void onMessage(Message message) {
						System.out.println("ACK from reciver | " + message);
					}
				});

				/**
				 * QueueSender.send() is synchronous. This will be blocking call
				 * till JMS Provider send the ACK.[Warning this ACK is from JMS
				 * Provide, not from the actual message receiver ], JMS server
				 * send the ACK to sender once its received the NON-PERSISTENT,
				 * NON-PERSISTENT message will be there in memory, JMS server
				 * sends the ACK to sender after Saving the PERSISTENT message
				 * to disk/file.
				 */
				topicPublisher.send(textMessage);

				/**
				 * Here calls to publish() and receive() is replaced with one
				 * call to request(), and its a synchronous call
				 */
				// TopicRequestor topicRequestor = new
				// TopicRequestor(topicSession, topic);
				// Message replyMsg = topicRequestor.request(textMessage);
				// System.out.println("replyMsg="+replyMsg);

			} catch (Exception e) {
				System.out.println("Caught: " + e);
				e.printStackTrace();
			}
		}

		public Publisher(String senderId) {
			super();
			this.senderId = senderId;
		}
	}

	public static class Subscriber implements Runnable {
		String receiverId;

		public void run() {
			try {
				Properties jndiProps = new Properties();
				jndiProps
						.setProperty(Context.INITIAL_CONTEXT_FACTORY,
								"org.apache.activemq.jndi.ActiveMQInitialContextFactory");
				jndiProps.setProperty(Context.PROVIDER_URL,
						"tcp://localhost:61616");
				javax.naming.Context jndiContext = new InitialContext(jndiProps);
				ActiveMQConnectionFactory activeMQConnectionFactory = (ActiveMQConnectionFactory) jndiContext
						.lookup("ConnectionFactory");
				Topic topic = (Topic) jndiContext
						.lookup("dynamicTopics/GunjanT");
				TopicConnection topicConnection = activeMQConnectionFactory
						.createTopicConnection();
				/**
				 * Below line is must for durable Subscription
				 */
				topicConnection.setClientID("DurableSubs");
				topicConnection.start();
				final TopicSession topicSession = topicConnection
						.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
				/**
				 * Below line creates a durable subscription for topic GunjanT,
				 * Means Message will not lost if the subscriber is temporary
				 * off-line, once the durable subscriber comes online it will
				 * receive the messages which was accumulated in JMS Server when
				 * durable subscriber was off-line, A durable subscription's
				 * uniqueness is defined by the ClientID set above and the
				 * Subscription name set below
				 */
				TopicSubscriber topicSubscriber = topicSession
						.createDurableSubscriber(topic, "Subscription Name");
				topicSubscriber.setMessageListener(new MessageListener() {

					public void onMessage(Message messgae) {
						TextMessage msg = (TextMessage) messgae;
						System.out.println("msg=" + msg);
						try {
							/**
							 * Incoming message contain the destination where to
							 * reply that message is successfully received, OR
							 * this destination can be used for further
							 * forwarding message to the attached destination
							 */
							Topic temporaryTopic = (Topic) msg.getJMSReplyTo();
							TopicPublisher topicPublisher = topicSession
									.createPublisher(temporaryTopic);
							TextMessage textMessage = topicSession
									.createTextMessage();
							/**
							 * JMS Correlation Id can be used as a identity of
							 * Subscriber to the Producer. Another way to
							 * associate Subscriber identity with the reply
							 * message would be to store something unique in a
							 * message property, or in the message body itself,
							 * A more common use of Correlation Id is not for
							 * the sake of establishing identity, it is for
							 * correlating the asynchronous reception of message
							 * with a message that had be previously sent. A
							 * message consumer wishing to create a message to
							 * be used as a response may place JMSMessageID of
							 * the original message in the JMSCorrelationID of
							 * the response message
							 * 
							 */
							textMessage.setJMSCorrelationID(messgae
									.getJMSMessageID());
							textMessage.setText("ACK");
							topicPublisher.send(textMessage);
							/**
							 * We can use below overridden method to set
							 * DeliveryMode, PRIORITY and time-to-live
							 */
							// topicPublisher.send(textMessage,DeliveryMode.PERSISTENT,
							// Message.DEFAULT_PRIORITY,1800000);

						} catch (JMSException e1) {
							e1.printStackTrace();
						}

					}
				});

				/**
				 * On Occasion If we want to create synchronous request-reply
				 * conversation. then we can use TopicSubscriber.receive() which
				 * is blocking call till message received from the Publisher,
				 * The receive() is a way of proactively asking for the message
				 * rather than passively receiving it through the onMessage()
				 */
				// while (true) {
				// /**
				// * Blocking call till message is received
				// */
				// TextMessage msg = (TextMessage) topicSubscriber.receive();
				// /**
				// * Blocking call till message is received or after 30 seconds
				// */
				// //TextMessage msg = (TextMessage)
				// topicSubscriber.receive(30000);
				// /**
				// * Return null if there nothing pending to delivered
				// */
				// //TextMessage msg = (TextMessage)
				// topicSubscriber.receiveNoWait();
				// //msg.acknowledge();
				// System.out.println("receiverId" + receiverId + " msg="
				// + msg.getText());
				// System.out.println("receiverId" + receiverId + " msg="
				// + msg.getJMSCorrelationID());
				// }

				/**
				 * Way to un-subscriber to a topic
				 */
				// topicSession.unsubscribe("dynamicTopics/GunjanT");

			} catch (Exception e) {
				System.out.println("Caught: " + e);
				e.printStackTrace();
			}
		}

		public Subscriber(String receiverId) {
			super();
			this.receiverId = receiverId;
		}
	}
}
