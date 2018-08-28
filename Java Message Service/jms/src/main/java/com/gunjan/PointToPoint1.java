package com.gunjan;

import java.util.Properties;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * @author Gunjan
 * 
 *         This class illustrate Point to Point JMS
 *         messaging.
 *         <br/>
 *         ActiveMQTextMessage {commandId = 5, responseRequired =
 *         true, messageId = ID:Gunjan-HP-49659-1390023190339-3:1:1:1:1,
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
public class PointToPoint1 {

	public static void main(String[] args) throws Exception {
		thread(new Receiver("Receiver 1"), false);
		thread(new Sender("Sender 1"), false);
	}

	public static void thread(Runnable runnable, boolean daemon) {
		Thread brokerThread = new Thread(runnable);
		brokerThread.setDaemon(daemon);
		brokerThread.start();
	}

	public static class Sender implements Runnable {
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

				Queue queue = (Queue) jndiContext
						.lookup("dynamicQueues/GunjanQ");
				QueueConnection queueConnection = activeMQConnectionFactory
						.createQueueConnection();
				queueConnection.start();

				/**
				 * AUTO_ACKNOWLEDGE, NO need to call message.acknowledge() to
				 * ACK, client runtime will auto ACK the JMS Server. And after
				 * getting the ACK the JMS Server delete the NON-PERSISTENT MSG
				 * and PERSISTENT MSG from memory and database/file respectively
				 */
				QueueSession queueSession = queueConnection.createQueueSession(
						false, Session.AUTO_ACKNOWLEDGE);
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
				TextMessage textMessage = queueSession.createTextMessage();
				textMessage.setJMSCorrelationID("helloCorrId");
				textMessage.setText("Hello.., Plz process me");
				/**
				 * PERSISTENT message is made persistent by JMS Server after
				 * receiving message from sender and once the ACK received by
				 * JMS Server from Receiver the message is deleted from the
				 * persistent store, If there is no ACK from the Receiver then
				 * JMS provide sends the message again to the receiver after
				 * making the re-delivered flag as true
				 */
				//textMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
				textMessage.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
				QueueSender queueSender = queueSession.createSender(queue);
				/**
				 * QueueSender.send() is synchronous. This will be blocking call
				 * till JMS Provider send the ACK.[Warning this ACK is from JMS
				 * Provide, not from the actual message receiver ], JMS server
				 * send the ACK to sender once its received the NON-PERSISTENT,
				 * NON-PERSISTENT message will be there in memory, JMS server
				 * sends the ACK to sender after Saving the PERSISTENT message
				 * to disk/file.
				 */
				queueSender.send(textMessage);
			} catch (Exception e) {
				System.out.println("Caught: " + e);
				e.printStackTrace();
			}
		}

		public Sender(String senderId) {
			super();
			this.senderId = senderId;
		}
	}

	public static class Receiver implements Runnable {
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
				Queue queue = (Queue) jndiContext
						.lookup("dynamicQueues/GunjanQ");
				QueueConnection queueConnection = activeMQConnectionFactory
						.createQueueConnection();
				queueConnection.start();
				QueueSession queueSession = queueConnection.createQueueSession(
						false, Session.DUPS_OK_ACKNOWLEDGE);
				QueueReceiver queueReceiver = queueSession
						.createReceiver(queue);
				queueReceiver.setMessageListener(new MessageListener() {

					public void onMessage(Message messgae) {
						TextMessage msg = (TextMessage) messgae;
						try {
							// msg.acknowledge();
							System.out.println("msg=" + msg);
						} catch (Exception e) {
							e.printStackTrace();
						}

					}
				});
				/*
				 * while (true) { TextMessage msg =
				 * (TextMessage)queueReceiver.receive(); msg.acknowledge();
				 * System.out.println("receiverId" + receiverId + " msg=" +
				 * msg.getText()); System.out.println("receiverId" + receiverId
				 * + " msg=" + msg.getJMSCorrelationID()); }
				 */

			} catch (Exception e) {
				System.out.println("Caught: " + e);
				e.printStackTrace();
			}
		}

		public Receiver(String receiverId) {
			super();
			this.receiverId = receiverId;
		}
	}
}
