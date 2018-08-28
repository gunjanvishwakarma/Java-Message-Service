package com.gunjan;

import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.MapMessage;
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
import javax.naming.NamingException;

import org.apache.activemq.ActiveMQConnectionFactory;

public class PointToPoint2 {

	public static void main(String[] args) throws Exception {
		thread(new Receiver(), false);
		thread(new Sender(), false);
	}

	public static void thread(Runnable runnable, boolean daemon) {
		Thread brokerThread = new Thread(runnable);
		brokerThread.setDaemon(daemon);
		brokerThread.start();
	}

	public static class Sender implements Runnable {
		private QueueConnection queueConnection = null;
		private QueueSession queueSession = null;
		private Queue queueIn = null;
		private Queue queueOut = null;

		public Sender() {
			try {
				// Connect to the provider and get the JMS connection
				Properties jndiProps = new Properties();
				jndiProps
						.setProperty(Context.INITIAL_CONTEXT_FACTORY,
								"org.apache.activemq.jndi.ActiveMQInitialContextFactory");
				jndiProps.setProperty(Context.PROVIDER_URL,
						"tcp://localhost:61616");
				Context ctx = new InitialContext(jndiProps);
				ActiveMQConnectionFactory amqConnFactory = (ActiveMQConnectionFactory) ctx
						.lookup("ConnectionFactory");
				queueConnection = amqConnFactory.createQueueConnection();

				// Create the JMS Session
				queueSession = queueConnection.createQueueSession(false,
						Session.AUTO_ACKNOWLEDGE);

				// Lookup the request and response queues
				queueIn = (Queue) ctx.lookup("dynamicQueues/QueueIN");
				queueOut = (Queue) ctx.lookup("dynamicQueues/QueueOUT");

				// Now that setup is complete, start the Connection
				queueConnection.start();
			} catch (JMSException jmse) {
				jmse.printStackTrace();
				System.exit(1);
			} catch (NamingException jne) {
				jne.printStackTrace();
				System.exit(1);
			}
		}

		private void sendAndReceiveMessage(String textMsg) {
			try {
				// Create JMS message
				MapMessage msg = queueSession.createMapMessage();
				msg.setString("TextMsg", textMsg);
				msg.setJMSReplyTo(queueOut);

				// Create the sender and send the message
				QueueSender qSender = queueSession.createSender(queueIn);
				System.out.println("Sender ===> Message Sent");
				qSender.send(msg);

				// Wait to see if the loan request was accepted or declined
				String filter = "JMSCorrelationID = '" + msg.getJMSMessageID()
						+ "'";
				QueueReceiver qReceiver = queueSession.createReceiver(queueOut,
						filter);
				TextMessage tmsg = (TextMessage) qReceiver.receive(30000);
				if (tmsg == null) {
					System.out.println("Receiver not responding");
				} else {
					System.out.println("Sender ===> Message received," + tmsg.getText());
				}
			} catch (JMSException jmse) {
				jmse.printStackTrace();
				System.exit(1);
			}
		}

		public void run() {
			new Sender().sendAndReceiveMessage("Hello! How Are You?");
		}
	}

	public static class Receiver implements Runnable, MessageListener {
		private QueueConnection queueConnection = null;
		private QueueSession queueSession = null;
		private Queue queueIn = null;

		public Receiver() {
			try {
				// Connect to the provider and get the JMS connection
				Properties jndiProps = new Properties();
				jndiProps
						.setProperty(Context.INITIAL_CONTEXT_FACTORY,
								"org.apache.activemq.jndi.ActiveMQInitialContextFactory");
				jndiProps.setProperty(Context.PROVIDER_URL,
						"tcp://localhost:61616");
				Context ctx = new InitialContext(jndiProps);
				ActiveMQConnectionFactory amqConnFactory = (ActiveMQConnectionFactory) ctx
						.lookup("ConnectionFactory");
				queueConnection = amqConnFactory.createQueueConnection();

				// Create the JMS Session
				queueSession = queueConnection.createQueueSession(false,
						Session.AUTO_ACKNOWLEDGE);
				// Lookup the request queue
				queueIn = (Queue) ctx.lookup("dynamicQueues/QueueIN");
				// Now that setup is complete, start the Connection
				queueConnection.start();
				// Create the message listener
				QueueReceiver qReceiver = queueSession.createReceiver(queueIn);
				qReceiver.setMessageListener(this);
			} catch (JMSException jmse) {
				jmse.printStackTrace();
				System.exit(1);
			} catch (NamingException jne) {
				jne.printStackTrace();
				System.exit(1);
			}
		}

		public void onMessage(Message message) {
			try {
				// Get the data from the message
				MapMessage mapMsg = (MapMessage) message;
				String msg = mapMsg.getString("TextMsg");
				System.out.println("Receiver ===> Message Received:" + msg);

				// Create the sender and send the message
				QueueSender qSender = queueSession.createSender((Queue) message
						.getJMSReplyTo());

				// Send the results back to the borrower
				TextMessage msgToReply = queueSession.createTextMessage();
				msgToReply.setText("I am fine");
				msgToReply.setJMSCorrelationID(message.getJMSMessageID());
				System.out.println("Receiver ===> Message Sent");
				qSender.send(msgToReply);
			} catch (JMSException jmse) {
				jmse.printStackTrace();
				System.exit(1);
			} catch (Exception jmse) {
				jmse.printStackTrace();
				System.exit(1);
			}
		}

		public void run() {
			new Receiver();
		}

	}
}
