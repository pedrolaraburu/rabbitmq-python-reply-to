import pika
import psycopg2

def on_request_message_received(ch, method, properties, body):
    print(f"Received Request: {properties.correlation_id}\n Message: {body}")
    
    conn = psycopg2.connect(
        user='postgres', password='1234', host='localhost', port='5432', database='postgres'
    )
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS registries (id SERIAL PRIMARY KEY, message TEXT)")
    conn.commit()

    cursor.execute("INSERT INTO registries (message) VALUES (%s)", (f'Request received: {properties.correlation_id}',))
    conn.commit()

    cursor.execute("SELECT * FROM registries")
    conn.commit()
    result = cursor.fetchall()

    cursor.close()
    conn.close()

    ch.basic_publish('', routing_key=properties.reply_to, body=f'Request received: {properties.correlation_id}\n Result: {result}')

connection_parameters = pika.ConnectionParameters(host='localhost', port=5672, credentials=pika.PlainCredentials(username='user', password='1234'))
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()

channel.queue_declare(queue='request-queue')

channel.basic_consume(queue='request-queue', auto_ack=True, on_message_callback=on_request_message_received)

print("Starting Server")
channel.start_consuming()
