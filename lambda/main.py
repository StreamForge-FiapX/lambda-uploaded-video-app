import json
import pika
import logging
import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SECRET_NAME = "rabbitmq_credentials"  
REGION_NAME = "sa-east-1" 

def get_secret():
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=REGION_NAME)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=SECRET_NAME)
        secret = get_secret_value_response["SecretString"]
        return json.loads(secret)
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Erro ao obter segredo do Secrets Manager: {e}")
        raise e

def send_to_rabbitmq(message, rabbitmq_config):
    credentials = pika.PlainCredentials(rabbitmq_config["username"], rabbitmq_config["password"])
    parameters = pika.ConnectionParameters(
        host=rabbitmq_config["host"],
        port=rabbitmq_config["port"],
        credentials=credentials
    )

    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        channel.queue_declare(queue=rabbitmq_config["queue"], durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=rabbitmq_config["queue"],
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  
            )
        )

        logger.info(f"Mensagem enviada para o RabbitMQ: {message}")

    except Exception as e:
        logger.error(f"Erro ao enviar mensagem para o RabbitMQ: {e}")
        raise e

    finally:
        if 'connection' in locals() and connection.is_open:
            connection.close()

def lambda_handler(event, context):
    try:
        rabbitmq_config = get_secret()

        for record in event['Records']:
            sqs_message = json.loads(record['body'])
            logger.info(f"Mensagem recebida do SQS: {sqs_message}")
            send_to_rabbitmq(sqs_message, rabbitmq_config)

        return {
            'statusCode': 200,
            'body': json.dumps('Mensagens processadas com sucesso!')
        }

    except Exception as e:
        logger.error(f"Erro ao processar evento: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Erro ao processar mensagens.')
        }
