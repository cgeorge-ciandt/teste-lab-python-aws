from flask import Flask
import boto3
import json
from PyPDF2 import PdfReader
import io
from dotenv import load_dotenv


load_dotenv()

app = Flask(__name__)

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

bucket_name = 'test-lab-python'
queue_name = 'test-lab-python'

@app.route("/")
def homepage():
    return 'API está ON!!!'

@app.route("/processar-mensagem")
def processarMensagem():
    bucket_name, object_key = processar_mensagem_sqs()
    text = processar_arquivo_s3(bucket_name, object_key)
    return text

def appRun():
    app.run(debug=False, port=8000)

def processar_arquivo_s3(bucket_name, object_key):
    try:
        # Fazer o download do arquivo S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read()

        # Adicionar aqui o processamento do conteúdo do arquivo

        pdf_reader = PdfReader(io.BytesIO(file_content))

        text = ''
        for page in pdf_reader.pages:
            text += page.extract_text()

        text = text.replace('.','')

        s3.delete_object(
            Bucket=bucket_name,
            Key=object_key
        )

        return text

    except Exception as e:
        print(f"Erro ao processar arquivo S3: {e}")

def processar_mensagem_sqs():
    try:

        queue_url = get_queue_url(queue_name)

        print("Queue URL: ",queue_url)

        # Receber mensagem da fila SQS
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )

        queue = sqs.get_queue_by_name(QueueName=queue_name)

        print("Fila: ", queue)

        # Processar a mensagem
        messages = response.get('Messages', [])
        if messages:
            for message in messages:
                body = json.loads(message['Body'])
                # Verificar se a mensagem contém registros de eventos S3
                if 'Records' in body:
                    for record in body['Records']:
                        if record['eventSource'] == 'aws:s3':
                            bucket_name = record['s3']['bucket']['name']
                            object_key = record['s3']['object']['key']
                            print(f"Arquivo carregado no S3: {bucket_name}/{object_key}")
                            

                            # Apagar a mensagem da fila depois de processá-la
                            receipt_handle = message['ReceiptHandle']
                            sqs.delete_message(
                                QueueUrl=queue_url,
                                ReceiptHandle=receipt_handle
                            )
                            print("Mensagem apagada da fila.")
                            return [bucket_name, object_key]
        else:
            print("Nenhuma mensagem recebida.")

    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")

def get_queue_url(queue):
    print("Queue: ", queue)
    response = sqs.get_queue_url(
        QueueName=queue,
    )
    print("Get queue url Response: ", response)
    return response["QueueUrl"]


if __name__ == "__main__":
    appRun()
