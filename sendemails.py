import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders
import boto3
from botocore.exceptions import ClientError
def send_email():

   
    AWS_REGION = "us-east-1"

    
    SUBJECT = "Daily Report"
    msg = MIMEMultipart()
    msg['Subject'] = SUBJECT 
    msg['From'] = self.EMAIL_FROM
    msg['To'] = ', '.join(self.EMAIL_TO)

    part = MIMEBase('application', "octet-stream")
    part.set_payload(open("my_report.csv", "rb").read())
    msg.attach(part)        
   
    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=AWS_REGION)

    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    msg['To'],
                ],
            },
            Message={
                'Body': {
                    
                    'Text': {
        
                        'Data': msg.as_string()
                    },
                },
                'Subject': {

                    'Data': msg['Subject']
                },
            },
            Source=msg['From']
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

def lambda_handler(event, context):
    send_email()
