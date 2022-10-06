import boto3
from botocore.exceptions import ClientError


SENDER = "trainee.john.andemeskel@sigmalabs.co.uk"
RECIPIENT = "trainee.john.andemeskel@sigmalabs.co.uk"
AWS_REGION = "us-east-1"
SUBJECT = "Important Message: Heart rate alert"
BODY_TEXT = (" Your heart rate was picked up at an abnormal rhythm, please seek medical attention! ")
BODY_HTML = """
<html>
  <head></head>
  <body>
    <h1> Your heart rate was picked up at an abnormal rhythm, please seek medical attention! </h1>
  </body>
</html>  
"""             
CHARSET = "UTF-8"

def heart_rate_boundaries(age: int) -> int:
    """
    Calculate heart rate boundaries given age
    """
    max_hr = 220 - age
    resting_hr = 40
    return resting_hr, max_hr

def is_abnormal(heart_rate: int, age:int) -> bool:
    """
    Compare current heart rate to boundaries, to establish abnormal heart rate
    """
    lower_boundary,upper_boundary = heart_rate_boundaries(age)
    if heart_rate > upper_boundary or heart_rate < lower_boundary:
        return True
    else:
        return False

def create_email():
    """
    Builds the email to be sent as a heart-rate alert
    """
    client = boto3.client('ses',region_name=AWS_REGION)
    response = client.send_email(
                Destination=
                {'ToAddresses': [RECIPIENT]},
                Message={
                    'Body': {
                        'Html': {'Charset': CHARSET,'Data': BODY_HTML},
                        'Text': {'Charset': CHARSET, 'Data': BODY_TEXT}
                    },
                    'Subject': {
                        'Charset': CHARSET, 'Data': SUBJECT
                        }
                    },
                Source=SENDER,
            )
    return response


def send_alert():
    """
    Fires off the email to the rider if abnormal heart rate occurs
    """
    try:
        response = create_email()
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])