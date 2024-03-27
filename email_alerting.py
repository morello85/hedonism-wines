import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from queries import *
import duckdb


# Specify the file path for the DuckDB database
db_path = '/Users/MacUser/hedonism-wines_app/database.db'  # Example path, replace with your desired path

# Establish a connection to an in-memory DuckDB database
conn = duckdb.connect(database=db_path, read_only=False)

df = queries.query_discounted_items()

def is_dataframe_empty(df):
    return df.empty

def send_email(subject, body):
    # Email configuration
    sender_email = "your_email@example.com"
    receiver_email = "recipient@example.com"
    password = "your_email_password"

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = receiver_email

    # Attach body to email
    msg.attach(MIMEText(body, 'plain'))

    # Send the message via SMTP server.
    try:
        server = smtplib.SMTP('smtp.example.com', 587)  # Update with your SMTP server details
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print("Failed to send email. Error:", str(e))

# Example usage
df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})

if not is_dataframe_empty(df):
    subject = "DataFrame is not empty"
    body = "Your DataFrame is not empty. It contains data:\n\n" + str(df)
    send_email(subject, body)
else:
    print("DataFrame is empty. No email sent.")
