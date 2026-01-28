import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# # Read the database file path from the environment variable
# db_path = os.getenv('DB_PATH')

# # Establish a connection to an in-memory DuckDB database
# conn = duckdb.connect(database=db_path, read_only=False)

def is_dataframe_empty(df):
    return df.empty

def send_email(subject, body):
    # Email configuration
    sender_email = "dariomorellialerts@gmail.com"
    receiver_email = "morello85@gmail.com"
    password = os.environ.get('EMAIL_PASSWORD')
    if password is None:
        print ("Error: EMAIL PASSWORD env var not set.")
        return

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = receiver_email

    # Attach body to email
    msg.attach(MIMEText(body, 'html'))

    # Send the message via SMTP server.
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)  # Update with your SMTP server details
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
        print("Email sent successfully.")
    except Exception as e:
        print("Failed to send email. Error:", str(e))
    
    return

# def main():
#     # Your main logic for processing
#     ...

#     # Make sure to close the connection after all operations are done
#     conn.close()

# if __name__ == "__main__":
#     main()
