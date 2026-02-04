import os, time, smtplib
from email.mime.text import MIMEText
from datetime import datetime, timezone
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
DB, COLL = "bdproj", "alerts"

# ---- Email (optional) ----
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
EMAIL_TO  = os.getenv("EMAIL_TO")   # comma-separated if multiple
EMAIL_FROM= os.getenv("EMAIL_FROM", SMTP_USER)

# ---- SMS via Twilio (optional) ----
TWILIO_SID   = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
TWILIO_FROM  = os.getenv("TWILIO_FROM")
SMS_TO       = os.getenv("SMS_TO")

def send_email(subj, body):
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and EMAIL_TO):
        return
    msg = MIMEText(body)
    msg["Subject"], msg["From"], msg["To"] = subj, EMAIL_FROM, EMAIL_TO
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(EMAIL_FROM, EMAIL_TO.split(","), msg.as_string())

def send_sms(text):
    if not (TWILIO_SID and TWILIO_TOKEN and TWILIO_FROM and SMS_TO):
        return
    from twilio.rest import Client
    Client(TWILIO_SID, TWILIO_TOKEN).messages.create(
        body=text, from_=TWILIO_FROM, to=SMS_TO
    )

def fmt_alert(a):
    return (f"[ALERT] {a.get('date'):%Y-%m-%d} hour={a.get('hour')}  "
            f"hits={a.get('hits')} errors={a.get('errors')}  "
            f"at {a.get('created_at')} (epoch_id={a.get('epoch_id')})")

def main():
    cli = MongoClient(MONGO_URI)
    coll = cli[DB][COLL]

    # resume from newest doc we’ve already seen
    last_ts = coll.find_one(sort=[("created_at", -1)])
    last_seen = last_ts["created_at"] if last_ts else datetime(1970,1,1, tzinfo=timezone.utc)

    print("Notifier watching for new alerts after:", last_seen.isoformat())
    while True:
        new_alerts = list(coll.find({"created_at": {"$gt": last_seen}})
                            .sort("created_at", 1))
        for a in new_alerts:
            text = fmt_alert(a)
            print(text)
            send_email("Spark Log ALERT", text)
            send_sms(text)
            last_seen = a["created_at"]
        time.sleep(10)

if __name__ == "__main__":
    main()
