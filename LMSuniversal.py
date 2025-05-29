from io import BytesIO
import uuid
import os
import numpy as np
from mysql.connector import Error
from flask import Flask, request, jsonify, session, render_template, redirect, url_for, send_file,flash, make_response
from datetime import datetime, timedelta
import pandas as pd
from xhtml2pdf import pisa
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
from psycopg2 import sql
import openpyxl
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.styles import PatternFill, Font
from werkzeug.utils import secure_filename
import matplotlib.pyplot as plt
import io
import base64
import json
import requests
import pdfkit
from weasyprint import HTML



today_date = datetime.now().strftime('%d %B %Y')
applied_date = datetime.now().strftime('%Y-%m-%d')

app = Flask(__name__)
app.secret_key = 'your_secret_key'  
app.secret_key = '011235'
app.permanent_session_lifetime = timedelta(minutes=30)
user_sessions = {}

external_database_url = "postgresql://lmsdatabase_8ag3_user:6WD9lOnHkiU7utlUUjT88m4XgEYQMTLb@dpg-ctp9h0aj1k6c739h9di0-a.oregon-postgres.render.com/lmsdatabase_8ag3"
database = 'lmsdatabase_8ag3'

connection = psycopg2.connect(external_database_url)

cursor = connection.cursor()

# WhatsApp API Credentials (Replace with your actual credentials)
ACCESS_TOKEN = "EAATESj1oB5YBOyIVfVPEAIZAZA7sgPboDN36Wa2Or11uZCBEZCVWaNAZB0exkYYG6gcIdiYbvPCST9tKjS54ib1NqXbNg7UvJYaZCIZAjxgTBQwvyoWE8cZCMgje1wkrUyb335TMwNwYSTA3rNwppRZAeQGt3M7s5x15nZCbZBtEfZBtSIu3p7ZCHOcF0pMTuLgjQreLz2QZDZD"
PHONE_NUMBER_ID = "558392750697195"
VERIFY_TOKEN = "521035180620700"
WHATSAPP_API_URL = f"https://graph.facebook.com/v18.0/{PHONE_NUMBER_ID}/messages"

create_table_query = f"""
CREATE TABLE whatsapptempapplication (
    id SERIAL PRIMARY KEY,
    empidwa INT,
    leavetypewa VARCHAR (100),
    startdate date,
    enddate date
);
"""
#cursor.execute(create_table_query)
#kjconnection.commit()

cursor.execute("""
    ALTER TABLE whatsapptempapplication
    ADD COLUMN IF NOT EXISTS companynamewa VARCHAR(255);
""")


#cursor.execute(create_table_query)
connection.commit()
print(f"column added to Table whatsapptempapplication successfully!")

def send_whatsapp_message(to, text, buttons=None):
    """Function to send a WhatsApp message using Meta API, with optional buttons."""
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    # If buttons are provided, send an interactive message
    if buttons:
        data = {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "interactive",
            "interactive": {
                "type": "button",
                "body": {"text": text},
                "action": {
                    "buttons": buttons
                }
            }
        }
    else:
        # Send a normal text message
        data = {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "text",
            "text": {"body": text}
        }

    response = requests.post(WHATSAPP_API_URL, headers=headers, json=data)
    
    # Debugging logs
    print("✅ Sending message to:", to)
    print("📩 Message body:", text)
    print("📡 WhatsApp API Response Status:", response.status_code)

    try:
        response_json = response.json()
        print("📝 WhatsApp API Response Data:", response_json)
    except Exception as e:
        print("❌ Error parsing response JSON:", e)

    return response.json()




def send_whatsapp_list_message(recipient, text, list_title, sections):
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": recipient,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "header": {
                "type": "text",
                "text": list_title
            },
            "body": {
                "text": text
            },
            "action": {
                "button": "Select Option",
                "sections": sections
            }
        }
    }
    
    response = requests.post(
        f"https://graph.facebook.com/v18.0/{PHONE_NUMBER_ID}/messages",
        headers=headers,
        json=payload
    )
    
    print("List message response:", response.json())
    return response

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        if request.args.get("hub.verify_token") == VERIFY_TOKEN:
            return request.args.get("hub.challenge")
        return "Verification failed", 403

    if request.method == "POST":
        global today_date
        data = request.get_json()
        print("📥 Full incoming data:", json.dumps(data, indent=2))

        if data and "entry" in data:
            for entry in data["entry"]:
                for change in entry["changes"]:
                    if "messages" in change["value"]:
                        for message in change["value"]["messages"]:

                            conversation_id = str(uuid.uuid4())
                            session['conversation_id'] = conversation_id
                        

                            sender_id = message["from"]
                            sender_number = sender_id[-9:]
                            print(f"📱 Conversation {conversation_id}: Sender's WhatsApp number: {sender_number}")
                            session['client'] = str(sender_number)

                            external_database_url = "postgresql://lmsdatabase_8ag3_user:6WD9lOnHkiU7utlUUjT88m4XgEYQMTLb@dpg-ctp9h0aj1k6c739h9di0-a.oregon-postgres.render.com/lmsdatabase_8ag3"

                            try:
                                connection = psycopg2.connect(external_database_url)
                                cursor = connection.cursor()

                                cursor.execute("""
                                    SELECT table_name 
                                    FROM information_schema.tables 
                                    WHERE table_schema = 'public'
                                """)
                                tables = cursor.fetchall()

                                for table in tables:
                                    table_name = table[0]  
                                    
                                    cursor.execute("""
                                        SELECT COUNT(*)
                                        FROM information_schema.columns
                                        WHERE table_schema = 'public'
                                        AND table_name = %s
                                        AND column_name IN ('password')
                                    """, (table_name,))
                                    
                                    if cursor.fetchone()[0] == 1:
                                        query = f"""
                                            SELECT * FROM {table_name}
                                            WHERE whatsapp::TEXT LIKE %s
                                        """
                                        cursor.execute(query, (f"%{sender_number}",))
                                        result = cursor.fetchone()

                                        print(result)

                                        if result:
                                            id_user = result[0]  
                                            first_name = result[1]  
                                            last_name = result[2]  
                                            whatsapp_foc_8 = f"0{result[3]}"
                                            email_foc_8 = result[4]
                                            address_foc_8 = result[5]
                                            role_foc_8 = result[8]
                                            days_days_balance = result[13]
                                            company_reg = table_name[:-4]  

                                            print(id_user)
                                            print(first_name)
                                            print(last_name)
                                            print(role_foc_8)

                                            print(f"Credentials found in table: {table_name}")
                                            first_column_value = result[0]
                                            print(f"First column value: {first_column_value}")
                                            continue
                                        
                                        else: 
                                            send_whatsapp_message(
                                                sender_id, 
                                                "Oops, you are not registered. Kindly get in touch with your leave administrator for assistance."
                                            )
                                            
                                            return jsonify({"status": "received"}), 200 
                                            
                                        return jsonify({"status": "received"}), 200    
                                               
                            finally:
                                if connection:
                                    print('DONE')


                            if role_foc_8 == "Ordinary User":

                                table_namexxxx = company_reg + "main"        

                                query = f"SELECT id FROM {table_namexxxx} WHERE leaveapproverid = {str(id_user)};"
                                cursor.execute(query)
                                rows = cursor.fetchall()

                                df_employeesempapp = pd.DataFrame(rows, columns=["id"])



                                if len(df_employeesempapp) == 0:
                                

                                    if message.get("type") == "interactive":
                                        interactive = message.get("interactive", {})


                                        if interactive.get("type") == "list_reply":
                                            selected_option = interactive.get("list_reply", {}).get("id")
                                            print(f"📋 User selected: {selected_option}")

                                            if selected_option in ["Annual","Sick","Study","Parental", "Bereavement","Other"] :
                                                button_id_leave_type = str(selected_option)

                                                cursor.execute("""
                                                    DELETE FROM whatsapptempapplication
                                                    WHERE empidwa = %s
                                                """, (str(id_user),))  
                                                
                                                connection.commit()

                                                cursor.execute(f"""
                                                    INSERT INTO whatsapptempapplication (empidwa, leavetypewa, companynamewa)
                                                    VALUES (%s, %s, %s)
                                                """, (id_user, button_id_leave_type, company_reg))

                                                connection.commit()

                                                send_whatsapp_message(
                                                    sender_id, 
                                                    f"Ok. When would you like your {selected_option} Leave to start {first_name}?\n\n"
                                                    "Please enter your response using the format: 👇🏻\n"
                                                    "`start 24 january 2025`"
                                                )

                                                continue



                                        elif interactive.get("type") == "button_reply":
                                            button_id = interactive.get("button_reply", {}).get("id")
                                            print(f"🔘 Button clicked: {button_id}")
                                            
                                            if button_id == "Apply":

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"

                                                query = f"SELECT id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor  FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor"])    

                                                if len(df_employeesappspendingcheck) == 0:

                                                    sections = [
                                                        {
                                                            "title": "Leave Type Options",
                                                            "rows": [
                                                                {"id": "Annual", "title": "Annual Leave"},
                                                                {"id": "Sick", "title": "Sick Leave"},
                                                                {"id": "Study", "title": "Study Leave"},
                                                                {"id": "Bereavement", "title": "Bereavement Leave"},
                                                                {"id": "Parental", "title": "Parental Leave"},
                                                                {"id": "Other", "title": "Other"},
                                                            ]
                                                        }
                                                    ]

                                                    send_whatsapp_list_message(
                                                        sender_id, 
                                                        f"{first_name}, kindly select the type of Leave that you are applying for.", 
                                                        "Leave Type Options",
                                                        sections) 

                                                elif len(df_employeesappspendingcheck) > 0:
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Reminder", "title": "Remind Approver"}},
                                                        {"type": "reply", "reply": {"id": "Cancelapp", "title": "Cancel Pending App"}},
                                                    ]
                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"Oops! 🥲. Sorry {first_name}, you cannot apply for leave whilst you have another leave application which is still pending approval.\n\n" 
                                                        f"Your `{df_employeesappspendingcheck.iat[0,1]}` Leave Application `[ID - {df_employeesappspendingcheck.iat[0,0]}]` applied on `{df_employeesappspendingcheck.iat[0,3].strftime('%d %B %Y')}` for `{df_employeesappspendingcheck.iat[0,6]} days from {df_employeesappspendingcheck.iat[0,4].strftime('%d %B %Y')} to {df_employeesappspendingcheck.iat[0,5].strftime('%d %B %Y')}` is still pending approval from {df_employeesappspendingcheck.iat[0,2]}.\n\n" 
                                                        f"Select an option below to either remind the approver to approved your pending application or you can cancel the pending application to submit a new leave application."         
                                                        , 
                                                        buttons
                                                    )

                                            elif button_id == "Track":

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                table_name_apps_approved = f"{company_reg}appsapproved"
                                                table_name_apps_declined = f"{company_reg}appsdeclined"
                                                table_name_apps_cancelled = f"{company_reg}appscancelled"


                                                query = f"SELECT id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leaveapproverwhatsapp  FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor", "leaveapproverwhatsapp"])    

                                                if len(df_employeesappspendingcheck) == 0:

                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf  FROM {table_name_apps_approved} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf"]) 

                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf  FROM {table_name_apps_declined} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappsdeclinedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf"])  
                            
                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf  FROM {table_name_apps_cancelled} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappscancelledcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf"])
                            
                                                    all_approved_declined = df_employeesappsapprovedcheck._append(df_employeesappsdeclinedcheck)
                                                    all_approved_declined_cancelled = all_approved_declined._append(df_employeesappscancelledcheck)
                                                    all_approved_declined_cancelled = all_approved_declined_cancelled.sort_values(by="appid", ascending=False)  

                                                    if len(all_approved_declined_cancelled) > 0:

                                                        print(f" hhhhhhhhhhhhhhhhhhhh  {all_approved_declined_cancelled.iat[0,8] }")

                                                        if all_approved_declined_cancelled.iat[0,8] == "Approved":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Revoke", "title": "Revoke Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, your recent `{all_approved_declined_cancelled.iat[0,2]}` Leave Application `[ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}` was {all_approved_declined_cancelled.iat[0,8]}✅ by `{all_approved_declined_cancelled.iat[0,3].title()}` on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}`." 
                                                            )


                                                            def generate_leave_pdf():
                                                                app = {
                                                                    'company_logo': 44,
                                                                    'company_name': company_reg.replace("_"," ").title(),
                                                                    'employee_name': f"{first_name} {last_name}",
                                                                    'leave_type': all_approved_declined_cancelled.iat[0,2],
                                                                    'generated_on': today_date,
                                                                    'date_applied': all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y'),
                                                                    'approver_name': all_approved_declined_cancelled.iat[0,3].title(),
                                                                    'reference_number': all_approved_declined_cancelled.iat[0,0],
                                                                    'approved_date': all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y'),
                                                                    'new_balance': all_approved_declined_cancelled.iat[0,10],
                                                                    'start_date':  all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y'),
                                                                    'end_date':  all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y'),
                                                                    'days_requested':  all_approved_declined_cancelled.iat[0,7], 
                                                                    'address': address_foc_8, 
                                                                    'whatsapp': whatsapp_foc_8, 
                                                                    'email': email_foc_8, 
                                                                    'status': 'Approved'
                                                                }

                                                                html_out = render_template("leave_pdf_template.html", app=app)
                                                                
                                                                # ✅ Return as bytes instead of saving to file
                                                                pdf_bytes = HTML(string=html_out).write_pdf()
                                                                return pdf_bytes

                                                            
                                                            global ACCESS_TOKEN
                                                            global PHONE_NUMBER_ID

                                                            def upload_pdf_to_whatsapp(pdf_bytes):
                                                                compxxy = company_reg.replace("_"," ").title()
                                                                filename=f"leave_application_{all_approved_declined_cancelled.iat[0,0]}_{first_name}_{last_name}_{compxxy}.pdf"
                                                            
                                                                url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/media"
                                                                headers = {
                                                                    "Authorization": f"Bearer {ACCESS_TOKEN}"
                                                                }

                                                                files = {
                                                                    "file": (filename, io.BytesIO(pdf_bytes), "application/pdf"),
                                                                    "type": (None, "application/pdf"),
                                                                    "messaging_product": (None, "whatsapp")
                                                                }

                                                                response = requests.post(url, headers=headers, files=files)
                                                                print("📥 Full incoming data:", response.text)  # Good for debugging
                                                                response.raise_for_status()
                                                                return response.json()["id"]

                                                                                                            
                                                            def send_whatsapp_pdf_by_media_id(recipient_number, media_id):
                                                                compxxy = company_reg.replace("_"," ").title()
                                                                filename=f"leave_application_{all_approved_declined_cancelled.iat[0,0]}_{first_name}_{last_name}_{compxxy}.pdf"
                                                                url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
                                                                headers = {
                                                                    "Authorization": f"Bearer {ACCESS_TOKEN}",
                                                                    "Content-Type": "application/json"
                                                                }
                                                                payload = {
                                                                    "messaging_product": "whatsapp",
                                                                    "to": recipient_number,
                                                                    "type": "document",
                                                                    "document": {
                                                                        "id": media_id,            # Media ID from upload step
                                                                        "filename": filename       # Desired file name on recipient's phone
                                                                    }
                                                                }

                                                                response = requests.post(url, headers=headers, json=payload)
                                                                response.raise_for_status()
                                                                return response.json()


                                                            pdf_path = generate_leave_pdf()
                                                            media_id = upload_pdf_to_whatsapp(pdf_path)
                                                            send_whatsapp_pdf_by_media_id(sender_id, media_id)

                                                            send_whatsapp_message(
                                                                sender_id,
                                                                "Select an option below to continue 👇",
                                                                buttons
                                                            )

                                                        elif all_approved_declined_cancelled.iat[0,8] == "Declined":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, your recent `{all_approved_declined_cancelled.iat[0,2]}` Leave Application `[ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}` was {all_approved_declined_cancelled.iat[0,8]}❌ by `{all_approved_declined_cancelled.iat[0,3].title()}` on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}`.",
                                                                buttons 
                                                            )

                                                        elif all_approved_declined_cancelled.iat[0,8] == "Cancelled":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}` you Cancelled ⛔ your recent `{all_approved_declined_cancelled.iat[0,2]} Leave Application [ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}`.",
                                                                buttons 
                                                            )

                                                    else:

                                                        buttons = [
                                                            {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                            {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}}
                                                        ]
                                                        companyxx = company_reg.replace("_"," ").title()
                                                        send_whatsapp_message(
                                                            sender_id, 
                                                            f"Hello {first_name} {last_name} from {companyxx}!\n\n You have not applied for any leave days yet.", 
                                                            buttons
                                                        )


                                                elif len(df_employeesappspendingcheck) > 0:
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Reminder", "title": "Remind Approver"}},
                                                        {"type": "reply", "reply": {"id": "Cancelapp", "title": "Cancel Pending App"}},
                                                    ]
                                                    approoooover = df_employeesappspendingcheck.iat[0,2].title()
                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"Hey {first_name}, your recent `{df_employeesappspendingcheck.iat[0,1]}` Leave Application `[ID - {df_employeesappspendingcheck.iat[0,0]}]` applied on `{df_employeesappspendingcheck.iat[0,3].strftime('%d %B %Y')}` for `{df_employeesappspendingcheck.iat[0,6]} days from {df_employeesappspendingcheck.iat[0,4].strftime('%d %B %Y')} to {df_employeesappspendingcheck.iat[0,5].strftime('%d %B %Y')}` is still pending approval from `{approoooover}`.\n\n" 
                                                        f"Select an option below to either remind `{approoooover}` to approve your pending leave application or you can cancel the pending application to submit a new leave application."         
                                                        , 
                                                        buttons
                                                    )

                                            elif button_id == "Submitapp":
                                    
                                                try:

                                                    table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                    table_name_apps_approved = f"{company_reg}appsapproved"

                                                    query = f"SELECT id FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()

                                                    df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id"])    

                                                    if len(df_employeesappspendingcheck) == 0:

                                                        cursor.execute("""
                                                            SELECT id ,empidwa, leavetypewa, startdate, enddate FROM whatsapptempapplication
                                                            WHERE empidwa = %s
                                                        """, (str(id_user)))
                                                
                                                        result = cursor.fetchone()

                                                        appid = result[0]
                                                        leavetype = result[2]
                                                        startdate = result[3]
                                                        enddate = result[4]
                                                        table_name = f"{company_reg}main"

                                                        if isinstance(startdate, str):
                                                            startdate = datetime.datetime.strptime(startdate, "%Y-%m-%d").date()
                                                        if isinstance(enddate, str):
                                                            enddate = datetime.datetime.strptime(enddate, "%Y-%m-%d").date()

                                                        business_days = 0
                                                        current_date = startdate

                                                        while current_date <= enddate:
                                                            if current_date.weekday() < 5:  # 0=Mon, 1=Tue, ..., 4=Fri
                                                                business_days += 1
                                                            current_date += timedelta(days=1)  # Use timedelta directly

                                                        query = f"SELECT id, firstname, surname, whatsapp, email, address, role, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, monthlyaccumulation, department FROM {table_name};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()

                                                        df_employees = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Address", "Role","Leave Approver Name","Leave Approver ID","Leave Approver Email", "Leave Approver WhatsAapp", "Leave Days Balance","Days Accumulated per Month", "Department"])
                                                        print(df_employees)
                                                        userdf = df_employees[df_employees['id'] == int(np.int64(id_user))].reset_index()
                                                        print("yeaarrrrr")
                                                        print(userdf)
                                                        firstname = userdf.iat[0,2]
                                                        surname = userdf.iat[0,3]
                                                        whatsapp = userdf.iat[0,4]
                                                        address = userdf.iat[0,6]
                                                        email = userdf.iat[0,5]
                                                        fullnamedisp = firstname + ' ' + surname
                                                        leaveapprovername = userdf.iat[0,8]
                                                        leaveapproverid = userdf.iat[0,9]
                                                        leaveapproveremail = userdf.iat[0, 10]
                                                        leaveapproverwhatsapp = userdf.iat[0,11]
                                                        role = userdf.iat[0,7]
                                                        leavedaysbalance = userdf.iat[0,12]
                                                        department = userdf.iat[0,14]
                                                        print('check')

                                                        departmentdf = df_employees[df_employees['Department'] == department].reset_index()
                                                        numberindepartment = len(departmentdf)

                                                        startdatex = pd.Timestamp(startdate)
                                                        enddatex = pd.Timestamp(enddate)

                                                        leave_dates = pd.date_range(startdatex, enddatex)

                                                        query = f"""
                                                            SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate,
                                                                leaveenddate, leavedaysappliedfor, approvalstatus, statusdate,
                                                                leavedaysbalancebf, department
                                                            FROM {table_name_apps_approved}
                                                            WHERE department = %s;
                                                        """
                                                        cursor.execute(query, (department,))
                                                        rows = cursor.fetchall()

                                                        df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf","department"]) 

                                                        # Create daily impact report
                                                        impact_report = []

                                                        for date in leave_dates:
                                            
                                                            date = pd.Timestamp(date)

                                                            df_employeesappsapprovedcheck["leavestartdate"] = pd.to_datetime(df_employeesappsapprovedcheck["leavestartdate"])
                                                            df_employeesappsapprovedcheck["leaveenddate"] = pd.to_datetime(df_employeesappsapprovedcheck["leaveenddate"])

                                                            print(type(date))  # Should be pandas._libs.tslibs.timestamps.Timestamp or datetime.datetime
                                                            print(df_employeesappsapprovedcheck.dtypes)  # Check all datetime columns

                                                            on_leave = ((df_employeesappsapprovedcheck["leavestartdate"] <= date) & (df_employeesappsapprovedcheck["leaveenddate"] >= date)).sum()
                                                            remaining = numberindepartment - on_leave - 1  # subtract 1 for the new leave
                                                            impact_report.append({
                                                                "date": date.strftime("%Y-%m-%d"),
                                                                "on leave (including new)": on_leave + 1,
                                                                "employees remaining": remaining
                                                            })

                                                        # Convert to DataFrame for display
                                                        impact_df = pd.DataFrame(impact_report)
                                                        print("IMPAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACT")
                                                        print(impact_df)
                                                        print(numberindepartment)

                                                        impact_df["date"] = pd.to_datetime(impact_df["date"], dayfirst=True)
                                                        impact_df = impact_df[impact_df["date"].dt.weekday != 6].copy()

                                                        impact_df["group"] = (impact_df[["on leave", "employees remaining"]] != impact_df[["on leave", "employees remaining"]].shift()).any(axis=1).cumsum()

                                                        statements = []
                                                        for _, group_df in impact_df.groupby("group"):
                                                            start = group_df["date"].iloc[0].strftime("%d %B %Y")
                                                            end = group_df["date"].iloc[-1].strftime("%d %B %Y")
                                                            on_leave = group_df["on leave"].iloc[0]
                                                            remaining = group_df["employees remaining"].iloc[0]
                                                            
                                                            if start == end:
                                                                statements.append(f"On {start}, the {department} department will have {remaining} employee(s) remaining at work and {on_leave} employee(s) on leave.")
                                                            else:
                                                                statements.append(f"From {start} to {end}, the {department} department will have {remaining} employee(s) remaining at work and {on_leave} employee(s) on leave.")

                                                        # Combine all statements into a single variable
                                                        final_summary = "\n".join(statements)
                                                        # Print output
                                                        for s in statements:
                                                            print(s)

                                                        leavedaysbalancebf = int(leavedaysbalance) - int(business_days)

                                                        status = "Pending"

                                                        insert_query = f"""
                                                        INSERT INTO {table_name_apps_pending_approval} (id, firstname, surname, department, leavetype, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus)
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                        """
                                                        cursor.execute(insert_query, (int(np.int64(id_user)), first_name, last_name, department, leavetype, leaveapprovername, int(np.int64(leaveapproverid)), leaveapproveremail, int(np.int64(leaveapproverwhatsapp)), int(np.int64(leavedaysbalance)), today_date, startdate, enddate, int(np.int64(business_days)), int(np.int64(leavedaysbalancebf)), status))
                                                        connection.commit()

                                                        query = f"SELECT appid FROM {table_name_apps_pending_approval};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()

                                                        df_employees = pd.DataFrame(rows, columns=["id"])
                                                        leaveappid = df_employees.iat[0,0]
                                                        companyxx = company_reg.replace("_"," ").title()
                                                        approovvver = leaveapprovername.title()

                                                        send_whatsapp_message(sender_id, f"✅ Great News {first_name} from {companyxx}! \n\n Your `{leavetype} Leave Application` for `{business_days} days` from `{startdate.strftime('%d %B %Y')}` to `{enddate.strftime('%d %B %Y')}` has been submitted successfully!\n\n"
                                                            f"Your Leave Application ID is `{leaveappid}`.\n\n"
                                                            f"A Notification has been sent to `{approovvver}`  on `+263{leaveapproverwhatsapp}` to decide on  your application.\n\n"
                                                            "To Check the approval status of your leave application, type `Hello` then select `Track Application`.")
                                                        
                                                        if leaveapproverwhatsapp:
            
                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": f"Approve5appwa_{leaveappid}", "title": "Approve"}},
                                                                {"type": "reply", "reply": {"id": f"Disapproveappwa_{leaveappid}", "title": "Disapprove"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                f"263{leaveapproverwhatsapp}", 
                                                                f"Hey {approovvver}! 😊. New `{leavetype}` Leave Application from `{first_name} {surname}` for `{business_days} days` from `{startdate.strftime('%d %B %Y')}` to `{enddate.strftime('%d %B %Y')}`.\n\n"                 
                                                                f"If you approve this leave application, {final_summary}\n\n"  
                                                                f"Select an option below to either approve or disapprove the application."         
                                                                , 
                                                                buttons
                                                            )

                                                    else:
                                                        print("leave app submission failed")

                                                except ValueError as e:
                                                    send_whatsapp_message(
                                                        sender_id,
                                                        f"{e}, ❌ No, incorrect message format. Please use:\n"
                                                        "`end 24 january 2025`\n"
                                                        "Example: `end 15 march 2024`"
                                                    )

                                            elif button_id == "Checkbal":

                                                buttons = [
                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                {"type": "reply", "reply": {"id": "Track", "title": "Track Application"}},
                                                ]

                                                send_whatsapp_message(
                                                    sender_id, 
                                                    f"Hey {first_name}, your current available leave days balance is `{days_days_balance} days`.\n\n"
                                                    "Select an option below to continue 👇",
                                                    buttons
                                                )

                                            elif button_id == "Resubmitapp" :
                                                table_name_apps_cancelled = f"{company_reg}appscancelled"
                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"

                                                query = f"SELECT appid, id, firstname, surname, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail , leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf FROM {table_name_apps_pending_approval} WHERE id = %s;"
                                                cursor.execute(query, (id_user,))
                                                result = cursor.fetchone()
                                                if result:
                                                    (app_id, employee_number, first_name, surname, leave_type,  leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf) = result
                                                
                                                    try:
                                                            status = "Cancelled"
                                                            statusdate = today_date
                                                        
                                                            insert_query = f"""
                                                            INSERT INTO {table_name_apps_cancelled} 
                                                            (appid, id, firstname, surname, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                            """

                                                            cursor.execute(insert_query, (
                                                                app_id, employee_number, first_name, surname, leave_type, leave_specify, approver_name, 
                                                                approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                                                end_date, leave_days, leavedaysbalancebf, status, statusdate
                                                            ))
                                                            
                                                            connection.commit()
                                                            print("Insert successful!")

                                                    except Exception as e:
                                                        print("Error inserting data:", e)

                                                    # SQL query to delete or mark the leave as canceled
                                                    query = f"""DELETE FROM {table_name_apps_pending_approval} WHERE appid = %s"""
                                                    cursor.execute(query, (app_id,))
                                                    connection.commit()                                       

                                                    companyxx = company_reg.replace("_", " ").title()
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                        {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                        {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                    ]

                                                    send_whatsapp_message(sender_id, f"Hey {first_name} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}` has been Cancelled successfully✅!\n\n"
                                                        "Select an option below to continue 👇",
                                                        buttons
                                                    )                                          
                                                
                                                else:
                                                    print("No record found for the user.")


                                            elif button_id == "Cancelapp" :

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                table_name_apps_cancelled = f"{company_reg}appscancelled"

                                                query = f"SELECT appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail , leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf FROM {table_name_apps_pending_approval} WHERE id = %s;"
                                                cursor.execute(query, (id_user,))
                                                result = cursor.fetchone()
                                                if result:
                                                    (app_id, employee_number, first_name, surname, department, leave_type,  leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf) = result
                                                
                                                    try:
                                                            status = "Cancelled"
                                                            statusdate = today_date
                                                        
                                                            insert_query = f"""
                                                            INSERT INTO {table_name_apps_cancelled} 
                                                            (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                            """

                                                            cursor.execute(insert_query, (
                                                                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                                                                approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                                                end_date, leave_days, leavedaysbalancebf, status, statusdate
                                                            ))
                                                            
                                                            connection.commit()
                                                            print("Insert successful!")

                                                    except Exception as e:
                                                        print("Error inserting data:", e)

                                                    # SQL query to delete or mark the leave as canceled
                                                    query = f"""DELETE FROM {table_name_apps_pending_approval} WHERE appid = %s"""
                                                    cursor.execute(query, (app_id,))
                                                    connection.commit()                                       

                                                    companyxx = company_reg.replace("_", " ").title()
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                        {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                        {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                    ]

                                                    send_whatsapp_message(sender_id, f"Hey {first_name} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}` has been Cancelled successfully✅!\n\n"
                                                        "Select an option below to continue 👇",
                                                        buttons
                                                    )                                          
                                                
                                                else:
                                                    print("No record found for the user.")

                                    else:

                                        text = message.get("text", {}).get("body", "").lower()
                                        print(f"📨 Message from {sender_id}: {text}")
                                        
                                        print("yearrrrrrrrrrrrrrrrrrrrrrrrrrrssrsrsrsrsrs")

                                        print(role_foc_8)

                                        if "hello" in text.lower():
                                            buttons = [
                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                {"type": "reply", "reply": {"id": "Track", "title": "Track Application"}},
                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}}
                                            ]
                                            companyxx = company_reg.replace("_"," ").title()
                                            send_whatsapp_message(
                                                sender_id, 
                                                f"Hello {first_name} {last_name} from {companyxx}!\n\n Echelon Bot Here 😎. How can I assist you?", 
                                                buttons
                                            )


                                        elif "start" in text.lower():

                                            date_part = text.split("start", 1)[1].strip()

                                            cursor.execute("""
                                                UPDATE whatsapptempapplication
                                                SET startdate = %s
                                                WHERE empidwa = %s
                                            """, (date_part, id_user))

                                            connection.commit()

                                            cursor.execute("""
                                                SELECT empidwa, leavetypewa FROM whatsapptempapplication
                                                WHERE empidwa = %s
                                            """, (str(id_user)))
                                    
                                            result = cursor.fetchone()

                                            if result:
                                                leavetypewa = result[1] 

                                            cursor.execute("SELECT * FROM whatsapptempapplication")
                                            columns = [desc[0] for desc in cursor.description]
                                            records = cursor.fetchall()
                                            
                                            df = pd.DataFrame(records, columns=columns)
                                            
                                            print("\n📊 whatsapptempapplication Table:")
                                            print(df)
                                            
                                            try:
                                                parsed_date = datetime.strptime(date_part, "%d %B %Y")
                                                send_whatsapp_message(sender_id, "✅ Yes! Valid start date format.\n\n"
                                                    f"Now Enter the last day that you will be on {leavetypewa} Leave.Use the format: 👇🏻\n"
                                                    "`end 24 january 2025`"                      
                                                                    )
                                                
                                            except ValueError:
                                                send_whatsapp_message(
                                                    sender_id,
                                                    f"❌ No, incorrect message format, {first_name}. Please use:\n"
                                                    "`start 24 january 2025`\n"
                                                    "Example: `start 15 march 2024`"
                                                )

                                        elif "end" in text.lower():

                                            date_part = text.split("end", 1)[1].strip()

                                            cursor.execute("""
                                                UPDATE whatsapptempapplication
                                                SET enddate = %s
                                                WHERE empidwa = %s
                                            """, (date_part, id_user))

                                            connection.commit()

                                            cursor.execute("""
                                                SELECT id ,empidwa, leavetypewa, startdate, enddate FROM whatsapptempapplication
                                                WHERE empidwa = %s
                                            """, (str(id_user)))
                                    
                                            result = cursor.fetchone()

                                            appid = result[0]
                                            leavetype = result[2]
                                            startdate = result[3]
                                            enddate = result[4]

                                            if isinstance(startdate, str):
                                                startdate = datetime.datetime.strptime(startdate, "%Y-%m-%d").date()
                                            if isinstance(enddate, str):
                                                enddate = datetime.datetime.strptime(enddate, "%Y-%m-%d").date()

                                            business_days = 0
                                            current_date = startdate

                                            while current_date <= enddate:
                                                if current_date.weekday() < 5:  # 0=Mon, 1=Tue, ..., 4=Fri
                                                    business_days += 1
                                                current_date += timedelta(days=1)  # Use timedelta directly

                                            print(f"📅 Business days between {startdate} and {enddate}: {business_days}")


                                            buttons = [
                                                {"type": "reply", "reply": {"id": "Submitapp", "title": "Yes, Submit"}},
                                                {"type": "reply", "reply": {"id": "Dontsubmit", "title": "No"}}
                                            ]
                                            send_whatsapp_message(
                                                sender_id, 
                                                f"Do you wish to submit your `{business_days} day {leavetype} Leave Application` leave starting from `{startdate.strftime('%d %B %Y')}` to `{enddate.strftime('%d %B %Y')}` {first_name} ?", 
                                                buttons
                                            )

                                        else:
                                            send_whatsapp_message(
                                                sender_id, 
                                                "Echelon Bot Here 😎. Say 'hello' to start!"
                                            )


                                elif len(df_employeesempapp) > 0:

                                    print("uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu")
                                

                                    if message.get("type") == "interactive":
                                        interactive = message.get("interactive", {})

                                        if interactive.get("type") == "list_reply":

                                            selected_option = interactive.get("list_reply", {}).get("id")
                                            print(f"📋 User selected: {selected_option}")

                                            if selected_option in ["Annual","Sick","Study","Parental", "Bereavement","Other"] :

                                                button_id_leave_type = str(selected_option)

                                                cursor.execute("""
                                                    DELETE FROM whatsapptempapplication
                                                    WHERE empidwa = %s
                                                """, (str(id_user),))  
                                                
                                                connection.commit()

                                                cursor.execute(f"""
                                                    INSERT INTO whatsapptempapplication (empidwa, leavetypewa, companynamewa)
                                                    VALUES (%s, %s, %s)
                                                """, (id_user, button_id_leave_type, company_reg))

                                                connection.commit()

                                                send_whatsapp_message(
                                                    sender_id, 
                                                    f"Ok. When would you like your {selected_option} Leave to start {first_name}?\n\n"
                                                    "Please enter your response using the format: 👇🏻\n"
                                                    "`start 24 january 2025`"
                                                )

                                                continue

                                            elif selected_option == "Apply":

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"

                                                query = f"SELECT id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor  FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor"])    

                                                if len(df_employeesappspendingcheck) == 0:

                                                        sections = [
                                                            {
                                                                "title": "Leave Type Options",
                                                                "rows": [
                                                                    {"id": "Annual", "title": "Annual Leave"},
                                                                    {"id": "Sick", "title": "Sick Leave"},
                                                                    {"id": "Study", "title": "Study Leave"},
                                                                    {"id": "Bereavement", "title": "Bereavement Leave"},
                                                                    {"id": "Parental", "title": "Parental Leave"},
                                                                    {"id": "Other", "title": "Other"},
                                                                ]
                                                            }
                                                        ]

                                                        send_whatsapp_list_message(
                                                            sender_id, 
                                                            f"{first_name}, kindly select the type of Leave that you are applying for.", 
                                                            "Leave Type Options",
                                                            sections) 

                                                elif len(df_employeesappspendingcheck) > 0:
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Reminder", "title": "Remind Approver"}},
                                                        {"type": "reply", "reply": {"id": "Cancelapp", "title": "Cancel Pending App"}},
                                                    ]
                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"Oops! 🥲. Sorry {first_name}, you cannot apply for leave whilst you have another leave application which is still pending approval.\n\n" 
                                                        f"Your `{df_employeesappspendingcheck.iat[0,1]}` Leave Application `[ID - {df_employeesappspendingcheck.iat[0,0]}]` applied on `{df_employeesappspendingcheck.iat[0,3].strftime('%d %B %Y')}` for `{df_employeesappspendingcheck.iat[0,6]} days from {df_employeesappspendingcheck.iat[0,4].strftime('%d %B %Y')} to {df_employeesappspendingcheck.iat[0,5].strftime('%d %B %Y')}` is still pending approval from {df_employeesappspendingcheck.iat[0,2]}.\n\n" 
                                                        f"Select an option below to either remind the approver to approved your pending application or you can cancel the pending application to submit a new leave application."         
                                                        , 
                                                        buttons
                                                    )

                                            elif selected_option == "Pending":

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"

                                                query = f"SELECT id, leavetype, firstname, surname, leaveapprovername, leaveapproverid, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, appid  FROM {table_name_apps_pending_approval} WHERE leaveapproverid = {str(id_user)};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id", "leavetype", "firstname", "surname", "leaveapprovername", "leaveapproverid", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor", "appid"])    
                                                df_employeesappspendingcheck = df_employeesappspendingcheck.sort_values(by=df_employeesappspendingcheck.columns[10], ascending=False)

                                                if len(df_employeesappspendingcheck) == 0:

                                                    companyxx = company_reg.replace("_", " ").title()
                                                    sections = [
                                                        {
                                                            "title": "User Options",
                                                            "rows": [
                                                                {"id": "Apply", "title": "Apply for Leave"},
                                                                {"id": "Track", "title": "Track My Application"},
                                                                {"id": "Checkbal", "title": "Check Days Balance"},
                                                                {"id": "Pending", "title": "Apps Pending My Approval"}
                                                            ]
                                                        }
                                                    ]
    
                                                    send_whatsapp_list_message(
                                                        sender_id, 
                                                        f"{first_name}, there are currently no leave applications that are pending your approval.", 
                                                    "Approver Options",
                                                    sections) 

                                                elif len(df_employeesappspendingcheck) > 0:

                                                    firstnameemp2 = df_employeesappspendingcheck.iat[0,2]
                                                    appid = df_employeesappspendingcheck.iat[0,10]
                                                    surnameemp2 = df_employeesappspendingcheck.iat[0,3]
                                                    leave_type2 = df_employeesappspendingcheck.iat[0,1]
                                                    days = df_employeesappspendingcheck.iat[0,9]
                                                    date_applied2 = df_employeesappspendingcheck.iat[0,6]
                                                    start_date2 = df_employeesappspendingcheck.iat[0,7]
                                                    end_date2 = df_employeesappspendingcheck.iat[0,8]

                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": f"Approve5appwa_{appid}", "title": "Approve"}},
                                                        {"type": "reply", "reply": {"id": f"Disapproveappwa_{appid}", "title": "Disapprove"}},
                                                    ]

                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"Hey {first_name}, {firstnameemp2} {surnameemp2}'s {days} day {leave_type2} Leave Application, applied on {date_applied2.strftime('%d %B %Y')} and running from {start_date2.strftime('%d %B %Y')} to {end_date2.strftime('%d %B %Y')} is pending your Approval.\n\n" 
                                                        "Select an option below to either approve or disapprove this leave application.", 
                                                        buttons
                                                    )

                                        elif interactive.get("type") == "button_reply":

                                            button_id = interactive.get("button_reply", {}).get("id")
                                            print(f"🔘 Button clicked: {button_id}")

                                            if button_id == "Track":

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                table_name_apps_approved = f"{company_reg}appsapproved"
                                                table_name_apps_declined = f"{company_reg}appsdeclined"
                                                table_name_apps_cancelled = f"{company_reg}appscancelled"


                                                query = f"SELECT id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leaveapproverwhatsapp, appid  FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor", "leaveapproverwhatsapp","appid"])    

                                                if len(df_employeesappspendingcheck) == 0:

                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf  FROM {table_name_apps_approved} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf"]) 

                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf  FROM {table_name_apps_declined} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappsdeclinedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf"])  
                            
                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf  FROM {table_name_apps_cancelled} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappscancelledcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf"])
                            
                                                    all_approved_declined = df_employeesappsapprovedcheck._append(df_employeesappsdeclinedcheck)
                                                    all_approved_declined_cancelled = all_approved_declined._append(df_employeesappscancelledcheck)
                                                    all_approved_declined_cancelled = all_approved_declined_cancelled.sort_values(by="appid", ascending=False) 

                                                    if len(all_approved_declined_cancelled) > 0:


                                                        print(f" hhhhhhhhhhhhhhhhhhhh  {all_approved_declined_cancelled.iat[0,8] }")

                                                        if all_approved_declined_cancelled.iat[0,8] == "Approved":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Revoke", "title": "Revoke Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, your recent `{all_approved_declined_cancelled.iat[0,2]}` Leave Application `[ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}` was {all_approved_declined_cancelled.iat[0,8]}✅ by `{all_approved_declined_cancelled.iat[0,3].title()}` on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}`." 
                                                            )


                                                            def generate_leave_pdf():
                                                                app = {
                                                                    'company_logo': 44,
                                                                    'company_name': company_reg.replace("_"," ").title(),
                                                                    'employee_name': f"{first_name} {last_name}",
                                                                    'leave_type': all_approved_declined_cancelled.iat[0,2],
                                                                    'generated_on': today_date,
                                                                    'date_applied': all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y'),
                                                                    'approver_name': all_approved_declined_cancelled.iat[0,3].title(),
                                                                    'reference_number': all_approved_declined_cancelled.iat[0,0],
                                                                    'approved_date': all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y'),
                                                                    'new_balance': all_approved_declined_cancelled.iat[0,10],
                                                                    'start_date':  all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y'),
                                                                    'end_date':  all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y'),
                                                                    'days_requested':  all_approved_declined_cancelled.iat[0,7], 
                                                                    'address': address_foc_8, 
                                                                    'whatsapp': whatsapp_foc_8, 
                                                                    'email': email_foc_8, 
                                                                    'status': 'Approved'
                                                                }

                                                                html_out = render_template("leave_pdf_template.html", app=app)
                                                                
                                                                # ✅ Return as bytes instead of saving to file
                                                                pdf_bytes = HTML(string=html_out).write_pdf()
                                                                return pdf_bytes

                                                            
                                                            global ACCESS_TOKEN
                                                            global PHONE_NUMBER_ID

                                                            def upload_pdf_to_whatsapp(pdf_bytes):
                                                                compxxy = company_reg.replace("_"," ").title()
                                                                filename=f"leave_application_{all_approved_declined_cancelled.iat[0,0]}_{first_name}_{last_name}_{compxxy}.pdf"
                                                            
                                                                url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/media"
                                                                headers = {
                                                                    "Authorization": f"Bearer {ACCESS_TOKEN}"
                                                                }

                                                                files = {
                                                                    "file": (filename, io.BytesIO(pdf_bytes), "application/pdf"),
                                                                    "type": (None, "application/pdf"),
                                                                    "messaging_product": (None, "whatsapp")
                                                                }

                                                                response = requests.post(url, headers=headers, files=files)
                                                                print("📥 Full incoming data:", response.text)  # Good for debugging
                                                                response.raise_for_status()
                                                                return response.json()["id"]

                                                                                                            
                                                            def send_whatsapp_pdf_by_media_id(recipient_number, media_id):
                                                                compxxy = company_reg.replace("_"," ").title()
                                                                filename=f"leave_application_{all_approved_declined_cancelled.iat[0,0]}_{first_name}_{last_name}_{compxxy}.pdf"
                                                                url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
                                                                headers = {
                                                                    "Authorization": f"Bearer {ACCESS_TOKEN}",
                                                                    "Content-Type": "application/json"
                                                                }
                                                                payload = {
                                                                    "messaging_product": "whatsapp",
                                                                    "to": recipient_number,
                                                                    "type": "document",
                                                                    "document": {
                                                                        "id": media_id,            # Media ID from upload step
                                                                        "filename": filename       # Desired file name on recipient's phone
                                                                    }
                                                                }

                                                                response = requests.post(url, headers=headers, json=payload)
                                                                response.raise_for_status()
                                                                return response.json()


                                                            pdf_path = generate_leave_pdf()
                                                            media_id = upload_pdf_to_whatsapp(pdf_path)
                                                            send_whatsapp_pdf_by_media_id(sender_id, media_id)

                                                            send_whatsapp_message(
                                                                sender_id,
                                                                "Select an option below to continue 👇",
                                                                buttons
                                                            )

                                                        elif all_approved_declined_cancelled.iat[0,8] == "Declined":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, your recent `{all_approved_declined_cancelled.iat[0,2]}` Leave Application `[ID - {all_approved_declined_cancelled.iat[0,9]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}` was {all_approved_declined_cancelled.iat[0,8]}❌ by `{all_approved_declined_cancelled.iat[0,3].title()}` on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}`.",
                                                                buttons 
                                                            )

                                                        elif all_approved_declined_cancelled.iat[0,8] == "Cancelled":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}` you Cancelled ⛔ your recent `{all_approved_declined_cancelled.iat[0,2]} Leave Application [ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}`.",
                                                                buttons 
                                                            )

                                                    else:


                                                        buttons = [
                                                            {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                            {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            {"type": "reply", "reply": {"id": "Pending", "title": "Pending My Approval"}},
                                                        ]
                                                        companyxx = company_reg.replace("_"," ").title()

                                                        send_whatsapp_message(
                                                            sender_id, 
                                                            f"Hello {first_name} {last_name}, LMS Leave Applications Approver from {companyxx}!\n\n You have not applied for any leave days yet.", 
                                                            buttons 
                                                        )

                                                elif len(df_employeesappspendingcheck) > 0:
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Reminder", "title": "Remind Approver"}},
                                                        {"type": "reply", "reply": {"id": "Cancelapp", "title": "Cancel Pending App"}},
                                                    ]
                                                    approoooover = df_employeesappspendingcheck.iat[0,2].title()
                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"Hey {first_name}, your recent `{df_employeesappspendingcheck.iat[0,1]}` Leave Application `[ID - {df_employeesappspendingcheck.iat[0,8]}]` applied on `{df_employeesappspendingcheck.iat[0,3].strftime('%d %B %Y')}` for `{df_employeesappspendingcheck.iat[0,6]} days from {df_employeesappspendingcheck.iat[0,4].strftime('%d %B %Y')} to {df_employeesappspendingcheck.iat[0,5].strftime('%d %B %Y')}` is still pending approval from `{approoooover}`.\n\n" 
                                                        f"Select an option below to either remind `{approoooover}` to approve your pending leave application or you can cancel the pending application to submit a new leave application."         
                                                        , 
                                                        buttons
                                                    )

                                            elif button_id == "Submitapp":
                                    
                                                try:

                                                    table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                    table_name_apps_approved = f"{company_reg}appsapproved"

                                                    query = f"SELECT id FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()

                                                    df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id"])    

                                                    if len(df_employeesappspendingcheck) == 0:

                                                        cursor.execute("""
                                                            SELECT id ,empidwa, leavetypewa, startdate, enddate FROM whatsapptempapplication
                                                            WHERE empidwa = %s
                                                        """, (str(id_user)))
                                                
                                                        result = cursor.fetchone()

                                                        appid = result[0]
                                                        leavetype = result[2]
                                                        startdate = result[3]
                                                        enddate = result[4]
                                                        table_name = f"{company_reg}main"

                                                        if isinstance(startdate, str):
                                                            startdate = datetime.datetime.strptime(startdate, "%Y-%m-%d").date()
                                                        if isinstance(enddate, str):
                                                            enddate = datetime.datetime.strptime(enddate, "%Y-%m-%d").date()

                                                        business_days = 0
                                                        current_date = startdate

                                                        while current_date <= enddate:
                                                            if current_date.weekday() < 5:  # 0=Mon, 1=Tue, ..., 4=Fri
                                                                business_days += 1
                                                            current_date += timedelta(days=1)  # Use timedelta directly

                                                        query = f"SELECT id, firstname, surname, whatsapp, email, address, role, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, monthlyaccumulation, department FROM {table_name};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()

                                                        df_employees = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Address", "Role","Leave Approver Name","Leave Approver ID","Leave Approver Email", "Leave Approver WhatsAapp", "Leave Days Balance","Days Accumulated per Month", "Department"])
                                                        print(df_employees)
                                                        userdf = df_employees[df_employees['id'] == int(np.int64(id_user))].reset_index()
                                                        print("yeaarrrrr")
                                                        print(userdf)
                                                        firstname = userdf.iat[0,2]
                                                        surname = userdf.iat[0,3]
                                                        whatsapp = userdf.iat[0,4]
                                                        address = userdf.iat[0,6]
                                                        email = userdf.iat[0,5]
                                                        fullnamedisp = firstname + ' ' + surname
                                                        leaveapprovername = userdf.iat[0,8]
                                                        leaveapproverid = userdf.iat[0,9]
                                                        leaveapproveremail = userdf.iat[0, 10]
                                                        leaveapproverwhatsapp = userdf.iat[0,11]
                                                        role = userdf.iat[0,7]
                                                        leavedaysbalance = userdf.iat[0,12]
                                                        department = userdf.iat[0,14]
                                                        print('check')

                                                        departmentdf = df_employees[df_employees['Department'] == department].reset_index()
                                                        numberindepartment = len(departmentdf)

                                                        startdatex = pd.Timestamp(startdate)
                                                        enddatex = pd.Timestamp(enddate)

                                                        leave_dates = pd.date_range(startdatex, enddatex)

                                                        query = f"""
                                                            SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate,
                                                                leaveenddate, leavedaysappliedfor, approvalstatus, statusdate,
                                                                leavedaysbalancebf, department
                                                            FROM {table_name_apps_approved}
                                                            WHERE department = %s;
                                                        """
                                                        cursor.execute(query, (department,))
                                                        rows = cursor.fetchall()

                                                        df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf","department"]) 

                                                        # Create daily impact report
                                                        impact_report = []

                                                        for date in leave_dates:

                                                            date = pd.Timestamp(date)

                                                            df_employeesappsapprovedcheck["leavestartdate"] = pd.to_datetime(df_employeesappsapprovedcheck["leavestartdate"])
                                                            df_employeesappsapprovedcheck["leaveenddate"] = pd.to_datetime(df_employeesappsapprovedcheck["leaveenddate"])

                                                            print(type(date))  # Should be pandas._libs.tslibs.timestamps.Timestamp or datetime.datetime
                                                            print(df_employeesappsapprovedcheck.dtypes)  # Check all datetime columns

                                                            on_leave = ((df_employeesappsapprovedcheck["leavestartdate"] <= date) & (df_employeesappsapprovedcheck["leaveenddate"] >= date)).sum()
                                                            remaining = numberindepartment - on_leave - 1  # subtract 1 for the new leave
                                                            impact_report.append({
                                                                "date": date.strftime("%Y-%m-%d"),
                                                                "on leave (including new)": on_leave + 1,
                                                                "employees remaining": remaining
                                                            })

                                                        # Convert to DataFrame for display
                                                        impact_df = pd.DataFrame(impact_report)
                                                        print("IMPAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACT")
                                                        print(impact_df)
                                                        print(numberindepartment)

                                                        impact_df["date"] = pd.to_datetime(impact_df["date"], dayfirst=True)
                                                        impact_df = impact_df[impact_df["date"].dt.weekday != 6].copy()

                                                        impact_df["group"] = (impact_df[["on leave", "employees remaining"]] != impact_df[["on leave", "employees remaining"]].shift()).any(axis=1).cumsum()

                                                        statements = []
                                                        for _, group_df in impact_df.groupby("group"):
                                                            start = group_df["date"].iloc[0].strftime("%d %B %Y")
                                                            end = group_df["date"].iloc[-1].strftime("%d %B %Y")
                                                            on_leave = group_df["on leave"].iloc[0]
                                                            remaining = group_df["employees remaining"].iloc[0]
                                                            
                                                            if start == end:
                                                                statements.append(f"On {start}, the {department} department will have {remaining} employee(s) remaining at work and {on_leave} employee(s) on leave.")
                                                            else:
                                                                statements.append(f"From {start} to {end}, the {department} department will have {remaining} employee(s) remaining at work and {on_leave} employee(s) on leave.")

                                                        # Combine all statements into a single variable
                                                        final_summary = "\n".join(statements)
                                                        # Print output
                                                        for s in statements:
                                                            print(s)

                                                        leavedaysbalancebf = int(leavedaysbalance) - int(business_days)

                                                        status = "Pending"

                                                        insert_query = f"""
                                                        INSERT INTO {table_name_apps_pending_approval} (id, firstname, surname, department, leavetype, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus)
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                        """
                                                        cursor.execute(insert_query, (int(np.int64(id_user)), first_name, last_name, department, leavetype, leaveapprovername, int(np.int64(leaveapproverid)), leaveapproveremail, int(np.int64(leaveapproverwhatsapp)), int(np.int64(leavedaysbalance)), today_date, startdate, enddate, int(np.int64(business_days)), int(np.int64(leavedaysbalancebf)), status))
                                                        connection.commit()

                                                        query = f"SELECT appid, id FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)} ;"
                                                        cursor.execute(query, )
                                                        rows = cursor.fetchall()

                                                        df_employees = pd.DataFrame(rows, columns=["appid","id"])
                                                        leaveappid = df_employees.iat[0,0]
                                                        companyxx = company_reg.replace("_"," ").title()
                                                        approovvver = leaveapprovername.title()

                                                        send_whatsapp_message(sender_id, f"✅ Great News {first_name} from {companyxx}! \n\n Your `{leavetype} Leave Application` for `{business_days} days` from `{startdate.strftime('%d %B %Y')}` to `{enddate.strftime('%d %B %Y')}` has been submitted successfully!\n\n"
                                                            f"Your Leave Application ID is `{leaveappid}`.\n\n"
                                                            f"A Notification has been sent to `{approovvver}`  on `+263{leaveapproverwhatsapp}` to decide on  your application.\n\n"
                                                            "To Check the approval status of your leave application, type `Hello` then select `Track Application`.")
                                                        
                                                        if leaveapproverwhatsapp:
            
                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": f"Approve5appwa_{leaveappid}", "title": "Approve"}},
                                                                {"type": "reply", "reply": {"id": f"Disapproveappwa_{leaveappid}", "title": "Disapprove"}},
                                                            ]
                                                            
                                                            send_whatsapp_message(
                                                                f"263{leaveapproverwhatsapp}", 
                                                                f"Hey {approovvver}! 😊. New `{leavetype}` Leave Application from `{first_name} {surname}` for `{business_days} days` from `{startdate.strftime('%d %B %Y')}` to `{enddate.strftime('%d %B %Y')}`.\n\n" 
                                                                f"If you approve this leave application, {final_summary}\n\n"  
                                                                f"Select an option below to either approve or disapprove the application."         
                                                                , 
                                                                buttons
                                                            )

                                                    else:
                                                        print("leave app submission failed")

                                                except ValueError as e:
                                                    send_whatsapp_message(
                                                        sender_id,
                                                        f"{e}, ❌ No, incorrect message format. Please use:\n"
                                                        "`end 24 january 2025`\n"
                                                        "Example: `end 15 march 2024`"
                                                    )

                                            elif button_id == "Checkbal":

                                                buttons = [
                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                {"type": "reply", "reply": {"id": "Track", "title": "Track Application"}},
                                                ]

                                                send_whatsapp_message(
                                                    sender_id, 
                                                    f"Hey {first_name}, your current available leave days balance is `{days_days_balance} days`.\n\n"
                                                    "Select an option below to continue 👇",
                                                    buttons
                                                )

                                            elif button_id == "Resubmitapp" :
                                                table_name_apps_cancelled = f"{company_reg}appscancelled"
                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"

                                                query = f"SELECT appid, id, firstname, surname, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail , leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf FROM {table_name_apps_pending_approval} WHERE id = %s;"
                                                cursor.execute(query, (id_user,))
                                                result = cursor.fetchone()
                                                if result:
                                                    (app_id, employee_number, first_name, surname, leave_type,  leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf) = result
                                                
                                                    try:
                                                            status = "Cancelled"
                                                            statusdate = today_date
                                                        
                                                            insert_query = f"""
                                                            INSERT INTO {table_name_apps_cancelled} 
                                                            (appid, id, firstname, surname, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                            """

                                                            cursor.execute(insert_query, (
                                                                app_id, employee_number, first_name, surname, leave_type, leave_specify, approver_name, 
                                                                approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                                                end_date, leave_days, leavedaysbalancebf, status, statusdate
                                                            ))
                                                            
                                                            connection.commit()
                                                            print("Insert successful!")

                                                    except Exception as e:
                                                        print("Error inserting data:", e)

                                                    # SQL query to delete or mark the leave as canceled
                                                    query = f"""DELETE FROM {table_name_apps_pending_approval} WHERE appid = %s"""
                                                    cursor.execute(query, (app_id,))
                                                    connection.commit()                                       

                                                    companyxx = company_reg.replace("_", " ").title()
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                        {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                        {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                    ]

                                                    send_whatsapp_message(sender_id, f"Hey {first_name} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}` has been Cancelled successfully✅!\n\n"
                                                        "Select an option below to continue 👇",
                                                        buttons
                                                    )                                          
                                                
                                                else:
                                                    print("No record found for the user.")


                                            elif button_id == "Cancelapp" :

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                table_name_apps_cancelled = f"{company_reg}appscancelled"

                                                query = f"SELECT appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail , leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf FROM {table_name_apps_pending_approval} WHERE id = %s;"
                                                cursor.execute(query, (id_user,))
                                                result = cursor.fetchone()
                                                if result:
                                                    (app_id, employee_number, first_name, surname, department, leave_type,  leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf) = result
                                                
                                                    try:
                                                            status = "Cancelled"
                                                            statusdate = today_date
                                                        
                                                            insert_query = f"""
                                                            INSERT INTO {table_name_apps_cancelled} 
                                                            (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                            """

                                                            cursor.execute(insert_query, (
                                                                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                                                                approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                                                end_date, leave_days, leavedaysbalancebf, status, statusdate
                                                            ))
                                                            
                                                            connection.commit()
                                                            print("Insert successful!")

                                                    except Exception as e:
                                                        print("Error inserting data:", e)

                                                    # SQL query to delete or mark the leave as canceled
                                                    query = f"""DELETE FROM {table_name_apps_pending_approval} WHERE appid = %s"""
                                                    cursor.execute(query, (app_id,))
                                                    connection.commit()                                       

                                                    companyxx = company_reg.replace("_", " ").title()
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                        {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                        {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                    ]

                                                    send_whatsapp_message(sender_id, f"Hey {first_name} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}` has been Cancelled successfully✅!\n\n"
                                                        "Select an option below to continue 👇",
                                                        buttons
                                                    )                                          
                                                
                                                else:
                                                    print("No record found for the user.")



                                            elif "appwa" in button_id.lower():

                                                app_id = button_id.split("_")[1]
                                                print(app_id)

                                                if "approve5" in button_id.lower():

                                                    try:
                                                       
                                                        print ("eissssssssshhhhhhhhhhhhhhhhhhhhhhhhhhhh")

                                                        table_name = company_reg + 'main'
                                                        company_name = company_reg.replace("_", " ").title()
                                                        table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                        table_name_apps_approved = f"{company_reg}appsapproved"

                                                        if not app_id:
                                                            print("none on appid")

                                                            return jsonify({"message": "Application ID is missing."}), 400

                                                        status = "Approved"
                                                        statusdate = today_date
                                                        print("bababababababababa")
                                                        print(table_name_apps_pending_approval)

                                                        query = f"SELECT * FROM {table_name_apps_pending_approval} WHERE appid = %s;"
                                                        cursor.execute(query, (app_id,))
                                                        result = cursor.fetchone()
                                                        app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf, statuspre = result
                                                        print("chiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                                                        print(employee_number)
                                                        print(approver_name)

                                                        try:
                                                            insert_query = f"""
                                                            INSERT INTO {table_name_apps_approved} 
                                                            (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                            """
                                                            
                                                            cursor.execute(insert_query, (
                                                                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                                                                approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                                                end_date, leave_days, leavedaysbalancebf, status, statusdate
                                                            ))
                                                            
                                                            connection.commit()
                                                            print("Insert successful!")

                                                            query = f"UPDATE {table_name} SET currentleavedaysbalance = %s WHERE id = %s;"
                                                            cursor.execute(query, (leavedaysbalancebf, employee_number))
                                                            connection.commit()

                                                        except Exception as e:
                                                            print("Error inserting data:", e)

                                                        query = f"""DELETE FROM {table_name_apps_pending_approval} WHERE appid = %s"""
                                                        cursor.execute(query, (app_id,))
                                                        connection.commit()

                                                        query = f"SELECT id, firstname, surname, whatsapp, email, address, role, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, monthlyaccumulation, department FROM {table_name};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()

                                                        df_employees = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Address", "Role","Leave Approver Name","Leave Approver ID","Leave Approver Email", "Leave Approver WhatsAapp", "Leave Days Balance","Days Accumulated per Month", "Department"])
                                                        print(df_employees)
                                                        userdf = df_employees[df_employees['id'] == int(np.int64(employee_number))].reset_index()
                                                        print("yeaarrrrr")
                                                        print(userdf)
                                                        firstname = userdf.iat[0,2].title()
                                                        surname = userdf.iat[0,3].title()
                                                        whatsappemp = userdf.iat[0,4]
                                                        email = userdf.iat[0,5]
                                                        address = userdf.iat[0,6]
                                                        companyxx = company_name.replace("_", " ").title()
                                                        app_namexx = approver_name.title()

                                                        query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf  FROM {table_name_apps_approved} WHERE id = {str(employee_number)};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()
                                                        df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf"]) 

                                                        df_employeesappsapprovedcheck = df_employeesappsapprovedcheck.sort_values(by="appid", ascending=False)  

                                                        def generate_leave_pdf():
                                                            app = {
                                                                'company_logo': 44,
                                                                'company_name': companyxx,
                                                                'employee_name': f"{first_name} {surname}",
                                                                'leave_type': leave_type,
                                                                'generated_on': today_date,
                                                                'department': department,
                                                                'date_applied': df_employeesappsapprovedcheck.iat[0,4].strftime('%d %B %Y'),
                                                                'approver_name': df_employeesappsapprovedcheck.iat[0,3].title(),
                                                                'reference_number': df_employeesappsapprovedcheck.iat[0,0],
                                                                'approved_date': df_employeesappsapprovedcheck.iat[0,9].strftime('%d %B %Y'),
                                                                'new_balance': df_employeesappsapprovedcheck.iat[0,10],
                                                                'start_date':  df_employeesappsapprovedcheck.iat[0,5].strftime('%d %B %Y'),
                                                                'end_date':  df_employeesappsapprovedcheck.iat[0,6].strftime('%d %B %Y'),
                                                                'days_requested':  df_employeesappsapprovedcheck.iat[0,7], 
                                                                'address': address, 
                                                                'whatsapp': f"+263{whatsappemp}", 
                                                                'email': email, 
                                                                'status': 'Approved'
                                                            }

                                                            html_out = render_template("leave_pdf_template.html", app=app)
                                                            
                                                            # ✅ Return as bytes instead of saving to file
                                                            pdf_bytes = HTML(string=html_out).write_pdf()
                                                            return pdf_bytes

                                                        
                                                        global ACCESS_TOKEN
                                                        global PHONE_NUMBER_ID

                                                        def upload_pdf_to_whatsapp(pdf_bytes):
                                                            filename=f"leave_application_{df_employeesappsapprovedcheck.iat[0,0]}_{first_name}_{surname}_{companyxx}.pdf"
                                                        
                                                            url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/media"
                                                            headers = {
                                                                "Authorization": f"Bearer {ACCESS_TOKEN}"
                                                            }

                                                            files = {
                                                                "file": (filename, io.BytesIO(pdf_bytes), "application/pdf"),
                                                                "type": (None, "application/pdf"),
                                                                "messaging_product": (None, "whatsapp")
                                                            }

                                                            response = requests.post(url, headers=headers, files=files)
                                                            print("📥 Full incoming data:", response.text)  # Good for debugging
                                                            response.raise_for_status()
                                                            return response.json()["id"]

                                                                                                        
                                                        def send_whatsapp_pdf_by_media_id(recipient_number, media_id):
                                                            filename=f"leave_application_{df_employeesappsapprovedcheck.iat[0,0]}_{first_name}_{surname}_{companyxx}.pdf"
                                                            url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
                                                            headers = {
                                                                "Authorization": f"Bearer {ACCESS_TOKEN}",
                                                                "Content-Type": "application/json"
                                                            }
                                                            payload = {
                                                                "messaging_product": "whatsapp",
                                                                "to": recipient_number,
                                                                "type": "document",
                                                                "document": {
                                                                    "id": media_id,            # Media ID from upload step
                                                                    "filename": filename       # Desired file name on recipient's phone
                                                                }
                                                            }

                                                            response = requests.post(url, headers=headers, json=payload)
                                                            response.raise_for_status()
                                                            return response.json()


                                                        pdf_path = generate_leave_pdf()
                                                        media_id = upload_pdf_to_whatsapp(pdf_path)

                                                        buttonsapproval = [
                                                            {"type": "reply", "reply": {"id": "Revoke", "title": "Revoke Approval"}},
                                                            {"type": "reply", "reply": {"id": "Pending", "title": "Pending My Approval"}},
                                                        ]

                                                        send_whatsapp_message(sender_id, f"✅ Great News {approver_name} from {companyxx}! \n\n You have successfully approved `{first_name} {surname}`'s  `{leave_days} day` `{leave_type} Leave Application` running from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}`✅!")
                                                        send_whatsapp_pdf_by_media_id(sender_id, media_id)
                                                        send_whatsapp_message(
                                                            sender_id,
                                                            "Select an option below to continue 👇y, or Type `Hello` to view all Approver options",
                                                            buttonsapproval
                                                        )

                                                        if whatsappemp:

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Revoke", "title": "Revoke Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]

                                                            send_whatsapp_message(f"263{whatsappemp}", f"✅ Great News {first_name} {surname} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}`, has been Approved ✅ by `{app_namexx}`!")
                                                            send_whatsapp_pdf_by_media_id(f"263{whatsappemp}", media_id)
                                                            send_whatsapp_message(
                                                                f"263{whatsappemp}",
                                                                "Select an option below to continue 👇",
                                                                buttons
                                                            )


                                                    except Exception as e:
                                                        print(e)
                                                        return jsonify({"message": "Error approving leave application.", "error": str(e)}), 500


                                                else:

                                                    print("disapproved")

                                                    try:
                                                       
                                                        print ("eissssssssshhhhhhhhhhhhhhhhhhhhhhhhhhhh")

                                                        table_name = company_reg + 'main'
                                                        company_name = company_reg.replace("_", " ").title()
                                                        table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                        table_name_apps_approved = f"{company_reg}appsapproved"
                                                        table_name_apps_declined = f"{company_reg}appsdeclined"


                                                        if not app_id:
                                                            print("none on appid")

                                                            return jsonify({"message": "Application ID is missing."}), 400

                                                        status = "Disapproved"
                                                        statusdate = today_date
                                                        print("bababababababababa")
                                                        print(table_name_apps_pending_approval)

                                                        query = f"SELECT * FROM {table_name_apps_pending_approval} WHERE appid = %s;"
                                                        cursor.execute(query, (app_id,))
                                                        result = cursor.fetchone()
                                                        app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf, statuspre = result
                                                        print("chiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                                                        print(employee_number)
                                                        print(approver_name)

                                                        try:
                                                            insert_query = f"""
                                                            INSERT INTO {table_name_apps_declined} 
                                                            (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                            """
                                                            
                                                            cursor.execute(insert_query, (
                                                                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                                                                approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                                                end_date, leave_days, leavedaysbalancebf, status, statusdate
                                                            ))
                                                            
                                                            connection.commit()
                                                            print("Insert successful!")

                                                        except Exception as e:
                                                            print("Error inserting data:", e)

                                                        query = f"""DELETE FROM {table_name_apps_pending_approval} WHERE appid = %s"""
                                                        cursor.execute(query, (app_id,))
                                                        connection.commit()

                                                        query = f"SELECT id, firstname, surname, whatsapp, email, address, role, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, monthlyaccumulation, department FROM {table_name};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()

                                                        df_employees = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Address", "Role","Leave Approver Name","Leave Approver ID","Leave Approver Email", "Leave Approver WhatsAapp", "Leave Days Balance","Days Accumulated per Month", "Department"])
                                                        print(df_employees)
                                                        userdf = df_employees[df_employees['id'] == int(np.int64(employee_number))].reset_index()
                                                        print("yeaarrrrr")
                                                        print(userdf)
                                                        firstname = userdf.iat[0,2].title()
                                                        surname = userdf.iat[0,3].title()
                                                        whatsappemp = userdf.iat[0,4]
                                                        email = userdf.iat[0,5]
                                                        address = userdf.iat[0,6]
                                                        companyxx = company_name.replace("_", " ").title()
                                                        app_namexx = approver_name.title()

                                                        query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf  FROM {table_name_apps_approved} WHERE id = {str(employee_number)};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()
                                                        df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf"]) 

                                                        df_employeesappsapprovedcheck["dateapplied"] = pd.to_datetime(df_employeesappsapprovedcheck["dateapplied"], errors='coerce')
                                                        df_employeesappsapprovedcheck = df_employeesappsapprovedcheck.sort_values(by="dateapplied", ascending=False)
                                                        
                                                        global ACCESS_TOKEN
                                                        global PHONE_NUMBER_ID

                                                        buttonsapproval = [
                                                            {"type": "reply", "reply": {"id": "Revokedis", "title": "Revoke Disapproval"}},
                                                            {"type": "reply", "reply": {"id": "Pending", "title": "Pending My Approval"}},
                                                        ]

                                                        send_whatsapp_message(sender_id, f"✅ Hey {approver_name} from {companyxx}! \n\n You have successfully disapproved `{first_name} {surname}`'s  `{leave_days} day` `{leave_type} Leave Application` running from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}`✅!")
                                                        send_whatsapp_message(
                                                            sender_id,
                                                            "Select an option below to continue 👇y, or Type `Hello` to view all Approver options",
                                                            buttonsapproval
                                                        )

                                                        if whatsappemp:

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Reapply", "title": "Resubmit Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]

                                                            send_whatsapp_message(f"263{whatsappemp}", f"✅ Oops, {first_name} {surname} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}`, has been disapproved ❌ by `{app_namexx}`!")
                                                            send_whatsapp_message(
                                                                f"263{whatsappemp}",
                                                                "Select an option below to continue 👇",
                                                                buttons
                                                            )


                                                    except Exception as e:
                                                        print(e)
                                                        return jsonify({"message": "Error approving leave application.", "error": str(e)}), 500


                                    else:

                                        text = message.get("text", {}).get("body", "").lower()
                                        print(f"📨 Message from {sender_id}: {text}")
                                        
                                        print("yearrrrrrrrrrrrrrrrrrrrrrrrrrrssrsrsrsrsrs")

                                        print(role_foc_8)
                                        companyxx = company_reg.replace("_", " ").title()

                                        if "hello" in text.lower():

                                            sections = [
                                                {
                                                    "title": "Approver Options",
                                                    "rows": [
                                                        {"id": "Apply", "title": "Apply for Leave"},
                                                        {"id": "Track", "title": "Track My Application"},
                                                        {"id": "Checkbal", "title": "Check Days Balance"},
                                                        {"id": "Pending", "title": "Apps Pending My Approval"}
                                                    ]
                                                }
                                            ]


                                            send_whatsapp_list_message(
                                                sender_id, 
                                                f"Hello {first_name} {last_name}, LMS Leave Applications Approver from {companyxx}!\n\n Echelon Bot Here 😎. How can I assist you?", 
                                            "User Options",
                                            sections)


                                        elif "start" in text.lower():

                                            date_part = text.split("start", 1)[1].strip()

                                            cursor.execute("""
                                                UPDATE whatsapptempapplication
                                                SET startdate = %s
                                                WHERE empidwa = %s
                                            """, (date_part, id_user))

                                            connection.commit()

                                            cursor.execute("""
                                                SELECT empidwa, leavetypewa FROM whatsapptempapplication
                                                WHERE empidwa = %s
                                            """, (str(id_user)))
                                    
                                            result = cursor.fetchone()

                                            if result:
                                                leavetypewa = result[1] 

                                            cursor.execute("SELECT * FROM whatsapptempapplication")
                                            columns = [desc[0] for desc in cursor.description]
                                            records = cursor.fetchall()
                                            
                                            df = pd.DataFrame(records, columns=columns)
                                            
                                            print("\n📊 whatsapptempapplication Table:")
                                            print(df)
                                            
                                            try:
                                                parsed_date = datetime.strptime(date_part, "%d %B %Y")
                                                send_whatsapp_message(sender_id, "✅ Yes! Valid start date format.\n\n"
                                                    f"Now Enter the last day that you will be on {leavetypewa} Leave.Use the format: 👇🏻\n"
                                                    "`end 24 january 2025`"                      
                                                                    )
                                                
                                            except ValueError:
                                                send_whatsapp_message(
                                                    sender_id,
                                                    f"❌ No, incorrect message format, {first_name}. Please use:\n"
                                                    "`start 24 january 2025`\n"
                                                    "Example: `start 15 march 2024`"
                                                )

                                        elif "end" in text.lower():

                                            date_part = text.split("end", 1)[1].strip()

                                            cursor.execute("""
                                                UPDATE whatsapptempapplication
                                                SET enddate = %s
                                                WHERE empidwa = %s
                                            """, (date_part, id_user))

                                            connection.commit()

                                            cursor.execute("""
                                                SELECT id ,empidwa, leavetypewa, startdate, enddate FROM whatsapptempapplication
                                                WHERE empidwa = %s
                                            """, (str(id_user)))
                                    
                                            result = cursor.fetchone()

                                            appid = result[0]
                                            leavetype = result[2]
                                            startdate = result[3]
                                            enddate = result[4]

                                            if isinstance(startdate, str):
                                                startdate = datetime.datetime.strptime(startdate, "%Y-%m-%d").date()
                                            if isinstance(enddate, str):
                                                enddate = datetime.datetime.strptime(enddate, "%Y-%m-%d").date()

                                            business_days = 0
                                            current_date = startdate

                                            while current_date <= enddate:
                                                if current_date.weekday() < 5:  # 0=Mon, 1=Tue, ..., 4=Fri
                                                    business_days += 1
                                                current_date += timedelta(days=1)  # Use timedelta directly

                                            print(f"📅 Business days between {startdate} and {enddate}: {business_days}")


                                            buttons = [
                                                {"type": "reply", "reply": {"id": "Submitapp", "title": "Yes, Submit"}},
                                                {"type": "reply", "reply": {"id": "Dontsubmit", "title": "No"}}
                                            ]
                                            send_whatsapp_message(
                                                sender_id, 
                                                f"Do you wish to submit your `{business_days} day {leavetype} Leave Application` leave starting from `{startdate.strftime('%d %B %Y')}` to `{enddate.strftime('%d %B %Y')}` {first_name} ?", 
                                                buttons
                                            )

                                        else:
                                            send_whatsapp_message(
                                                sender_id, 
                                                "Echelon Bot Here 😎. Say 'hello' to start!"
                                            )

                            elif role_foc_8 == "Administrator":

                                table_namexxxx = company_reg + "main"        

                                query = f"SELECT id FROM {table_namexxxx} WHERE leaveapproverid = {str(id_user)};"
                                cursor.execute(query)
                                rows = cursor.fetchall()

                                df_employeesempapp = pd.DataFrame(rows, columns=["id"])

                                if len(df_employeesempapp) == 0:

                                    if message.get("type") == "interactive":
                                        interactive = message.get("interactive", {})


                                        if interactive.get("type") == "button_reply":
                                            button_id = interactive.get("button_reply", {}).get("id")
                                            print(f"🔘 Button clicked: {button_id}")
                                            
                                            if button_id == "Track":

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                table_name_apps_approved = f"{company_reg}appsapproved"
                                                table_name_apps_declined = f"{company_reg}appsdeclined"
                                                table_name_apps_cancelled = f"{company_reg}appscancelled"


                                                query = f"SELECT id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leaveapproverwhatsapp  FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor", "leaveapproverwhatsapp"])    

                                                if len(df_employeesappspendingcheck) == 0:

                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf  FROM {table_name_apps_approved} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf"]) 

                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf  FROM {table_name_apps_declined} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappsdeclinedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf"])  
                            
                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf  FROM {table_name_apps_cancelled} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappscancelledcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf"])
                            
                                                    all_approved_declined = df_employeesappsapprovedcheck._append(df_employeesappsdeclinedcheck)
                                                    all_approved_declined_cancelled = all_approved_declined._append(df_employeesappscancelledcheck)
                                                    all_approved_declined_cancelled = all_approved_declined_cancelled.sort_values(by="appid", ascending=False)

                                                    if len(all_approved_declined_cancelled) > 0:
  
                                                        print(f" hhhhhhhhhhhhhhhhhhhh  {all_approved_declined_cancelled.iat[0,8] }")

                                                        if all_approved_declined_cancelled.iat[0,8] == "Approved":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Revoke", "title": "Revoke Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, your recent `{all_approved_declined_cancelled.iat[0,2]}` Leave Application `[ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}` was {all_approved_declined_cancelled.iat[0,8]}✅ by `{all_approved_declined_cancelled.iat[0,3].title()}` on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}`." 
                                                            )


                                                            def generate_leave_pdf():
                                                                app = {
                                                                    'company_logo': 44,
                                                                    'company_name': company_reg.replace("_"," ").title(),
                                                                    'employee_name': f"{first_name} {last_name}",
                                                                    'leave_type': all_approved_declined_cancelled.iat[0,2],
                                                                    'generated_on': today_date,
                                                                    'date_applied': all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y'),
                                                                    'approver_name': all_approved_declined_cancelled.iat[0,3].title(),
                                                                    'reference_number': all_approved_declined_cancelled.iat[0,0],
                                                                    'approved_date': all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y'),
                                                                    'new_balance': all_approved_declined_cancelled.iat[0,10],
                                                                    'start_date':  all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y'),
                                                                    'end_date':  all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y'),
                                                                    'days_requested':  all_approved_declined_cancelled.iat[0,7], 
                                                                    'address': address_foc_8, 
                                                                    'whatsapp': whatsapp_foc_8, 
                                                                    'email': email_foc_8, 
                                                                    'status': 'Approved'
                                                                }

                                                                html_out = render_template("leave_pdf_template.html", app=app)
                                                                
                                                                # ✅ Return as bytes instead of saving to file
                                                                pdf_bytes = HTML(string=html_out).write_pdf()
                                                                return pdf_bytes

                                                            
                                                            global ACCESS_TOKEN
                                                            global PHONE_NUMBER_ID

                                                            def upload_pdf_to_whatsapp(pdf_bytes):
                                                                compxxy = company_reg.replace("_"," ").title()
                                                                filename=f"leave_application_{all_approved_declined_cancelled.iat[0,0]}_{first_name}_{last_name}_{compxxy}.pdf"
                                                            
                                                                url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/media"
                                                                headers = {
                                                                    "Authorization": f"Bearer {ACCESS_TOKEN}"
                                                                }

                                                                files = {
                                                                    "file": (filename, io.BytesIO(pdf_bytes), "application/pdf"),
                                                                    "type": (None, "application/pdf"),
                                                                    "messaging_product": (None, "whatsapp")
                                                                }

                                                                response = requests.post(url, headers=headers, files=files)
                                                                print("📥 Full incoming data:", response.text)  # Good for debugging
                                                                response.raise_for_status()
                                                                return response.json()["id"]

                                                                                                            
                                                            def send_whatsapp_pdf_by_media_id(recipient_number, media_id):
                                                                compxxy = company_reg.replace("_"," ").title()
                                                                filename=f"leave_application_{all_approved_declined_cancelled.iat[0,0]}_{first_name}_{last_name}_{compxxy}.pdf"
                                                                url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
                                                                headers = {
                                                                    "Authorization": f"Bearer {ACCESS_TOKEN}",
                                                                    "Content-Type": "application/json"
                                                                }
                                                                payload = {
                                                                    "messaging_product": "whatsapp",
                                                                    "to": recipient_number,
                                                                    "type": "document",
                                                                    "document": {
                                                                        "id": media_id,            # Media ID from upload step
                                                                        "filename": filename       # Desired file name on recipient's phone
                                                                    }
                                                                }

                                                                response = requests.post(url, headers=headers, json=payload)
                                                                response.raise_for_status()
                                                                return response.json()


                                                            pdf_path = generate_leave_pdf()
                                                            media_id = upload_pdf_to_whatsapp(pdf_path)
                                                            send_whatsapp_pdf_by_media_id(sender_id, media_id)

                                                            send_whatsapp_message(
                                                                sender_id,
                                                                "Select an option below to continue 👇",
                                                                buttons
                                                            )

                                                        elif all_approved_declined_cancelled.iat[0,8] == "Declined":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, your recent `{all_approved_declined_cancelled.iat[0,2]}` Leave Application `[ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}` was {all_approved_declined_cancelled.iat[0,8]}❌ by `{all_approved_declined_cancelled.iat[0,3].title()}` on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}`.",
                                                                buttons 
                                                            )

                                                        elif all_approved_declined_cancelled.iat[0,8] == "Cancelled":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}` you Cancelled ⛔ your recent `{all_approved_declined_cancelled.iat[0,2]} Leave Application [ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}`.",
                                                                buttons 
                                                            )
                                                    
                                                    else:


                                                        sections = [
                                                            {
                                                                "title": "Administrator Options",
                                                                "rows": [
                                                                    {"id": "Apply", "title": "Apply for Leave"},
                                                                    {"id": "Checkbal", "title": "Check Days Balance"},
                                                                    {"id": "Template", "title": "Add Employees"},
                                                                    {"id": "Rolechange", "title": "Change Employee's Role"},
                                                                    {"id": "Book", "title": "Extract Leave Book"}
                                                                ]
                                                            }
                                                        ]
                                                        companyxx = company_reg.replace("_"," ").title()


                                                        send_whatsapp_list_message(
                                                            sender_id, 
                                                            f"Hello {first_name} {last_name}, LMS Leave Applications Approver from {companyxx}!\n\n You have not applied for any leave days yet.", 
                                                            "Administrator Options",
                                                            sections
                                                        )


                                                elif len(df_employeesappspendingcheck) > 0:
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Reminder", "title": "Remind Approver"}},
                                                        {"type": "reply", "reply": {"id": "Cancelapp", "title": "Cancel Pending App"}},
                                                    ]
                                                    approoooover = df_employeesappspendingcheck.iat[0,2].title()
                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"Hey {first_name}, your recent `{df_employeesappspendingcheck.iat[0,1]}` Leave Application `[ID - {df_employeesappspendingcheck.iat[0,0]}]` applied on `{df_employeesappspendingcheck.iat[0,3].strftime('%d %B %Y')}` for `{df_employeesappspendingcheck.iat[0,6]} days from {df_employeesappspendingcheck.iat[0,4].strftime('%d %B %Y')} to {df_employeesappspendingcheck.iat[0,5].strftime('%d %B %Y')}` is still pending approval from `{approoooover}`.\n\n" 
                                                        f"Select an option below to either remind `{approoooover}` to approve your pending leave application or you can cancel the pending application to submit a new leave application."         
                                                        , 
                                                        buttons
                                                    )

                                                else:

                                                    sections = [
                                                        {
                                                            "title": "Administrator Options",
                                                            "rows": [
                                                                {"id": "Apply", "title": "Apply for Leave"},
                                                                {"id": "Checkbal", "title": "Check Days Balance"},
                                                            ]
                                                        }
                                                    ]

                                                    send_whatsapp_list_message(
                                                        sender_id, 
                                                        f"Hello {first_name} {last_name}, LMS Leave Applications Approver from {companyxx}!\n\n You have not applied for any leave days yet.", 
                                                    "Administrator Options",
                                                    sections)










                                            elif button_id == "Submitapp":
                                    
                                                try:

                                                    table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                    table_name_apps_approved =  f"{company_reg}appsapproved"
                                                    
                                                    query = f"SELECT id FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()

                                                    df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id"])    

                                                    if len(df_employeesappspendingcheck) == 0:

                                                        cursor.execute("""
                                                            SELECT id ,empidwa, leavetypewa, startdate, enddate FROM whatsapptempapplication
                                                            WHERE empidwa = %s
                                                        """, (str(id_user)))
                                                
                                                        result = cursor.fetchone()

                                                        appid = result[0]
                                                        leavetype = result[2]
                                                        startdate = result[3]
                                                        enddate = result[4]
                                                        table_name = f"{company_reg}main"

                                                        if isinstance(startdate, str):
                                                            startdate = datetime.datetime.strptime(startdate, "%Y-%m-%d").date()
                                                        if isinstance(enddate, str):
                                                            enddate = datetime.datetime.strptime(enddate, "%Y-%m-%d").date()

                                                        business_days = 0
                                                        current_date = startdate

                                                        while current_date <= enddate:
                                                            if current_date.weekday() < 5:  # 0=Mon, 1=Tue, ..., 4=Fri
                                                                business_days += 1
                                                            current_date += timedelta(days=1)  # Use timedelta directly

                                                        query = f"SELECT id, firstname, surname, whatsapp, email, address, role, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, monthlyaccumulation, department FROM {table_name};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()

                                                        df_employees = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Address", "Role","Leave Approver Name","Leave Approver ID","Leave Approver Email", "Leave Approver WhatsAapp", "Leave Days Balance","Days Accumulated per Month", "Department"])
                                                        print(df_employees)
                                                        userdf = df_employees[df_employees['id'] == int(np.int64(id_user))].reset_index()
                                                        print("yeaarrrrr")
                                                        print(userdf)
                                                        firstname = userdf.iat[0,2]
                                                        surname = userdf.iat[0,3]
                                                        whatsapp = userdf.iat[0,4]
                                                        address = userdf.iat[0,6]
                                                        email = userdf.iat[0,5]
                                                        fullnamedisp = firstname + ' ' + surname
                                                        leaveapprovername = userdf.iat[0,8]
                                                        leaveapproverid = userdf.iat[0,9]
                                                        leaveapproveremail = userdf.iat[0, 10]
                                                        leaveapproverwhatsapp = userdf.iat[0,11]
                                                        role = userdf.iat[0,7]
                                                        leavedaysbalance = userdf.iat[0,12]
                                                        department = userdf.iat[0,14]
                                                        print('check')

                                                        departmentdf = df_employees[df_employees['Department'] == department].reset_index()
                                                        numberindepartment = len(departmentdf)

                                                        startdatex = pd.Timestamp(startdate)
                                                        enddatex = pd.Timestamp(enddate)

                                                        leave_dates = pd.date_range(startdatex, enddatex)

                                                        query = f"""
                                                            SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate,
                                                                leaveenddate, leavedaysappliedfor, approvalstatus, statusdate,
                                                                leavedaysbalancebf, department
                                                            FROM {table_name_apps_approved}
                                                            WHERE department = %s;
                                                        """
                                                        cursor.execute(query, (department,))
                                                        rows = cursor.fetchall()

                                                        df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf","department"]) 

                                                        # Create daily impact report
                                                        impact_report = []

                                                        for date in leave_dates:

                                                            date = pd.Timestamp(date)

                                                            df_employeesappsapprovedcheck["leavestartdate"] = pd.to_datetime(df_employeesappsapprovedcheck["leavestartdate"])
                                                            df_employeesappsapprovedcheck["leaveenddate"] = pd.to_datetime(df_employeesappsapprovedcheck["leaveenddate"])

                                                            print(type(date))  # Should be pandas._libs.tslibs.timestamps.Timestamp or datetime.datetime
                                                            print(df_employeesappsapprovedcheck.dtypes)  # Check all datetime columns

                                                            on_leave = ((df_employeesappsapprovedcheck["leavestartdate"] <= date) & (df_employeesappsapprovedcheck["leaveenddate"] >= date)).sum()
                                                            remaining = numberindepartment - on_leave - 1  # subtract 1 for the new leave
                                                            impact_report.append({
                                                                "date": date.strftime("%Y-%m-%d"),
                                                                "on leave (including new)": on_leave + 1,
                                                                "employees remaining": remaining
                                                            })

                                                        # Convert to DataFrame for display
                                                        impact_df = pd.DataFrame(impact_report)
                                                        print("IMPAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACT")
                                                        print(impact_df)
                                                        print(numberindepartment)

                                                        impact_df["date"] = pd.to_datetime(impact_df["date"], dayfirst=True)
                                                        impact_df = impact_df[impact_df["date"].dt.weekday != 6].copy()

                                                        impact_df["group"] = (impact_df[["on leave", "employees remaining"]] != impact_df[["on leave", "employees remaining"]].shift()).any(axis=1).cumsum()

                                                        statements = []
                                                        for _, group_df in impact_df.groupby("group"):
                                                            start = group_df["date"].iloc[0].strftime("%d %B %Y")
                                                            end = group_df["date"].iloc[-1].strftime("%d %B %Y")
                                                            on_leave = group_df["on leave"].iloc[0]
                                                            remaining = group_df["employees remaining"].iloc[0]
                                                            
                                                            if start == end:
                                                                statements.append(f"On {start}, the {department} department will have {remaining} employee(s) remaining at work and {on_leave} employee(s) on leave.")
                                                            else:
                                                                statements.append(f"From {start} to {end}, the {department} department will have {remaining} employee(s) remaining at work and {on_leave} employee(s) on leave.")
                                                                
                                                        # Combine all statements into a single variable
                                                        final_summary = "\n".join(statements)
                                                        # Print output
                                                        for s in statements:
                                                            print(s)

                                                        leavedaysbalancebf = int(leavedaysbalance) - int(business_days)

                                                        status = "Pending"

                                                        insert_query = f"""
                                                        INSERT INTO {table_name_apps_pending_approval} (id, firstname, surname, department, leavetype, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus)
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                        """
                                                        cursor.execute(insert_query, (int(np.int64(id_user)), first_name, last_name, department, leavetype, leaveapprovername, int(np.int64(leaveapproverid)), leaveapproveremail, int(np.int64(leaveapproverwhatsapp)), int(np.int64(leavedaysbalance)), today_date, startdate, enddate, int(np.int64(business_days)), int(np.int64(leavedaysbalancebf)), status))
                                                        connection.commit()

                                                        query = f"SELECT appid FROM {table_name_apps_pending_approval};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()

                                                        df_employees = pd.DataFrame(rows, columns=["id"])
                                                        leaveappid = df_employees.iat[0,0]
                                                        companyxx = company_reg.replace("_"," ").title()
                                                        approovvver = leaveapprovername.title()

                                                        send_whatsapp_message(sender_id, f"✅ Great News {first_name} from {companyxx}! \n\n Your `{leavetype} Leave Application` for `{business_days} days` from `{startdate.strftime('%d %B %Y')}` to `{enddate.strftime('%d %B %Y')}` has been submitted successfully!\n\n"
                                                            f"Your Leave Application ID is `{leaveappid}`.\n\n"
                                                            f"A Notification has been sent to `{approovvver}`  on `+263{leaveapproverwhatsapp}` to decide on  your application.\n\n"
                                                            "To Check the approval status of your leave application, type `Hello` then select `Track Application`.")
                                                        
                                                        if leaveapproverwhatsapp:
            
                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": f"Approve5appwa_{leaveappid}", "title": "Approve"}},
                                                                {"type": "reply", "reply": {"id": f"Disapproveappwa_{leaveappid}", "title": "Disapprove"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                f"263{leaveapproverwhatsapp}", 
                                                                f"Hey {approovvver}! 😊. New `{leavetype}` Leave Application from `{first_name} {surname}` for `{business_days} days` from `{startdate.strftime('%d %B %Y')}` to `{enddate.strftime('%d %B %Y')}`.\n\n" 
                                                                f"If you approve this leave application, {final_summary}\n\n"  
                                                                f"Select an option below to either approve or disapprove the application."         
                                                                , 
                                                                buttons
                                                            )

                                                    else:
                                                        print("leave app submission failed")

                                                except ValueError as e:
                                                    send_whatsapp_message(
                                                        sender_id,
                                                        f"{e}, ❌ No, incorrect message format. Please use:\n"
                                                        "`end 24 january 2025`\n"
                                                        "Example: `end 15 march 2024`"
                                                    )

                                            elif button_id == "Checkbal":

                                                buttons = [
                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                {"type": "reply", "reply": {"id": "Track", "title": "Track Application"}},
                                                ]

                                                send_whatsapp_message(
                                                    sender_id, 
                                                    f"Hey {first_name}, your current available leave days balance is `{days_days_balance} days`.\n\n"
                                                    "Select an option below to continue 👇",
                                                    buttons
                                                )

                                            elif button_id == "Resubmitapp" :

                                                table_name_apps_cancelled = f"{company_reg}appscancelled"
                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"

                                                query = f"SELECT appid, id, firstname, surname, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail , leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, department FROM {table_name_apps_cancelled} WHERE id = %s;"
                                                cursor.execute(query, (id_user,))
                                                result = cursor.fetchall()

                                                if result:

                                                    df_employees = pd.DataFrame(result)
                                                    df_employees = df_employees.sort_values(by=df_employees.columns[0], ascending=False)
                                                    print(df_employees)
                                                            
                                                    try:

                                                        status = "Pending"
                                                        app_id = int(np.int64(df_employees.iat[0,0]))
                                                        employee_number = int(np.int64(df_employees.iat[0,1]))
                                                        first_name = df_employees.iat[0,2]
                                                        surname = df_employees.iat[0,3]
                                                        leave_type = df_employees.iat[0,4]
                                                        leave_specify = df_employees.iat[0,5]
                                                        approver_name = df_employees.iat[0,6]
                                                        approver_id =  int(np.int64(df_employees.iat[0,7]))
                                                        approver_email =  df_employees.iat[0,8]
                                                        approver_whatsapp =  int(np.int64(df_employees.iat[0,9]))
                                                        leave_days_balance =  int(np.int64(df_employees.iat[0,10]))
                                                        date_applied = df_employees.iat[0,11]
                                                        start_date = df_employees.iat[0,12]
                                                        end_date = df_employees.iat[0,13]
                                                        leave_days =  int(np.int64(df_employees.iat[0,14]))
                                                        leavedaysbalancebf =  int(np.int64(df_employees.iat[0,15]))
                                                        department = df_employees.iat[0,16]
                                                        insert_query = f"""
                                                        INSERT INTO {table_name_apps_pending_approval} 
                                                        (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus)
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                        """

                                                        cursor.execute(insert_query, (
                                                            app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                                                            approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                                            end_date, leave_days, leavedaysbalancebf, status
                                                        ))
                                                        
                                                        connection.commit()
                                                        print("Insert successful!")

                                                    except Exception as e:
                                                        print("Error inserting data:", e)

                                                    # SQL query to delete or mark the leave as canceled
                                                    query = f"""DELETE FROM {table_name_apps_cancelled} WHERE appid = %s"""
                                                    cursor.execute(query, (app_id,))
                                                    connection.commit()                                       

                                                    companyxx = company_reg.replace("_", " ").title()
                                                    sections = [
                                                        {
                                                            "title": "Administrator Options",
                                                            "rows": [
                                                                {"id": "Apply", "title": "Apply for Leave"},
                                                                {"id": "Track", "title": "Track My Application"},
                                                                {"id": "Checkbal", "title": "Check Days Balance"},
                                                                {"id": "Template", "title": "Add Employees"},
                                                                {"id": "Rolechange", "title": "Change Employee's Role"},
                                                                {"id": "Book", "title": "Extract Leave Book"}
                                                            ]
                                                        }
                                                    ]

                                                    send_whatsapp_list_message(sender_id, f"Hey {first_name} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}` has been Re-Submitted for approval successfully✅!",
                                                    "Administrator Options",
                                                    sections)                                          
                                                
                                                else:
                                                    print("No record found for the user.")

                                            elif button_id == "Cancelapp" :

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                table_name_apps_cancelled = f"{company_reg}appscancelled"

                                                query = f"SELECT appid, id, firstname, surname, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail , leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, department FROM {table_name_apps_pending_approval} WHERE id = %s;"
                                                cursor.execute(query, (id_user,))
                                                result = cursor.fetchone()
                                                if result:
                                                    (app_id, employee_number, first_name, surname, leave_type,  leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf, department) = result
                                                
                                                    try:
                                                            status = "Cancelled"
                                                            statusdate = today_date
                                                        
                                                            insert_query = f"""
                                                            INSERT INTO {table_name_apps_cancelled} 
                                                            (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                            """

                                                            cursor.execute(insert_query, (
                                                                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                                                                approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                                                end_date, leave_days, leavedaysbalancebf, status, statusdate
                                                            ))
                                                            
                                                            connection.commit()
                                                            print("Insert successful!")

                                                    except Exception as e:
                                                        print("Error inserting data:", e)

                                                    # SQL query to delete or mark the leave as canceled
                                                    query = f"""DELETE FROM {table_name_apps_pending_approval} WHERE appid = %s"""
                                                    cursor.execute(query, (app_id,))
                                                    connection.commit()                                       

                                                    companyxx = company_reg.replace("_", " ").title()
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                        {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                        {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                    ]

                                                    send_whatsapp_message(sender_id, f"Hey {first_name} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}` has been Cancelled successfully✅!\n\n"
                                                        "Select an option below to continue 👇",
                                                        buttons
                                                    )                                          
                                                
                                                else:
                                                    print("No record found for the user.")


                                        
                                        if interactive.get("type") == "list_reply":

                                            selected_option = interactive.get("list_reply", {}).get("id")
                                            print(f"📋 User selected: {selected_option}")

                                            if selected_option == "Apply":

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"

                                                query = f"SELECT id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor  FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor"])    

                                                if len(df_employeesappspendingcheck) == 0:

                                                        sections = [
                                                            {
                                                                "title": "Leave Type Options",
                                                                "rows": [
                                                                    {"id": "Annual", "title": "Annual Leave"},
                                                                    {"id": "Sick", "title": "Sick Leave"},
                                                                    {"id": "Study", "title": "Study Leave"},
                                                                    {"id": "Bereavement", "title": "Bereavement Leave"},
                                                                    {"id": "Parental", "title": "Parental Leave"},
                                                                    {"id": "Other", "title": "Other"},
                                                                ]
                                                            }
                                                        ]

                                                        send_whatsapp_list_message(
                                                            sender_id, 
                                                            f"{first_name}, kindly select the type of Leave that you are applying for.", 
                                                            "Leave Type Options",
                                                            sections) 

                                                elif len(df_employeesappspendingcheck) > 0:
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Reminder", "title": "Remind Approver"}},
                                                        {"type": "reply", "reply": {"id": "Cancelapp", "title": "Cancel Pending App"}},
                                                    ]
                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"Oops! 🥲. Sorry {first_name}, you cannot apply for leave whilst you have another leave application which is still pending approval.\n\n" 
                                                        f"Your `{df_employeesappspendingcheck.iat[0,1]}` Leave Application `[ID - {df_employeesappspendingcheck.iat[0,0]}]` applied on `{df_employeesappspendingcheck.iat[0,3].strftime('%d %B %Y')}` for `{df_employeesappspendingcheck.iat[0,6]} days from {df_employeesappspendingcheck.iat[0,4].strftime('%d %B %Y')} to {df_employeesappspendingcheck.iat[0,5].strftime('%d %B %Y')}` is still pending approval from {df_employeesappspendingcheck.iat[0,2]}.\n\n" 
                                                        f"Select an option below to either remind the approver to approved your pending application or you can cancel the pending application to submit a new leave application."         
                                                        , 
                                                        buttons
                                                    )

                                            elif selected_option in ["Annual","Sick","Study","Parental", "Bereavement","Other"] :
                                                button_id_leave_type = str(selected_option)

                                                cursor.execute("""
                                                    DELETE FROM whatsapptempapplication
                                                    WHERE empidwa = %s
                                                """, (str(id_user),))  
                                                
                                                connection.commit()

                                                cursor.execute(f"""
                                                    INSERT INTO whatsapptempapplication (empidwa, leavetypewa, companynamewa)
                                                    VALUES (%s, %s, %s)
                                                """, (id_user, button_id_leave_type, company_reg))

                                                connection.commit()

                                                send_whatsapp_message(
                                                    sender_id, 
                                                    f"Ok. When would you like your {selected_option} Leave to start {first_name}?\n\n"
                                                    "Please enter your response using the format: 👇🏻\n"
                                                    "`start 24 january 2025`"
                                                )

                                                continue
                                                
                                            elif selected_option == "Track":

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                table_name_apps_approved = f"{company_reg}appsapproved"
                                                table_name_apps_declined = f"{company_reg}appsdeclined"
                                                table_name_apps_cancelled = f"{company_reg}appscancelled"


                                                query = f"SELECT id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leaveapproverwhatsapp, department  FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id","leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor", "leaveapproverwhatsapp", "department"])    

                                                if len(df_employeesappspendingcheck) == 0:

                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf, department FROM {table_name_apps_approved} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid", "id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf", "department"]) 

                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf, department FROM {table_name_apps_declined} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappsdeclinedcheck = pd.DataFrame(rows, columns=["appid", "id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf", "department"])  
                            
                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf, department FROM {table_name_apps_cancelled} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappscancelledcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf", "department"])
                            
                                                    all_approved_declined = df_employeesappsapprovedcheck._append(df_employeesappsdeclinedcheck)
                                                    all_approved_declined_cancelled = all_approved_declined._append(df_employeesappscancelledcheck)
                                                    all_approved_declined_cancelled = all_approved_declined_cancelled.sort_values(by="appid", ascending=False)  
                                                    print(f" hhhhhhhhhhhhhhhhhhhh  {all_approved_declined_cancelled.iat[0,8] }")

                                                    if all_approved_declined_cancelled.iat[0,8] == "Approved":

                                                        buttons = [
                                                            {"type": "reply", "reply": {"id": "Revoke", "title": "Revoke Application"}},
                                                            {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                            {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                        ]
                                                        send_whatsapp_message(
                                                            sender_id, 
                                                            f"Hey {first_name}, your recent `{all_approved_declined_cancelled.iat[0,2]}` Leave Application `[ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}` was {all_approved_declined_cancelled.iat[0,8]}✅ by `{all_approved_declined_cancelled.iat[0,3].title()}` on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}`." 
                                                        )


                                                        def generate_leave_pdf():
                                                            app = {
                                                                'company_logo': 44,
                                                                'company_name': company_reg.replace("_"," ").title(),
                                                                'employee_name': f"{first_name} {last_name}",
                                                                'leave_type': all_approved_declined_cancelled.iat[0,2],
                                                                'generated_on': today_date,
                                                                'date_applied': all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y'),
                                                                'approver_name': all_approved_declined_cancelled.iat[0,3].title(),
                                                                'reference_number': all_approved_declined_cancelled.iat[0,0],
                                                                'approved_date': all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y'),
                                                                'new_balance': all_approved_declined_cancelled.iat[0,10],
                                                                'start_date':  all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y'),
                                                                'end_date':  all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y'),
                                                                'days_requested':  all_approved_declined_cancelled.iat[0,7], 
                                                                'department': all_approved_declined_cancelled.iat[0,11], 
                                                                'address': address_foc_8, 
                                                                'whatsapp': whatsapp_foc_8, 
                                                                'email': email_foc_8, 
                                                                'status': 'Approved'
                                                            }

                                                            html_out = render_template("leave_pdf_template.html", app=app)
                                                            
                                                            # ✅ Return as bytes instead of saving to file
                                                            pdf_bytes = HTML(string=html_out).write_pdf()
                                                            return pdf_bytes

                                                        
                                                        global ACCESS_TOKEN
                                                        global PHONE_NUMBER_ID

                                                        def upload_pdf_to_whatsapp(pdf_bytes):
                                                            compxxy = company_reg.replace("_"," ").title()
                                                            filename=f"leave_application_{all_approved_declined_cancelled.iat[0,0]}_{first_name}_{last_name}_{compxxy}.pdf"
                                                        
                                                            url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/media"
                                                            headers = {
                                                                "Authorization": f"Bearer {ACCESS_TOKEN}"
                                                            }

                                                            files = {
                                                                "file": (filename, io.BytesIO(pdf_bytes), "application/pdf"),
                                                                "type": (None, "application/pdf"),
                                                                "messaging_product": (None, "whatsapp")
                                                            }

                                                            response = requests.post(url, headers=headers, files=files)
                                                            print("📥 Full incoming data:", response.text)  # Good for debugging
                                                            response.raise_for_status()
                                                            return response.json()["id"]

                                                                                                        
                                                        def send_whatsapp_pdf_by_media_id(recipient_number, media_id):
                                                            compxxy = company_reg.replace("_"," ").title()
                                                            filename=f"leave_application_{all_approved_declined_cancelled.iat[0,0]}_{first_name}_{last_name}_{compxxy}.pdf"
                                                            url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
                                                            headers = {
                                                                "Authorization": f"Bearer {ACCESS_TOKEN}",
                                                                "Content-Type": "application/json"
                                                            }
                                                            payload = {
                                                                "messaging_product": "whatsapp",
                                                                "to": recipient_number,
                                                                "type": "document",
                                                                "document": {
                                                                    "id": media_id,            # Media ID from upload step
                                                                    "filename": filename       # Desired file name on recipient's phone
                                                                }
                                                            }

                                                            response = requests.post(url, headers=headers, json=payload)
                                                            response.raise_for_status()
                                                            return response.json()


                                                        pdf_path = generate_leave_pdf()
                                                        media_id = upload_pdf_to_whatsapp(pdf_path)
                                                        send_whatsapp_pdf_by_media_id(sender_id, media_id)

                                                        send_whatsapp_message(
                                                            sender_id,
                                                            "Select an option below to continue 👇",
                                                            buttons
                                                        )

                                                    elif all_approved_declined_cancelled.iat[0,8] == "Declined":

                                                        buttons = [
                                                            {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                            {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                            {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                        ]
                                                        send_whatsapp_message(
                                                            sender_id, 
                                                            f"Hey {first_name}, your recent `{all_approved_declined_cancelled.iat[0,2]}` Leave Application `[ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}` was {all_approved_declined_cancelled.iat[0,8]}❌ by `{all_approved_declined_cancelled.iat[0,3].title()}` on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}`.",
                                                            buttons 
                                                        )

                                                    elif all_approved_declined_cancelled.iat[0,8] == "Cancelled":

                                                        buttons = [
                                                            {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                            {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                            {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                        ]
                                                        send_whatsapp_message(
                                                            sender_id, 
                                                            f"Hey {first_name}, on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}` you Cancelled ⛔ your recent `{all_approved_declined_cancelled.iat[0,2]} Leave Application [ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}`.",
                                                            buttons 
                                                        )

                                                elif len(df_employeesappspendingcheck) > 0:
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Reminder", "title": "Remind Approver"}},
                                                        {"type": "reply", "reply": {"id": "Cancelapp", "title": "Cancel Pending App"}},
                                                    ]
                                                    approoooover = df_employeesappspendingcheck.iat[0,2].title()
                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"Hey {first_name}, your recent `{df_employeesappspendingcheck.iat[0,1]}` Leave Application `[ID - {df_employeesappspendingcheck.iat[0,0]}]` applied on `{df_employeesappspendingcheck.iat[0,3].strftime('%d %B %Y')}` for `{df_employeesappspendingcheck.iat[0,6]} days from {df_employeesappspendingcheck.iat[0,4].strftime('%d %B %Y')} to {df_employeesappspendingcheck.iat[0,5].strftime('%d %B %Y')}` is still pending approval from `{approoooover}`.\n\n" 
                                                        f"Select an option below to either remind `{approoooover}` to approve your pending leave application or you can cancel the pending application to submit a new leave application."         
                                                        , 
                                                        buttons
                                                    )

                                                
                                            elif selected_option == "Checkbal":

                                                sections = [
                                                    {
                                                        "title": "Administrator Options",
                                                        "rows": [
                                                            {"id": "Apply", "title": "Apply for Leave"},
                                                            {"id": "Track", "title": "Track My Application"},
                                                            {"id": "Checkbal", "title": "Check Days Balance"},
                                                            {"id": "Template", "title": "Add Employees"},
                                                            {"id": "Rolechange", "title": "Change Employee's Role"},
                                                            {"id": "Book", "title": "Extract Leave Book"}
                                                        ]
                                                    }
                                                ]

                                                send_whatsapp_list_message(
                                                    sender_id, 
                                                    f"Hey {first_name}, your current available leave days balance is `{days_days_balance} days`.",
                                                    "Administrator Options",
                                                    sections
                                                )
                                                
                                            elif selected_option == "Pending":




                                                
                                                # Handle Apps Pending My Approval
                                                pass
                                                
                                            elif selected_option == "Template":
                                                # Handle Add Employees
                                                pass
                                                
                                            elif selected_option == "Rolechange":
                                                # Handle Change Employee's Role
                                                pass
                                                
                                            elif selected_option == "Book":
                                                # Handle Extract Leave Book
                                                pass
                                                

                                    elif message.get("type") == "text":
                                        text = message.get("text", {}).get("body", "").lower()
                                        print(f"📨 Message from {sender_id}: {text}")
                                        
                                        if "hello" in text.lower():
                                            companyxx = company_reg.replace("_"," ").title()
                                            
                                            sections = [
                                                {
                                                    "title": "Administrator Options",
                                                    "rows": [
                                                        {"id": "Apply", "title": "Apply for Leave"},
                                                        {"id": "Track", "title": "Track My Application"},
                                                        {"id": "Checkbal", "title": "Check Days Balance"},
                                                        {"id": "Template", "title": "Add Employees"},
                                                        {"id": "Rolechange", "title": "Change Employee's Role"},
                                                        {"id": "Book", "title": "Extract Leave Book"}
                                                    ]
                                                }
                                            ]
                                            
                                            send_whatsapp_list_message(
                                                sender_id,
                                                f"Hello {first_name} {last_name}, LMS Administrator from {companyxx}!\n\nEchelon Bot Here 😎. How can I assist you?",
                                                "Administrator Options",
                                                sections
                                            )

                                        elif "start" in text.lower():

                                            date_part = text.split("start", 1)[1].strip()

                                            cursor.execute("""
                                                UPDATE whatsapptempapplication
                                                SET startdate = %s
                                                WHERE empidwa = %s
                                            """, (date_part, id_user))

                                            connection.commit()

                                            cursor.execute("""
                                                SELECT empidwa, leavetypewa FROM whatsapptempapplication
                                                WHERE empidwa = %s
                                            """, (str(id_user)))
                                    
                                            result = cursor.fetchone()

                                            if result:
                                                leavetypewa = result[1] 

                                            cursor.execute("SELECT * FROM whatsapptempapplication")
                                            columns = [desc[0] for desc in cursor.description]
                                            records = cursor.fetchall()
                                            
                                            df = pd.DataFrame(records, columns=columns)
                                            
                                            print("\n📊 whatsapptempapplication Table:")
                                            print(df)
                                            
                                            try:
                                                parsed_date = datetime.strptime(date_part, "%d %B %Y")
                                                send_whatsapp_message(sender_id, "✅ Yes! Valid start date format.\n\n"
                                                    f"Now Enter the last day that you will be on {leavetypewa} Leave.Use the format: 👇🏻\n"
                                                    "`end 24 january 2025`"                      
                                                                    )
                                                
                                            except ValueError:
                                                send_whatsapp_message(
                                                    sender_id,
                                                    f"❌ No, incorrect message format, {first_name}. Please use:\n"
                                                    "`start 24 january 2025`\n"
                                                    "Example: `start 15 march 2024`"
                                                )

                                        elif "end" in text.lower():

                                            date_part = text.split("end", 1)[1].strip()

                                            cursor.execute("""
                                                UPDATE whatsapptempapplication
                                                SET enddate = %s
                                                WHERE empidwa = %s
                                            """, (date_part, id_user))

                                            connection.commit()

                                            cursor.execute("""
                                                SELECT id ,empidwa, leavetypewa, startdate, enddate FROM whatsapptempapplication
                                                WHERE empidwa = %s
                                            """, (str(id_user)))
                                    
                                            result = cursor.fetchone()

                                            appid = result[0]
                                            leavetype = result[2]
                                            startdate = result[3]
                                            enddate = result[4]

                                            if isinstance(startdate, str):
                                                startdate = datetime.datetime.strptime(startdate, "%Y-%m-%d").date()
                                            if isinstance(enddate, str):
                                                enddate = datetime.datetime.strptime(enddate, "%Y-%m-%d").date()

                                            business_days = 0
                                            current_date = startdate

                                            while current_date <= enddate:
                                                if current_date.weekday() < 5:  # 0=Mon, 1=Tue, ..., 4=Fri
                                                    business_days += 1
                                                current_date += timedelta(days=1)  # Use timedelta directly

                                            print(f"📅 Business days between {startdate} and {enddate}: {business_days}")


                                            buttons = [
                                                {"type": "reply", "reply": {"id": "Submitapp", "title": "Yes, Submit"}},
                                                {"type": "reply", "reply": {"id": "Dontsubmit", "title": "No"}}
                                            ]
                                            send_whatsapp_message(
                                                sender_id, 
                                                f"Do you wish to submit your `{business_days} day {leavetype} Leave Application` leave starting from `{startdate.strftime('%d %B %Y')}` to `{enddate.strftime('%d %B %Y')}` {first_name} ?", 
                                                buttons
                                            )

                                        else:
                                            send_whatsapp_message(
                                                sender_id, 
                                                "Echelon Bot Here 😎. Say 'hello' to start!"
                                            )



                                elif len(df_employeesempapp) > 0:

                                    if message.get("type") == "interactive":
                                        interactive = message.get("interactive", {})


                                        if interactive.get("type") == "button_reply":

                                            button_id = interactive.get("button_reply", {}).get("id")
                                            print(f"🔘 Button clicked: {button_id}")
                                            
                                            if button_id == "Track":

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                table_name_apps_approved = f"{company_reg}appsapproved"
                                                table_name_apps_declined = f"{company_reg}appsdeclined"
                                                table_name_apps_cancelled = f"{company_reg}appscancelled"


                                                query = f"SELECT id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leaveapproverwhatsapp, appid, department  FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor", "leaveapproverwhatsapp", "appid", "department"])    

                                                if len(df_employeesappspendingcheck) == 0:

                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf, department  FROM {table_name_apps_approved} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf", "department"]) 

                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf, department  FROM {table_name_apps_declined} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappsdeclinedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf", "department"])  
                            
                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf, department  FROM {table_name_apps_cancelled} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappscancelledcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf", "department"])
                            
                                                    all_approved_declined = df_employeesappsapprovedcheck._append(df_employeesappsdeclinedcheck)
                                                    all_approved_declined_cancelled = all_approved_declined._append(df_employeesappscancelledcheck)
                                                    all_approved_declined_cancelled = all_approved_declined_cancelled.sort_values(by="appid", ascending=False)  


                                                    if len(all_approved_declined_cancelled) > 0:

                                                        print(f" hhhhhhhhhhhhhhhhhhhh  {all_approved_declined_cancelled.iat[0,8] }")

                                                        if all_approved_declined_cancelled.iat[0,8] == "Approved":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Revoke", "title": "Revoke Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, your recent `{all_approved_declined_cancelled.iat[0,2]}` Leave Application `[ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}` was {all_approved_declined_cancelled.iat[0,8]}✅ by `{all_approved_declined_cancelled.iat[0,3].title()}` on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}`." 
                                                            )


                                                            def generate_leave_pdf():
                                                                app = {
                                                                    'company_logo': 44,
                                                                    'company_name': company_reg.replace("_"," ").title(),
                                                                    'employee_name': f"{first_name} {last_name}",
                                                                    'leave_type': all_approved_declined_cancelled.iat[0,2],
                                                                    'generated_on': today_date,
                                                                    'date_applied': all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y'),
                                                                    'approver_name': all_approved_declined_cancelled.iat[0,3].title(),
                                                                    'reference_number': all_approved_declined_cancelled.iat[0,0],
                                                                    'approved_date': all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y'),
                                                                    'new_balance': all_approved_declined_cancelled.iat[0,10],
                                                                    'start_date':  all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y'),
                                                                    'end_date':  all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y'),
                                                                    'days_requested':  all_approved_declined_cancelled.iat[0,7], 
                                                                    'department':  all_approved_declined_cancelled.iat[0,11], 
                                                                    'address': address_foc_8, 
                                                                    'whatsapp': whatsapp_foc_8, 
                                                                    'email': email_foc_8, 
                                                                    'status': 'Approved'
                                                                }

                                                                html_out = render_template("leave_pdf_template.html", app=app)
                                                                
                                                                # ✅ Return as bytes instead of saving to file
                                                                pdf_bytes = HTML(string=html_out).write_pdf()
                                                                return pdf_bytes

                                                            
                                                            global ACCESS_TOKEN
                                                            global PHONE_NUMBER_ID

                                                            def upload_pdf_to_whatsapp(pdf_bytes):
                                                                compxxy = company_reg.replace("_"," ").title()
                                                                filename=f"leave_application_{all_approved_declined_cancelled.iat[0,0]}_{first_name}_{last_name}_{compxxy}.pdf"
                                                            
                                                                url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/media"
                                                                headers = {
                                                                    "Authorization": f"Bearer {ACCESS_TOKEN}"
                                                                }

                                                                files = {
                                                                    "file": (filename, io.BytesIO(pdf_bytes), "application/pdf"),
                                                                    "type": (None, "application/pdf"),
                                                                    "messaging_product": (None, "whatsapp")
                                                                }

                                                                response = requests.post(url, headers=headers, files=files)
                                                                print("📥 Full incoming data:", response.text)  # Good for debugging
                                                                response.raise_for_status()
                                                                return response.json()["id"]

                                                                                                            
                                                            def send_whatsapp_pdf_by_media_id(recipient_number, media_id):
                                                                compxxy = company_reg.replace("_"," ").title()
                                                                filename=f"leave_application_{all_approved_declined_cancelled.iat[0,0]}_{first_name}_{last_name}_{compxxy}.pdf"
                                                                url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
                                                                headers = {
                                                                    "Authorization": f"Bearer {ACCESS_TOKEN}",
                                                                    "Content-Type": "application/json"
                                                                }
                                                                payload = {
                                                                    "messaging_product": "whatsapp",
                                                                    "to": recipient_number,
                                                                    "type": "document",
                                                                    "document": {
                                                                        "id": media_id,            # Media ID from upload step
                                                                        "filename": filename       # Desired file name on recipient's phone
                                                                    }
                                                                }

                                                                response = requests.post(url, headers=headers, json=payload)
                                                                response.raise_for_status()
                                                                return response.json()


                                                            pdf_path = generate_leave_pdf()
                                                            media_id = upload_pdf_to_whatsapp(pdf_path)
                                                            send_whatsapp_pdf_by_media_id(sender_id, media_id)

                                                            send_whatsapp_message(
                                                                sender_id,
                                                                "Select an option below to continue 👇",
                                                                buttons
                                                            )

                                                        elif all_approved_declined_cancelled.iat[0,8] == "Declined":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, your recent `{all_approved_declined_cancelled.iat[0,2]}` Leave Application `[ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}` was {all_approved_declined_cancelled.iat[0,8]}❌ by `{all_approved_declined_cancelled.iat[0,3].title()}` on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}`.",
                                                                buttons 
                                                            )

                                                        elif all_approved_declined_cancelled.iat[0,8] == "Cancelled":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}` you Cancelled ⛔ your recent `{all_approved_declined_cancelled.iat[0,2]} Leave Application [ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}`.",
                                                                buttons 
                                                            )

                                                    else:

                                                        sections = [
                                                            {
                                                                "title": "Administrator Options",
                                                                "rows": [
                                                                    {"id": "Apply", "title": "Apply for Leave"},
                                                                    {"id": "Checkbal", "title": "Check Days Balance"},
                                                                    {"id": "Pending", "title": "Apps Pending My Approval"},
                                                                    {"id": "Template", "title": "Add Employees"},
                                                                    {"id": "Rolechange", "title": "Change Employee's Role"},
                                                                    {"id": "Book", "title": "Extract Leave Book"}
                                                                ]
                                                            }
                                                        ]
                                                        companyxx = company_reg.replace("_"," ").title()


                                                        send_whatsapp_list_message(
                                                            sender_id, 
                                                            f"Hello {first_name} {last_name}, LMS Leave Applications Approver from {companyxx}!\n\n You have not applied for any leave days yet.", 
                                                            "Administrator Options",
                                                            sections
                                                        )

                                                elif len(df_employeesappspendingcheck) > 0:
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Reminder", "title": "Remind Approver"}},
                                                        {"type": "reply", "reply": {"id": "Cancelapp", "title": "Cancel Pending App"}},
                                                    ]
                                                    approoooover = df_employeesappspendingcheck.iat[0,2].title()
                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"Hey {first_name}, your recent `{df_employeesappspendingcheck.iat[0,1]}` Leave Application `[ID - {df_employeesappspendingcheck.iat[0,9]}]` applied on `{df_employeesappspendingcheck.iat[0,3].strftime('%d %B %Y')}` for `{df_employeesappspendingcheck.iat[0,6]} days from {df_employeesappspendingcheck.iat[0,4].strftime('%d %B %Y')} to {df_employeesappspendingcheck.iat[0,5].strftime('%d %B %Y')}` is still pending approval from `{approoooover}`.\n\n" 
                                                        f"Select an option below to either remind `{approoooover}` to approve your pending leave application or you can cancel the pending application to submit a new leave application."         
                                                        , 
                                                        buttons
                                                    )

                                            elif button_id == "Apply":

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"

                                                query = f"SELECT id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor  FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor"])    

                                                if len(df_employeesappspendingcheck) == 0:

                                                    sections = [
                                                        {
                                                            "title": "Leave Type Options",
                                                            "rows": [
                                                                {"id": "Annual", "title": "Annual Leave"},
                                                                {"id": "Sick", "title": "Sick Leave"},
                                                                {"id": "Study", "title": "Study Leave"},
                                                                {"id": "Bereavement", "title": "Bereavement Leave"},
                                                                {"id": "Parental", "title": "Parental Leave"},
                                                                {"id": "Other", "title": "Other"},
                                                            ]
                                                        }
                                                    ]

                                                    send_whatsapp_list_message(
                                                        sender_id, 
                                                        f"{first_name}, kindly select the type of Leave that you are applying for.", 
                                                        "Leave Type Options",
                                                        sections) 

                                                elif len(df_employeesappspendingcheck) > 0:
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Reminder", "title": "Remind Approver"}},
                                                        {"type": "reply", "reply": {"id": "Cancelapp", "title": "Cancel Pending App"}},
                                                    ]
                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"Oops! 🥲. Sorry {first_name}, you cannot apply for leave whilst you have another leave application which is still pending approval.\n\n" 
                                                        f"Your `{df_employeesappspendingcheck.iat[0,1]}` Leave Application `[ID - {df_employeesappspendingcheck.iat[0,0]}]` applied on `{df_employeesappspendingcheck.iat[0,3].strftime('%d %B %Y')}` for `{df_employeesappspendingcheck.iat[0,6]} days from {df_employeesappspendingcheck.iat[0,4].strftime('%d %B %Y')} to {df_employeesappspendingcheck.iat[0,5].strftime('%d %B %Y')}` is still pending approval from {df_employeesappspendingcheck.iat[0,2]}.\n\n" 
                                                        f"Select an option below to either remind the approver to approved your pending application or you can cancel the pending application to submit a new leave application."         
                                                        , 
                                                        buttons
                                                    )

                                            elif button_id == "Submitapp":
                                    
                                                try:

                                                    table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                    table_name_apps_approved = f"{company_reg}appsapproved"

                                                    query = f"SELECT id FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()

                                                    df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id"])    

                                                    if len(df_employeesappspendingcheck) == 0:

                                                        cursor.execute("""
                                                            SELECT id ,empidwa, leavetypewa, startdate, enddate FROM whatsapptempapplication
                                                            WHERE empidwa = %s
                                                        """, (str(id_user)))
                                                
                                                        result = cursor.fetchone()

                                                        appid = result[0]
                                                        leavetype = result[2]
                                                        startdate = result[3]
                                                        enddate = result[4]
                                                        table_name = f"{company_reg}main"

                                                        if isinstance(startdate, str):
                                                            startdate = datetime.datetime.strptime(startdate, "%Y-%m-%d").date()
                                                        if isinstance(enddate, str):
                                                            enddate = datetime.datetime.strptime(enddate, "%Y-%m-%d").date()

                                                        business_days = 0
                                                        current_date = startdate

                                                        while current_date <= enddate:
                                                            if current_date.weekday() < 5:  # 0=Mon, 1=Tue, ..., 4=Fri
                                                                business_days += 1
                                                            current_date += timedelta(days=1)  # Use timedelta directly

                                                        query = f"SELECT id, firstname, surname, whatsapp, email, address, role, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, monthlyaccumulation, department FROM {table_name};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()

                                                        df_employees = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Address", "Role","Leave Approver Name","Leave Approver ID","Leave Approver Email", "Leave Approver WhatsAapp", "Leave Days Balance","Days Accumulated per Month", "Department"])
                                                        print(df_employees)
                                                        userdf = df_employees[df_employees['id'] == int(np.int64(id_user))].reset_index()
                                                        print("yeaarrrrr")
                                                        print(userdf)
                                                        firstname = userdf.iat[0,2]
                                                        surname = userdf.iat[0,3]
                                                        whatsapp = userdf.iat[0,4]
                                                        address = userdf.iat[0,6]
                                                        email = userdf.iat[0,5]
                                                        fullnamedisp = firstname + ' ' + surname
                                                        leaveapprovername = userdf.iat[0,8]
                                                        leaveapproverid = userdf.iat[0,9]
                                                        leaveapproveremail = userdf.iat[0, 10]
                                                        leaveapproverwhatsapp = userdf.iat[0,11]
                                                        role = userdf.iat[0,7]
                                                        leavedaysbalance = userdf.iat[0,12]
                                                        department = userdf.iat[0,14] 
                                                        print('check')

                                                        departmentdf = df_employees[df_employees['Department'] == department].reset_index()
                                                        numberindepartment = len(departmentdf)
                                                        
                                                        startdatex = pd.Timestamp(startdate)
                                                        enddatex = pd.Timestamp(enddate)

                                                        leave_dates = pd.date_range(startdatex, enddatex)

                                                        query = f"""
                                                            SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate,
                                                                leaveenddate, leavedaysappliedfor, approvalstatus, statusdate,
                                                                leavedaysbalancebf, department
                                                            FROM {table_name_apps_approved}
                                                            WHERE department = %s;
                                                        """
                                                        cursor.execute(query, (department,))
                                                        rows = cursor.fetchall()

                                                        df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf","department"]) 

                                                        # Create daily impact report
                                                        impact_report = []

                                                        for date in leave_dates:

                                                            date = pd.Timestamp(date)

                                                            df_employeesappsapprovedcheck["leavestartdate"] = pd.to_datetime(df_employeesappsapprovedcheck["leavestartdate"])
                                                            df_employeesappsapprovedcheck["leaveenddate"] = pd.to_datetime(df_employeesappsapprovedcheck["leaveenddate"])

                                                            print(type(date))  # Should be pandas._libs.tslibs.timestamps.Timestamp or datetime.datetime
                                                            print(df_employeesappsapprovedcheck.dtypes)  # Check all datetime columns

                                                            on_leave = ((df_employeesappsapprovedcheck["leavestartdate"] <= date) & (df_employeesappsapprovedcheck["leaveenddate"] >= date)).sum()
                                                            remaining = numberindepartment - on_leave - 1  # subtract 1 for the new leave
                                                            impact_report.append({
                                                                "date": date.strftime("%Y-%m-%d"),
                                                                "on leave": on_leave + 1,
                                                                "employees remaining": remaining
                                                            })

                                                        # Convert to DataFrame for display
                                                        impact_df = pd.DataFrame(impact_report)
                                                        print("IMPAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACT")
                                                        print(impact_df)
                                                        print(numberindepartment)

                                                        impact_df["date"] = pd.to_datetime(impact_df["date"], dayfirst=True)
                                                        impact_df = impact_df[impact_df["date"].dt.weekday != 6].copy()

                                                        impact_df["group"] = (impact_df[["on leave", "employees remaining"]] != impact_df[["on leave", "employees remaining"]].shift()).any(axis=1).cumsum()

                                                        statements = []
                                                        for _, group_df in impact_df.groupby("group"):
                                                            start = group_df["date"].iloc[0].strftime("%d %B %Y")
                                                            end = group_df["date"].iloc[-1].strftime("%d %B %Y")
                                                            on_leave = group_df["on leave"].iloc[0]
                                                            remaining = group_df["employees remaining"].iloc[0]
                                                            
                                                            if start == end:
                                                                statements.append(f"On {start}, the {department} department will have {remaining} employee(s) remaining at work and {on_leave} employee(s) on leave.")
                                                            else:
                                                                statements.append(f"From {start} to {end}, the {department} department will have {remaining} employee(s) remaining at work and {on_leave} employee(s) on leave.")
                                                                # Combine all statements into a single variable
                                                        final_summary = "\n".join(statements)
                                                        # Print output
                                                        for s in statements:
                                                            print(s)

                                                        leavedaysbalancebf = int(leavedaysbalance) - int(business_days)

                                                        status = "Pending"

                                                        insert_query = f"""
                                                        INSERT INTO {table_name_apps_pending_approval} (id, firstname, surname, department, leavetype, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus)
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                        """
                                                        cursor.execute(insert_query, (int(np.int64(id_user)), first_name, last_name, department, leavetype, leaveapprovername, int(np.int64(leaveapproverid)), leaveapproveremail, int(np.int64(leaveapproverwhatsapp)), int(np.int64(leavedaysbalance)), today_date, startdate, enddate, int(np.int64(business_days)), int(np.int64(leavedaysbalancebf)), status))
                                                        connection.commit()

                                                        query = f"SELECT appid FROM {table_name_apps_pending_approval};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()

                                                        df_employees = pd.DataFrame(rows, columns=["id"])
                                                        leaveappid = df_employees.iat[0,0]
                                                        companyxx = company_reg.replace("_"," ").title()
                                                        approovvver = leaveapprovername.title()

                                                        send_whatsapp_message(sender_id, f"✅ Great News {first_name} from {companyxx}! \n\n Your `{leavetype} Leave Application` for `{business_days} days` from `{startdate.strftime('%d %B %Y')}` to `{enddate.strftime('%d %B %Y')}` has been submitted successfully!\n\n"
                                                            f"Your Leave Application ID is `{leaveappid}`.\n\n"
                                                            f"A Notification has been sent to `{approovvver}`  on `+263{leaveapproverwhatsapp}` to decide on  your application.\n\n"
                                                            "To Check the approval status of your leave application, type `Hello` then select `Track Application`.")
                                                        
                                                        if leaveapproverwhatsapp:
            
                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": f"Approve5appwa_{leaveappid}", "title": "Approve"}},
                                                                {"type": "reply", "reply": {"id": f"Disapproveappwa_{leaveappid}", "title": "Disapprove"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                f"263{leaveapproverwhatsapp}", 
                                                                f"Hey {approovvver}! 😊. New `{leavetype}` Leave Application from `{first_name} {surname}` for `{business_days} days` from `{startdate.strftime('%d %B %Y')}` to `{enddate.strftime('%d %B %Y')}`.\n\n" 
                                                                f"If you approve this leave application, {final_summary}\n\n"  
                                                                f"Select an option below to either approve or disapprove the application."         
                                                                , 
                                                                buttons
                                                            )

                                                    else:
                                                        print("leave app submission failed")

                                                except ValueError as e:
                                                    send_whatsapp_message(
                                                        sender_id,
                                                        f"{e}, ❌ No, incorrect message format. Please use:\n"
                                                        "`end 24 january 2025`\n"
                                                        "Example: `end 15 march 2024`"
                                                    )

                                            elif button_id == "Checkbal":

                                                buttons = [
                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                {"type": "reply", "reply": {"id": "Track", "title": "Track Application"}},
                                                ]

                                                send_whatsapp_message(
                                                    sender_id, 
                                                    f"Hey {first_name}, your current available leave days balance is `{days_days_balance} days`.\n\n"
                                                    "Select an option below to continue 👇",
                                                    buttons
                                                )

                                            elif button_id == "Resubmitapp" :

                                                table_name_apps_cancelled = f"{company_reg}appscancelled"
                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"

                                                query = f"SELECT appid, id, firstname, surname, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail , leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf FROM {table_name_apps_cancelled} WHERE id = %s;"
                                                cursor.execute(query, (id_user,))
                                                result = cursor.fetchall()

                                                if result:

                                                    df_employees = pd.DataFrame(result)
                                                    df_employees = df_employees.sort_values(by=df_employees.columns[0], ascending=False)
                                                    print(df_employees)
                                                            
                                                    try:

                                                        status = "Pending"
                                                        app_id = int(np.int64(df_employees.iat[0,0]))
                                                        employee_number = int(np.int64(df_employees.iat[0,1]))
                                                        first_name = df_employees.iat[0,2]
                                                        surname = df_employees.iat[0,3]
                                                        leave_type = df_employees.iat[0,4]
                                                        leave_specify = df_employees.iat[0,5]
                                                        approver_name = df_employees.iat[0,6]
                                                        approver_id =  int(np.int64(df_employees.iat[0,7]))
                                                        approver_email =  df_employees.iat[0,8]
                                                        approver_whatsapp =  int(np.int64(df_employees.iat[0,9]))
                                                        leave_days_balance =  int(np.int64(df_employees.iat[0,10]))
                                                        date_applied = df_employees.iat[0,11]
                                                        start_date = df_employees.iat[0,12]
                                                        end_date = df_employees.iat[0,13]
                                                        leave_days =  int(np.int64(df_employees.iat[0,14]))
                                                        leavedaysbalancebf =  int(np.int64(df_employees.iat[0,15]))
                                                        insert_query = f"""
                                                        INSERT INTO {table_name_apps_pending_approval} 
                                                        (appid, id, firstname, surname, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus)
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                        """

                                                        cursor.execute(insert_query, (
                                                            app_id, employee_number, first_name, surname, leave_type, leave_specify, approver_name, 
                                                            approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                                            end_date, leave_days, leavedaysbalancebf, status
                                                        ))
                                                        
                                                        connection.commit()
                                                        print("Insert successful!")

                                                    except Exception as e:
                                                        print("Error inserting data:", e)

                                                    # SQL query to delete or mark the leave as canceled
                                                    query = f"""DELETE FROM {table_name_apps_cancelled} WHERE appid = %s"""
                                                    cursor.execute(query, (app_id,))
                                                    connection.commit()                                       

                                                    companyxx = company_reg.replace("_", " ").title()
                                                    sections = [
                                                        {
                                                            "title": "Administrator Options",
                                                            "rows": [
                                                                {"id": "Apply", "title": "Apply for Leave"},
                                                                {"id": "Track", "title": "Track My Application"},
                                                                {"id": "Checkbal", "title": "Check Days Balance"},
                                                                {"id": "Pending", "title": "Apps Pending My Approval"},
                                                                {"id": "Template", "title": "Add Employees"},
                                                                {"id": "Rolechange", "title": "Change Employee's Role"},
                                                                {"id": "Book", "title": "Extract Leave Book"}
                                                            ]
                                                        }
                                                    ]

                                                    send_whatsapp_list_message(sender_id, f"Hey {first_name} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}` has been Re-Submitted for approval successfully✅!",
                                                    "Administrator Options",
                                                    sections)                                          
                                                
                                                else:
                                                    print("No record found for the user.")

                                            elif "appwa" in button_id.lower():

                                                app_id = button_id.split("_")[1]
                                                print(app_id)

                                                if "approve5" in button_id.lower():

                                                    try:
                                                       
                                                        print ("eissssssssshhhhhhhhhhhhhhhhhhhhhhhhhhhh")

                                                        table_name = company_reg + 'main'
                                                        company_name = company_reg.replace("_", " ").title()
                                                        table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                        table_name_apps_approved = f"{company_reg}appsapproved"

                                                        if not app_id:
                                                            print("none on appid")

                                                            return jsonify({"message": "Application ID is missing."}), 400

                                                        status = "Approved"
                                                        statusdate = today_date
                                                        print("bababababababababa")
                                                        print(table_name_apps_pending_approval)

                                                        query = f"SELECT * FROM {table_name_apps_pending_approval} WHERE appid = %s;"
                                                        cursor.execute(query, (app_id,))
                                                        result = cursor.fetchone()
                                                        app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf, statuspre = result
                                                        print("chiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                                                        print(employee_number)
                                                        print(approver_name)

                                                        try:
                                                            insert_query = f"""
                                                            INSERT INTO {table_name_apps_approved} 
                                                            (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                            """
                                                            
                                                            cursor.execute(insert_query, (
                                                                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                                                                approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                                                end_date, leave_days, leavedaysbalancebf, status, statusdate
                                                            ))
                                                            
                                                            connection.commit()
                                                            print("Insert successful!")

                                                            query = f"UPDATE {table_name} SET currentleavedaysbalance = %s WHERE id = %s;"
                                                            cursor.execute(query, (leavedaysbalancebf, employee_number))
                                                            connection.commit()

                                                        except Exception as e:
                                                            print("Error inserting data:", e)

                                                        query = f"""DELETE FROM {table_name_apps_pending_approval} WHERE appid = %s"""
                                                        cursor.execute(query, (app_id,))
                                                        connection.commit()

                                                        query = f"SELECT id, firstname, surname, whatsapp, email, address, role, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, monthlyaccumulation, department FROM {table_name};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()

                                                        df_employees = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Address", "Role","Leave Approver Name","Leave Approver ID","Leave Approver Email", "Leave Approver WhatsAapp", "Leave Days Balance","Days Accumulated per Month", "Department"])
                                                        print(df_employees)
                                                        userdf = df_employees[df_employees['id'] == int(np.int64(employee_number))].reset_index()
                                                        print("yeaarrrrr")
                                                        print(userdf)
                                                        firstname = userdf.iat[0,2].title()
                                                        surname = userdf.iat[0,3].title()
                                                        whatsappemp = userdf.iat[0,4]
                                                        email = userdf.iat[0,5]
                                                        address = userdf.iat[0,6]
                                                        companyxx = company_name.replace("_", " ").title()
                                                        app_namexx = approver_name.title()

                                                        query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf, department  FROM {table_name_apps_approved} WHERE id = {str(employee_number)};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()
                                                        df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf", "department"]) 

                                                        df_employeesappsapprovedcheck = df_employeesappsapprovedcheck.sort_values(by="appid", ascending=False)  

                                                        def generate_leave_pdf():
                                                            app = {
                                                                'company_logo': 44,
                                                                'company_name': companyxx,
                                                                'employee_name': f"{first_name} {surname}",
                                                                'leave_type': leave_type,
                                                                'generated_on': today_date,
                                                                'date_applied': df_employeesappsapprovedcheck.iat[0,4].strftime('%d %B %Y'),
                                                                'approver_name': df_employeesappsapprovedcheck.iat[0,3].title(),
                                                                'reference_number': df_employeesappsapprovedcheck.iat[0,0],
                                                                'approved_date': df_employeesappsapprovedcheck.iat[0,9].strftime('%d %B %Y'),
                                                                'new_balance': df_employeesappsapprovedcheck.iat[0,10],
                                                                'start_date':  df_employeesappsapprovedcheck.iat[0,5].strftime('%d %B %Y'),
                                                                'end_date':  df_employeesappsapprovedcheck.iat[0,6].strftime('%d %B %Y'),
                                                                'days_requested':  df_employeesappsapprovedcheck.iat[0,7], 
                                                                'department':  department, 
                                                                'address': address, 
                                                                'whatsapp': f"+263{whatsappemp}", 
                                                                'email': email, 
                                                                'status': 'Approved'
                                                            }

                                                            html_out = render_template("leave_pdf_template.html", app=app)
                                                            
                                                            # ✅ Return as bytes instead of saving to file
                                                            pdf_bytes = HTML(string=html_out).write_pdf()
                                                            return pdf_bytes

                                                        
                                                        global ACCESS_TOKEN
                                                        global PHONE_NUMBER_ID

                                                        def upload_pdf_to_whatsapp(pdf_bytes):
                                                            filename=f"leave_application_{df_employeesappsapprovedcheck.iat[0,0]}_{first_name}_{surname}_{companyxx}.pdf"
                                                        
                                                            url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/media"
                                                            headers = {
                                                                "Authorization": f"Bearer {ACCESS_TOKEN}"
                                                            }

                                                            files = {
                                                                "file": (filename, io.BytesIO(pdf_bytes), "application/pdf"),
                                                                "type": (None, "application/pdf"),
                                                                "messaging_product": (None, "whatsapp")
                                                            }

                                                            response = requests.post(url, headers=headers, files=files)
                                                            print("📥 Full incoming data:", response.text)  # Good for debugging
                                                            response.raise_for_status()
                                                            return response.json()["id"]

                                                                                                        
                                                        def send_whatsapp_pdf_by_media_id(recipient_number, media_id):
                                                            filename=f"leave_application_{df_employeesappsapprovedcheck.iat[0,0]}_{first_name}_{surname}_{companyxx}.pdf"
                                                            url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
                                                            headers = {
                                                                "Authorization": f"Bearer {ACCESS_TOKEN}",
                                                                "Content-Type": "application/json"
                                                            }
                                                            payload = {
                                                                "messaging_product": "whatsapp",
                                                                "to": recipient_number,
                                                                "type": "document",
                                                                "document": {
                                                                    "id": media_id,            # Media ID from upload step
                                                                    "filename": filename       # Desired file name on recipient's phone
                                                                }
                                                            }

                                                            response = requests.post(url, headers=headers, json=payload)
                                                            response.raise_for_status()
                                                            return response.json()


                                                        pdf_path = generate_leave_pdf()
                                                        media_id = upload_pdf_to_whatsapp(pdf_path)

                                                        buttonsapproval = [
                                                            {"type": "reply", "reply": {"id": "Revoke", "title": "Revoke Approval"}},
                                                            {"type": "reply", "reply": {"id": "Pending", "title": "Pending My Approval"}},
                                                        ]

                                                        send_whatsapp_message(sender_id, f"✅ Great News {approver_name} from {companyxx}! \n\n You have successfully approved `{first_name} {surname}`'s  `{leave_days} day` `{leave_type} Leave Application` running from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}`✅!")
                                                        send_whatsapp_pdf_by_media_id(sender_id, media_id)
                                                        send_whatsapp_message(
                                                            sender_id,
                                                            "Select an option below to continue 👇, or Type `Hello` to view all Administrator/Approver Options",
                                                            buttonsapproval
                                                        )

                                                        if whatsappemp:

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Revoke", "title": "Revoke Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]

                                                            send_whatsapp_message(f"263{whatsappemp}", f"✅ Great News {first_name} {surname} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}`, has been Approved ✅ by `{app_namexx}`!")
                                                            send_whatsapp_pdf_by_media_id(f"263{whatsappemp}", media_id)
                                                            send_whatsapp_message(
                                                                f"263{whatsappemp}",
                                                                "Select an option below to continue 👇, or Type `Hello` to view all User Options",
                                                                buttons
                                                            )
                                                    
                                                    except Exception as e:
                                                        return jsonify({"message": "Error approving leave application.", "error": str(e)}), 500

                                                elif "disapprove" in button_id.lower():

                                                    print("disapproved")

                                                    try:
                                                       
                                                        print ("eissssssssshhhhhhhhhhhhhhhhhhhhhhhhhhhh")

                                                        table_name = company_reg + 'main'
                                                        company_name = company_reg.replace("_", " ").title()
                                                        table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                        table_name_apps_approved = f"{company_reg}appsapproved"
                                                        table_name_apps_declined = f"{company_reg}appsdeclined"


                                                        if not app_id:
                                                            print("none on appid")

                                                            return jsonify({"message": "Application ID is missing."}), 400

                                                        status = "Disapproved"
                                                        statusdate = today_date
                                                        print("bababababababababa")
                                                        print(table_name_apps_pending_approval)

                                                        query = f"SELECT * FROM {table_name_apps_pending_approval} WHERE appid = %s;"
                                                        cursor.execute(query, (app_id,))
                                                        result = cursor.fetchone()
                                                        app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf, statuspre = result
                                                        print("chiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                                                        print(employee_number)
                                                        print(approver_name)

                                                        try:
                                                            insert_query = f"""
                                                            INSERT INTO {table_name_apps_declined} 
                                                            (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                            """
                                                            
                                                            cursor.execute(insert_query, (
                                                                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                                                                approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                                                end_date, leave_days, leavedaysbalancebf, status, statusdate
                                                            ))
                                                            
                                                            connection.commit()
                                                            print("Insert successful!")

                                                        except Exception as e:
                                                            print("Error inserting data:", e)

                                                        query = f"""DELETE FROM {table_name_apps_pending_approval} WHERE appid = %s"""
                                                        cursor.execute(query, (app_id,))
                                                        connection.commit()

                                                        query = f"SELECT id, firstname, surname, whatsapp, email, address, role, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, monthlyaccumulation, department FROM {table_name};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()

                                                        df_employees = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Address", "Role","Leave Approver Name","Leave Approver ID","Leave Approver Email", "Leave Approver WhatsAapp", "Leave Days Balance","Days Accumulated per Month", "Department"])
                                                        print(df_employees)
                                                        userdf = df_employees[df_employees['id'] == int(np.int64(employee_number))].reset_index()
                                                        print("yeaarrrrr")
                                                        print(userdf)
                                                        firstname = userdf.iat[0,2].title()
                                                        surname = userdf.iat[0,3].title()
                                                        whatsappemp = userdf.iat[0,4]
                                                        email = userdf.iat[0,5]
                                                        address = userdf.iat[0,6]
                                                        companyxx = company_name.replace("_", " ").title()
                                                        app_namexx = approver_name.title()

                                                        query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf  FROM {table_name_apps_approved} WHERE id = {str(employee_number)};"
                                                        cursor.execute(query)
                                                        rows = cursor.fetchall()
                                                        df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf"]) 

                                                        df_employeesappsapprovedcheck["dateapplied"] = pd.to_datetime(df_employeesappsapprovedcheck["dateapplied"], errors='coerce')
                                                        df_employeesappsapprovedcheck = df_employeesappsapprovedcheck.sort_values(by="dateapplied", ascending=False)
                                                        
                                                        global ACCESS_TOKEN
                                                        global PHONE_NUMBER_ID

                                                        buttonsapproval = [
                                                            {"type": "reply", "reply": {"id": "Revokedis", "title": "Revoke Disapproval"}},
                                                            {"type": "reply", "reply": {"id": "Pending", "title": "Pending My Approval"}},
                                                        ]

                                                        send_whatsapp_message(sender_id, f"✅ Hey {approver_name} from {companyxx}! \n\n You have successfully disapproved `{first_name} {surname}`'s  `{leave_days} day` `{leave_type} Leave Application` running from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}`✅!")
                                                        send_whatsapp_message(
                                                            sender_id,
                                                            "Select an option below to continue 👇y, or Type `Hello` to view all Approver options",
                                                            buttonsapproval
                                                        )

                                                        if whatsappemp:

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Reapply", "title": "Resubmit Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]

                                                            send_whatsapp_message(f"263{whatsappemp}", f"✅ Oops, {first_name} {surname} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}`, has been disapproved ❌ by `{app_namexx}`!")
                                                            send_whatsapp_message(
                                                                f"263{whatsappemp}",
                                                                "Select an option below to continue 👇",
                                                                buttons
                                                            )


                                                    except Exception as e:
                                                        print(e)
                                                        return jsonify({"message": "Error approving leave application.", "error": str(e)}), 500


                                                else:
                                                    pass
                                                
                                            elif button_id == "Cancelapp" :

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                table_name_apps_cancelled = f"{company_reg}appscancelled"

                                                query = f"SELECT appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail , leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf FROM {table_name_apps_pending_approval} WHERE id = %s;"
                                                cursor.execute(query, (id_user,))
                                                result = cursor.fetchone()
                                                if result:
                                                    (app_id, employee_number, first_name, surname, department, leave_type,  leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf) = result
                                                
                                                    try:
                                                            status = "Cancelled"
                                                            statusdate = today_date
                                                        
                                                            insert_query = f"""
                                                            INSERT INTO {table_name_apps_cancelled} 
                                                            (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                            """

                                                            cursor.execute(insert_query, (
                                                                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                                                                approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                                                end_date, leave_days, leavedaysbalancebf, status, statusdate
                                                            ))
                                                            
                                                            connection.commit()
                                                            print("Insert successful!")

                                                    except Exception as e:
                                                        print("Error inserting data:", e)

                                                    # SQL query to delete or mark the leave as canceled
                                                    query = f"""DELETE FROM {table_name_apps_pending_approval} WHERE appid = %s"""
                                                    cursor.execute(query, (app_id,))
                                                    connection.commit()                                       

                                                    companyxx = company_reg.replace("_", " ").title()
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                        {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                        {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                    ]

                                                    send_whatsapp_message(sender_id, f"Hey {first_name} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}` has been Cancelled successfully✅!\n\n"
                                                        "Select an option below to continue 👇",
                                                        buttons
                                                    )                                          
                                                
                                                else:
                                                    print("No record found for the user.")


                                        
                                        if interactive.get("type") == "list_reply":
                                            selected_option = interactive.get("list_reply", {}).get("id")
                                            print(f"📋 User selected: {selected_option}")

                                            if selected_option == "Apply":

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"

                                                query = f"SELECT id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor  FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor"])    

                                                if len(df_employeesappspendingcheck) == 0:

                                                        sections = [
                                                            {
                                                                "title": "Leave Type Options",
                                                                "rows": [
                                                                    {"id": "Annual", "title": "Annual Leave"},
                                                                    {"id": "Sick", "title": "Sick Leave"},
                                                                    {"id": "Study", "title": "Study Leave"},
                                                                    {"id": "Bereavement", "title": "Bereavement Leave"},
                                                                    {"id": "Parental", "title": "Parental Leave"},
                                                                    {"id": "Other", "title": "Other"},
                                                                ]
                                                            }
                                                        ]

                                                        send_whatsapp_list_message(
                                                            sender_id, 
                                                            f"{first_name}, kindly select the type of Leave that you are applying for.", 
                                                            "Leave Type Options",
                                                            sections) 

                                                elif len(df_employeesappspendingcheck) > 0:
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Reminder", "title": "Remind Approver"}},
                                                        {"type": "reply", "reply": {"id": "Cancelapp", "title": "Cancel Pending App"}},
                                                    ]
                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"Oops! 🥲. Sorry {first_name}, you cannot apply for leave whilst you have another leave application which is still pending approval.\n\n" 
                                                        f"Your `{df_employeesappspendingcheck.iat[0,1]}` Leave Application `[ID - {df_employeesappspendingcheck.iat[0,0]}]` applied on `{df_employeesappspendingcheck.iat[0,3].strftime('%d %B %Y')}` for `{df_employeesappspendingcheck.iat[0,6]} days from {df_employeesappspendingcheck.iat[0,4].strftime('%d %B %Y')} to {df_employeesappspendingcheck.iat[0,5].strftime('%d %B %Y')}` is still pending approval from {df_employeesappspendingcheck.iat[0,2]}.\n\n" 
                                                        f"Select an option below to either remind the approver to approved your pending application or you can cancel the pending application to submit a new leave application."         
                                                        , 
                                                        buttons
                                                    )

                                            elif selected_option in ["Annual","Sick","Study","Parental", "Bereavement","Other"] :
                                                button_id_leave_type = str(selected_option)

                                                cursor.execute("""
                                                    DELETE FROM whatsapptempapplication
                                                    WHERE empidwa = %s
                                                """, (str(id_user),))  
                                                
                                                connection.commit()

                                                cursor.execute(f"""
                                                    INSERT INTO whatsapptempapplication (empidwa, leavetypewa, companynamewa)
                                                    VALUES (%s, %s, %s)
                                                """, (id_user, button_id_leave_type, company_reg))

                                                connection.commit()

                                                send_whatsapp_message(
                                                    sender_id, 
                                                    f"Ok. When would you like your {selected_option} Leave to start {first_name}?\n\n"
                                                    "Please enter your response using the format: 👇🏻\n"
                                                    "`start 24 january 2025`"
                                                )

                                                continue

                                            elif selected_option == "Book":
                                                
                                                table_name = f"{company_reg}main"
                                                appsapproved = f"{company_reg}appsapproved"

                                                query = f"SELECT id, firstname, surname, whatsapp, email, address ,role,currentleavedaysbalance, monthlyaccumulation, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp  FROM {table_name};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employees = pd.DataFrame(rows, columns=["ID","First Name", "Surname", "WhatsApp","Email", "Address", "Role","Leave Days Balance","Days Accumulated per Month","Leave Approver Name", "Leave Approver ID", "Leave Approver Email", "Leave Approver WhatsaApp"])
                                                df_employees = df_employees.sort_values(by="ID", ascending=True)

                                                query = f"SELECT appid, id, firstname, surname, leavetype, leaveapprovername, TO_CHAR(dateapplied, 'FMDD-Month-YYYY') AS dateapplied,  TO_CHAR(leavestartdate, 'FMDD Month YYYY') AS leavestartdate,   TO_CHAR(leaveenddate, 'FMDD Month YYYY') AS leaveenddate, leavedaysappliedfor,   TO_CHAR(statusdate, 'FMDD Month YYYY') AS statusdate, leavedaysbalancebf  FROM {appsapproved};"
                                                cursor.execute(query)
                                                rows2 = cursor.fetchall()

                                                df_apps = pd.DataFrame(rows2, columns=["AppID","Emp ID", "First Name", "Surname", "Leave Type","Leave Approver Name", "Date Applied", "Leave Start Date", "Leave End Date","Leave Days Applied for","Date Approved","Leave Days Balance"])
                                                df_apps = df_apps.sort_values(by="AppID", ascending=False)




                                                print(df_employees)


                                                df_apps['Leave Start Date'] = pd.to_datetime(df_apps['Leave Start Date'])
                                                df_apps['Leave End Date'] = pd.to_datetime(df_apps['Leave End Date'])

                                                # Function to expand dates and exclude Sundays
                                                def expand_leave_days(row):
                                                    dates = pd.date_range(row['Leave Start Date'], row['Leave End Date'], freq='D')
                                                    # Exclude Sundays (weekday=6)
                                                    dates = [d for d in dates if d.weekday() != 6]
                                                    return dates

                                                # Apply the function and explode the DataFrame
                                                df_apps['Leave Dates'] = df_apps.apply(expand_leave_days, axis=1)
                                                df_exploded = df_apps.explode('Leave Dates')

                                                # Extract month and year for grouping
                                                df_exploded['Month'] = df_exploded['Leave Dates'].dt.to_period('M')

                                                # Group by Employee and Month
                                                result = df_exploded.groupby(['Emp ID', 'First Name', 'Surname', 'Month']).size().reset_index(name='Leave Days Taken')

                                                # Pivot to MoM format (months as columns)
                                                mom_leave = result.pivot_table(
                                                    index=['Emp ID', 'First Name', 'Surname'],
                                                    columns='Month',
                                                    values='Leave Days Taken',
                                                    fill_value=0
                                                ).reset_index()

                                                # Rename columns for clarity
                                                mom_leave.columns.name = None
                                                mom_leave.columns = ['Emp ID', 'First Name', 'Surname'] + [f"{col.strftime('%b-%Y')}" for col in mom_leave.columns[3:]]

                                                print(mom_leave)

                                                def upload_excel_to_whatsapp(excel_bytes, company_reg, first_name, last_name, reference_number=None):
                                                    """Uploads an Excel file to WhatsApp servers and returns the media ID"""
                                                    compxxy = company_reg.replace("_"," ").title()
                                                    
                                                    filename = f"leave_records_{compxxy}.xlsx"
                                                    
                                                    url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/media"
                                                    headers = {
                                                        "Authorization": f"Bearer {ACCESS_TOKEN}"
                                                    }

                                                    files = {
                                                        "file": (filename, io.BytesIO(excel_bytes), 
                                                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                                                        "type": (None, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                                                        "messaging_product": (None, "whatsapp")
                                                    }

                                                    response = requests.post(url, headers=headers, files=files)
                                                    print("📊 Excel upload response:", response.text)  # Debugging
                                                    response.raise_for_status()
                                                    return response.json()["id"]

                                                def send_whatsapp_excel_by_media_id(recipient_number, media_id, company_reg, first_name, last_name, reference_number=None, caption=None):
                                                    """Sends an Excel file via WhatsApp using the uploaded media ID"""
                                                    compxxy = company_reg.replace("_"," ").title()
                                                    
                                                    filename = f"leave_records_{compxxy}.xlsx"
                                                    
                                                    url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
                                                    headers = {
                                                        "Authorization": f"Bearer {ACCESS_TOKEN}",
                                                        "Content-Type": "application/json"
                                                    }
                                                    
                                                    payload = {
                                                        "messaging_product": "whatsapp",
                                                        "to": recipient_number,
                                                        "type": "document",
                                                        "document": {
                                                            "id": media_id,
                                                            "filename": filename
                                                        }
                                                    }
                                                    
                                                    if caption:
                                                        payload["document"]["caption"] = caption

                                                    response = requests.post(url, headers=headers, json=payload)
                                                    response.raise_for_status()
                                                    return response.json()

                                                output = BytesIO()
                                                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                                                    df_employees.to_excel(writer, index=False, sheet_name=f'LMS Book {today_date}')
                                                    df_apps.to_excel(writer, index=False, sheet_name=f'All Approved')
                                                    mom_leave.to_excel(writer, index=False, sheet_name=f'Month on Month')

                                                output.seek(0)
                                                excel_bytes = output.getvalue()
                                                
                                                try:
                                                    media_id = upload_excel_to_whatsapp(
                                                        excel_bytes=excel_bytes,
                                                        company_reg=company_reg,
                                                        first_name=first_name,
                                                        last_name=last_name
                                                    )
                                                    
                                                    send_whatsapp_excel_by_media_id(
                                                        recipient_number=sender_id,
                                                        media_id=media_id,
                                                        company_reg=company_reg,
                                                        first_name=first_name,
                                                        last_name=last_name,
                                                        caption=f"Employee Leave Records as of {today_date}"
                                                    )
                                                    
                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"Excel file with leave records has been sent, {first_name}.\n\n"
                                                        "Send `hello` to see your LMS Options."
                                                    )

                                                except Exception as e:
                                                    print(f"Error sending Excel file: {str(e)}")
                                                    send_whatsapp_message(
                                                        sender_id,
                                                        f"Sorry {first_name}, we encountered an error preparing your document. Please try again later."
                                                    )

                                            elif selected_option == "Pending":

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"

                                                query = f"SELECT id, leavetype, firstname, surname, leaveapprovername, leaveapproverid, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, appid  FROM {table_name_apps_pending_approval} WHERE leaveapproverid = {str(id_user)};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id", "leavetype", "firstname", "surname", "leaveapprovername", "leaveapproverid", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor", "appid"])    
                                                df_employeesappspendingcheck = df_employeesappspendingcheck.sort_values(by=df_employeesappspendingcheck.columns[10], ascending=False)

                                                if len(df_employeesappspendingcheck) == 0:

                                                    companyxx = company_reg.replace("_", " ").title()
                                                    sections = [
                                                        {
                                                            "title": "Administrator/Approver Options",
                                                            "rows": [
                                                                {"id": "Apply", "title": "Apply for Leave"},
                                                                {"id": "Track", "title": "Track My Application"},
                                                                {"id": "Checkbal", "title": "Check Days Balance"},
                                                                {"id": "Pending", "title": "Apps Pending My Approval"},
                                                                {"id": "Template", "title": "Add Employees"},
                                                                {"id": "Rolechange", "title": "Change Employee's Role"},
                                                                {"id": "Book", "title": "Extract Leave Book"}
                                                            ]
                                                        }
                                                    ]
    
                                                    send_whatsapp_list_message(
                                                        sender_id, 
                                                        f"{first_name}, there are currently no leave applications that are pending your approval.", 
                                                    "Administrator/Approver Options",
                                                    sections) 

                                                elif len(df_employeesappspendingcheck) > 0:

                                                    firstnameemp2 = df_employeesappspendingcheck.iat[0,2]
                                                    appid = df_employeesappspendingcheck.iat[0,10]
                                                    surnameemp2 = df_employeesappspendingcheck.iat[0,3]
                                                    leave_type2 = df_employeesappspendingcheck.iat[0,1]
                                                    days = df_employeesappspendingcheck.iat[0,9]
                                                    date_applied2 = df_employeesappspendingcheck.iat[0,6]
                                                    start_date2 = df_employeesappspendingcheck.iat[0,7]
                                                    end_date2 = df_employeesappspendingcheck.iat[0,8]

                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": f"Approve5appwa_{appid}", "title": "Approve"}},
                                                        {"type": "reply", "reply": {"id": f"Disapproveappwa_{appid}", "title": "Disapprove"}},
                                                    ]

                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"{firstnameemp2} {surnameemp2}'s {days} day {leave_type2} Leave Application, applied on {date_applied2.strftime('%d %B %Y')} and running from {start_date2.strftime('%d %B %Y')} to {end_date2.strftime('%d %B %Y')} is pending your Approval.\n\n" 
                                                        "Select an option below to either approve or disapprove this leave application.", 
                                                        buttons
                                                    )
                                                
                                            elif selected_option == "Track":

                                                table_name_apps_pending_approval = f"{company_reg}appspendingapproval"
                                                table_name_apps_approved = f"{company_reg}appsapproved"
                                                table_name_apps_declined = f"{company_reg}appsdeclined"
                                                table_name_apps_cancelled = f"{company_reg}appscancelled"


                                                query = f"SELECT id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leaveapproverwhatsapp, appid, department  FROM {table_name_apps_pending_approval} WHERE id = {str(id_user)};"
                                                cursor.execute(query)
                                                rows = cursor.fetchall()

                                                df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor", "leaveapproverwhatsapp", "appid", "department"])    

                                                if len(df_employeesappspendingcheck) == 0:

                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf, department  FROM {table_name_apps_approved} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf", "department"]) 

                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf, department  FROM {table_name_apps_declined} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappsdeclinedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf", "department"])  
                            
                                                    query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf, department  FROM {table_name_apps_cancelled} WHERE id = {str(id_user)};"
                                                    cursor.execute(query)
                                                    rows = cursor.fetchall()
                                                    df_employeesappscancelledcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf", "department"])
                            
                                                    all_approved_declined = df_employeesappsapprovedcheck._append(df_employeesappsdeclinedcheck)
                                                    all_approved_declined_cancelled = all_approved_declined._append(df_employeesappscancelledcheck)
                                                    all_approved_declined_cancelled = all_approved_declined_cancelled.sort_values(by="appid", ascending=False) 

                                                    if len(all_approved_declined_cancelled) > 0:

                                                        print(f" hhhhhhhhhhhhhhhhhhhh  {all_approved_declined_cancelled.iat[0,8] }")

                                                        if all_approved_declined_cancelled.iat[0,8] == "Approved":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Revoke", "title": "Revoke Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, your recent `{all_approved_declined_cancelled.iat[0,2]}` Leave Application `[ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}` was {all_approved_declined_cancelled.iat[0,8]}✅ by `{all_approved_declined_cancelled.iat[0,3].title()}` on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}`." 
                                                            )


                                                            def generate_leave_pdf():
                                                                app = {
                                                                    'company_logo': 44,
                                                                    'company_name': company_reg.replace("_"," ").title(),
                                                                    'employee_name': f"{first_name} {last_name}",
                                                                    'leave_type': all_approved_declined_cancelled.iat[0,2],
                                                                    'generated_on': today_date,
                                                                    'date_applied': all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y'),
                                                                    'approver_name': all_approved_declined_cancelled.iat[0,3].title(),
                                                                    'reference_number': all_approved_declined_cancelled.iat[0,0],
                                                                    'approved_date': all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y'),
                                                                    'new_balance': all_approved_declined_cancelled.iat[0,10],
                                                                    'start_date':  all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y'),
                                                                    'end_date':  all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y'),
                                                                    'days_requested':  all_approved_declined_cancelled.iat[0,7], 
                                                                    'department': all_approved_declined_cancelled.iat[0,11],
                                                                    'address': address_foc_8, 
                                                                    'whatsapp': whatsapp_foc_8, 
                                                                    'email': email_foc_8, 
                                                                    'status': 'Approved'
                                                                }

                                                                html_out = render_template("leave_pdf_template.html", app=app)
                                                                
                                                                # ✅ Return as bytes instead of saving to file
                                                                pdf_bytes = HTML(string=html_out).write_pdf()
                                                                return pdf_bytes

                                                            
                                                            global ACCESS_TOKEN
                                                            global PHONE_NUMBER_ID

                                                            def upload_pdf_to_whatsapp(pdf_bytes):
                                                                compxxy = company_reg.replace("_"," ").title()
                                                                filename=f"leave_application_{all_approved_declined_cancelled.iat[0,0]}_{first_name}_{last_name}_{compxxy}.pdf"
                                                            
                                                                url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/media"
                                                                headers = {
                                                                    "Authorization": f"Bearer {ACCESS_TOKEN}"
                                                                }

                                                                files = {
                                                                    "file": (filename, io.BytesIO(pdf_bytes), "application/pdf"),
                                                                    "type": (None, "application/pdf"),
                                                                    "messaging_product": (None, "whatsapp")
                                                                }

                                                                response = requests.post(url, headers=headers, files=files)
                                                                print("📥 Full incoming data:", response.text)  # Good for debugging
                                                                response.raise_for_status()
                                                                return response.json()["id"]

                                                                                                            
                                                            def send_whatsapp_pdf_by_media_id(recipient_number, media_id):
                                                                compxxy = company_reg.replace("_"," ").title()
                                                                filename=f"leave_application_{all_approved_declined_cancelled.iat[0,0]}_{first_name}_{last_name}_{compxxy}.pdf"
                                                                url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
                                                                headers = {
                                                                    "Authorization": f"Bearer {ACCESS_TOKEN}",
                                                                    "Content-Type": "application/json"
                                                                }
                                                                payload = {
                                                                    "messaging_product": "whatsapp",
                                                                    "to": recipient_number,
                                                                    "type": "document",
                                                                    "document": {
                                                                        "id": media_id,            # Media ID from upload step
                                                                        "filename": filename       # Desired file name on recipient's phone
                                                                    }
                                                                }

                                                                response = requests.post(url, headers=headers, json=payload)
                                                                response.raise_for_status()
                                                                return response.json()


                                                            pdf_path = generate_leave_pdf()
                                                            media_id = upload_pdf_to_whatsapp(pdf_path)
                                                            send_whatsapp_pdf_by_media_id(sender_id, media_id)

                                                            send_whatsapp_message(
                                                                sender_id,
                                                                "Select an option below to continue 👇",
                                                                buttons
                                                            )

                                                        elif all_approved_declined_cancelled.iat[0,8] == "Declined":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, your recent `{all_approved_declined_cancelled.iat[0,2]}` Leave Application `[ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}` was {all_approved_declined_cancelled.iat[0,8]}❌ by `{all_approved_declined_cancelled.iat[0,3].title()}` on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}`.",
                                                                buttons 
                                                            )

                                                        elif all_approved_declined_cancelled.iat[0,8] == "Cancelled":

                                                            buttons = [
                                                                {"type": "reply", "reply": {"id": "Resubmitapp", "title": "ReSubmit Application"}},
                                                                {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                                                                {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                                                            ]
                                                            send_whatsapp_message(
                                                                sender_id, 
                                                                f"Hey {first_name}, on `{all_approved_declined_cancelled.iat[0,9].strftime('%d %B %Y')}` you Cancelled ⛔ your recent `{all_approved_declined_cancelled.iat[0,2]} Leave Application [ID - {all_approved_declined_cancelled.iat[0,0]}]` that you applied for on `{all_approved_declined_cancelled.iat[0,4].strftime('%d %B %Y')}` for `{all_approved_declined_cancelled.iat[0,7]} days` from `{all_approved_declined_cancelled.iat[0,5].strftime('%d %B %Y')}` to `{all_approved_declined_cancelled.iat[0,6].strftime('%d %B %Y')}`.",
                                                                buttons 
                                                            )


                                                    else:

                                                        sections = [
                                                            {
                                                                "title": "Administrator Options",
                                                                "rows": [
                                                                    {"id": "Apply", "title": "Apply for Leave"},
                                                                    {"id": "Checkbal", "title": "Check Days Balance"},
                                                                    {"id": "Pending", "title": "Apps Pending My Approval"},
                                                                    {"id": "Template", "title": "Add Employees"},
                                                                    {"id": "Rolechange", "title": "Change Employee's Role"},
                                                                    {"id": "Book", "title": "Extract Leave Book"}
                                                                ]
                                                            }
                                                        ]
                                                        companyxx = company_reg.replace("_"," ").title()


                                                        send_whatsapp_list_message(
                                                            sender_id, 
                                                            f"Hello {first_name} {last_name}, LMS Leave Applications Approver from {companyxx}!\n\n You have not applied for any leave days yet.", 
                                                            "Administrator Options",
                                                            sections
                                                        )

                                                elif len(df_employeesappspendingcheck) > 0:
                                                    buttons = [
                                                        {"type": "reply", "reply": {"id": "Reminder", "title": "Remind Approver"}},
                                                        {"type": "reply", "reply": {"id": "Cancelapp", "title": "Cancel Pending App"}},
                                                    ]
                                                    approoooover = df_employeesappspendingcheck.iat[0,2].title()
                                                    send_whatsapp_message(
                                                        sender_id, 
                                                        f"Hey {first_name}, your recent `{df_employeesappspendingcheck.iat[0,1]}` Leave Application `[ID - {df_employeesappspendingcheck.iat[0,8]}]` applied on `{df_employeesappspendingcheck.iat[0,3].strftime('%d %B %Y')}` for `{df_employeesappspendingcheck.iat[0,6]} days from {df_employeesappspendingcheck.iat[0,4].strftime('%d %B %Y')} to {df_employeesappspendingcheck.iat[0,5].strftime('%d %B %Y')}` is still pending approval from `{approoooover}`.\n\n" 
                                                        f"Select an option below to either remind `{approoooover}` to approve your pending leave application or you can cancel the pending application to submit a new leave application."         
                                                        , 
                                                        buttons
                                                    )

                                                
                                            elif selected_option == "Checkbal":

                                                sections = [
                                                    {
                                                        "title": "Administrator Options",
                                                        "rows": [
                                                            {"id": "Apply", "title": "Apply for Leave"},
                                                            {"id": "Track", "title": "Track My Application"},
                                                            {"id": "Checkbal", "title": "Check Days Balance"},
                                                            {"id": "Pending", "title": "Apps Pending My Approval"},
                                                            {"id": "Template", "title": "Add Employees"},
                                                            {"id": "Rolechange", "title": "Change Employee's Role"},
                                                            {"id": "Book", "title": "Extract Leave Book"}
                                                        ]
                                                    }
                                                ]

                                                send_whatsapp_list_message(
                                                    sender_id, 
                                                    f"Hey {first_name}, your current available leave days balance is `{days_days_balance} days`.",
                                                    "Administrator Options",
                                                    sections
                                                )
                                                
                                            elif selected_option == "Pending":




                                                
                                                # Handle Apps Pending My Approval
                                                pass
                                                
                                            elif selected_option == "Template":
                                                # Handle Add Employees
                                                pass
                                                
                                            elif selected_option == "Rolechange":
                                                # Handle Change Employee's Role
                                                pass
                                                
                                            elif selected_option == "Book":
                                                # Handle Extract Leave Book
                                                pass
                                                





                                    elif message.get("type") == "text":

                                        text = message.get("text", {}).get("body", "").lower()
                                        print(f"📨 Message from {sender_id}: {text}")
                                        
                                        if "hello" in text.lower():
                                            companyxx = company_reg.replace("_"," ").title()
                                            
                                            sections = [
                                                {
                                                    "title": "Administrator Options",
                                                    "rows": [
                                                        {"id": "Apply", "title": "Apply for Leave"},
                                                        {"id": "Track", "title": "Track My Application"},
                                                        {"id": "Checkbal", "title": "Check Days Balance"},
                                                        {"id": "Pending", "title": "Apps Pending My Approval"},
                                                        {"id": "Template", "title": "Add Employees"},
                                                        {"id": "Rolechange", "title": "Change Employee's Role"},
                                                        {"id": "Book", "title": "Extract Leave Book"}
                                                    ]
                                                }
                                            ]
                                            
                                            send_whatsapp_list_message(
                                                sender_id,
                                                f"Hello {first_name} {last_name}, LMS Administrator & Leave Applications Approver from {companyxx}!\n\nEchelon Bot Here 😎. How can I assist you?",
                                                "Administrator/Approver Options",
                                                sections
                                            )

                                        elif "start" in text.lower():

                                            date_part = text.split("start", 1)[1].strip()

                                            cursor.execute("""
                                                UPDATE whatsapptempapplication
                                                SET startdate = %s
                                                WHERE empidwa = %s
                                            """, (date_part, id_user))

                                            connection.commit()

                                            cursor.execute("""
                                                SELECT empidwa, leavetypewa FROM whatsapptempapplication
                                                WHERE empidwa = %s
                                            """, (str(id_user)))
                                    
                                            result = cursor.fetchone()

                                            if result:
                                                leavetypewa = result[1] 

                                            cursor.execute("SELECT * FROM whatsapptempapplication")
                                            columns = [desc[0] for desc in cursor.description]
                                            records = cursor.fetchall()
                                            
                                            df = pd.DataFrame(records, columns=columns)
                                            
                                            print("\n📊 whatsapptempapplication Table:")
                                            print(df)
                                            
                                            try:
                                                parsed_date = datetime.strptime(date_part, "%d %B %Y")
                                                send_whatsapp_message(sender_id, "✅ Yes! Valid start date format.\n\n"
                                                    f"Now Enter the last day that you will be on {leavetypewa} Leave.Use the format: 👇🏻\n"
                                                    "`end 24 january 2025`"                      
                                                                    )
                                                
                                            except ValueError:
                                                send_whatsapp_message(
                                                    sender_id,
                                                    f"❌ No, incorrect message format, {first_name}. Please use:\n"
                                                    "`start 24 january 2025`\n"
                                                    "Example: `start 15 march 2024`"
                                                )

                                        elif "end" in text.lower():

                                            date_part = text.split("end", 1)[1].strip()

                                            cursor.execute("""
                                                UPDATE whatsapptempapplication
                                                SET enddate = %s
                                                WHERE empidwa = %s
                                            """, (date_part, id_user))

                                            connection.commit()

                                            cursor.execute("""
                                                SELECT id ,empidwa, leavetypewa, startdate, enddate FROM whatsapptempapplication
                                                WHERE empidwa = %s
                                            """, (str(id_user)))
                                    
                                            result = cursor.fetchone()

                                            appid = result[0]
                                            leavetype = result[2]
                                            startdate = result[3]
                                            enddate = result[4]

                                            if isinstance(startdate, str):
                                                startdate = datetime.datetime.strptime(startdate, "%Y-%m-%d").date()
                                            if isinstance(enddate, str):
                                                enddate = datetime.datetime.strptime(enddate, "%Y-%m-%d").date()

                                            business_days = 0
                                            current_date = startdate

                                            while current_date <= enddate:
                                                if current_date.weekday() < 5:  # 0=Mon, 1=Tue, ..., 4=Fri
                                                    business_days += 1
                                                current_date += timedelta(days=1)  # Use timedelta directly

                                            print(f"📅 Business days between {startdate} and {enddate}: {business_days}")


                                            buttons = [
                                                {"type": "reply", "reply": {"id": "Submitapp", "title": "Yes, Submit"}},
                                                {"type": "reply", "reply": {"id": "Dontsubmit", "title": "No"}}
                                            ]
                                            send_whatsapp_message(
                                                sender_id, 
                                                f"Do you wish to submit your `{business_days} day {leavetype} Leave Application` leave starting from `{startdate.strftime('%d %B %Y')}` to `{enddate.strftime('%d %B %Y')}` {first_name} ?", 
                                                buttons
                                            )

                                        else:
                                            send_whatsapp_message(
                                                sender_id, 
                                                "Echelon Bot Here 😎. Say 'hello' to start!"
                                            )










        return jsonify({"status": "received"}), 200
    

    return jsonify({"status": "received"}), 200


def delete_all_tables():
    try:
        
        # Get all table names in the public schema
        cursor.execute("""
            SELECT tablename FROM pg_tables WHERE schemaname = 'public';
        """)
        tables = cursor.fetchall()

        # Loop through and drop each table
        for table in tables:
            table_name = table[0]
            cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            print(f"Dropped table: {table_name}")
            connection.commit()

        # Close the cursor and connection
        print("All tables deleted successfully.")

    except Exception as e:
        print("Error:", e)

# Run the function

def find_credentials(email, password):
    connection.reconnect()
    cursor = connection.cursor()
    
    try:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            query = f"""
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = 'LMSuniversal_guarddoing'
            AND table_name = '{table_name}'
            AND column_name IN ('email', 'password')
            """
            cursor.execute(query)
            if cursor.fetchone()[0] == 2:  

                query = f"""
                SELECT * FROM {table_name}
                WHERE email = %s AND password = %s
                """
                cursor.execute(query, (email, password))
                result = cursor.fetchone()
                if result:
                    print(f"Credentials found in table: {table_name}")
                    return result  

    except: 
        print("No matching credentials found in any table.")
        return None

def run1(table_name, empid):
    print(empid)

    query = f"SELECT id, firstname, surname, whatsapp, email, address, role, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, monthlyaccumulation, department FROM {table_name};"
    cursor.execute(query)
    rows = cursor.fetchall()

    df_employees = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Address", "Role","Leave Approver Name","Leave Approver ID","Leave Approver Email", "Leave Approver WhatsAapp", "Leave Days Balance","Days Accumulated per Month","Department"])
    print(df_employees)
    userdf = df_employees[df_employees['id'] == empid].reset_index()
    print("yeaarrrrr")
    print(userdf)
    firstname = userdf.iat[0,2]
    surname = userdf.iat[0,3]
    whatsapp = userdf.iat[0,4]
    address = userdf.iat[0,6]
    email = userdf.iat[0,5]
    fullnamedisp = firstname + ' ' + surname
    leaveapprovername = userdf.iat[0,8]
    leaveapproverid = userdf.iat[0,9]
    leaveapproveremail = userdf.iat[0, 10]
    leaveapproverwhatsapp = userdf.iat[0,11]
    role = userdf.iat[0,7]
    leavedaysbalance = userdf.iat[0,12]
    department = userdf.iat[0,14]

    print('check')

    df_employees['Employee Name'] = df_employees['firstname'] + ' ' + df_employees['surname']

    df_employees['Action'] = df_employees['id'].apply(
        lambda x: f'''<div style="display: flex; gap: 10px;font-size: 12px;"> <button class="btn btn-primary3 edit-priv-btn" data-bs-toggle="modal" data-bs-target="#editModalpriv" data-name="{x}"  data-ID="{x}">Edit Role</button> <button class="btn btn-primary3 edit-department-btn" data-bs-toggle="modal" data-bs-target="#editModaldepartment" data-name="{x}"  data-ID="{x}">Change Department</button>  <button class="btn btn-primary3 change-approver-btn" data-bs-toggle="modal" data-bs-target="#editModalapprover" data-name="{x}" data-ID="{x}">Change Approver</button>  <button class="btn btn-primary3 edit-balance-btn" data-bs-toggle="modal" data-bs-target="#editModalbalance" data-name="{x}" data-ID="{x}">Edit Balance</button> </div>'''
    )

    selected_columns = df_employees[['id','Employee Name', "Role", "Department", "Leave Approver Name", "Leave Days Balance", "Action"]]
    selected_columns.columns = ['ID','EMPLOYEE NAME','ROLE','DEPARTMENT','APPROVER','DAYS BALANCE','ACTION']

    table_employees_html = selected_columns.to_html(classes="table table-bordered table-theme", table_id="employeesTable", index=False,  escape=False,)

    selected_columns['Combined'] = selected_columns.apply(
        lambda row: f"{row['ID']}--{row['EMPLOYEE NAME']}", axis=1
    )

    employees_list = selected_columns['Combined'].tolist()

    selected_columns_accumulators = df_employees[['id','Employee Name', "Days Accumulated per Month"]]
    selected_columns_accumulators.columns = ['ID','EMPLOYEE NAME','DAYS ACCUMULATED PER MONTH']
    selected_columns_accumulators.loc[:, 'LEAVE DAYS ACCUMULATED PER MONTH'] = selected_columns_accumulators.apply(
        lambda row: f'<input type="number" class="editable-field" value="{row["DAYS ACCUMULATED PER MONTH"] if row["DAYS ACCUMULATED PER MONTH"] is not None else 0}" data-id="{row["ID"]}" style="width: 100%;"/>',
        axis=1
    )

    seacc = selected_columns_accumulators[['ID','EMPLOYEE NAME','LEAVE DAYS ACCUMULATED PER MONTH']]

    table_employees_accumulators_html = seacc.to_html(classes="table table-bordered table-theme", table_id="employeesaccumulatorsTable", index=False,  escape=False,)

    rememployees = selected_columns_accumulators[['ID','EMPLOYEE NAME']]
    rememployees.loc[:, 'SELECTION'] = rememployees.apply(
        lambda row: f'<input type="checkbox" class="custom-checkbox employee-checkbox" name="employee_ids" value="{row["ID"]}" data-employee-name="{row["EMPLOYEE NAME"]}">',
        axis=1
    )

    table_rememployees_html = rememployees.to_html(classes="table table-bordered table-theme", table_id="removeemployeesTable", index=False,  escape=False,)

    rememployees1 = selected_columns_accumulators[['ID','EMPLOYEE NAME']]
    rememployees1.loc[:, 'SELECTION'] = rememployees1.apply(
        lambda row: f'<input type="checkbox" class="custom-checkbox employee-checkbox-bulk-approver" name="employee_ids" value="{row["ID"]}" data-employee-name="{row["EMPLOYEE NAME"]}">',
        axis=1
    )
    table_rememployees_bulk1_html = rememployees1.to_html(classes="table table-bordered table-theme", table_id="employeesbulk1Table", index=False,  escape=False,)

    rememployees2 = selected_columns_accumulators[['ID','EMPLOYEE NAME']]
    rememployees2.loc[:, 'SELECTION'] = rememployees2.apply(
        lambda row: f'<input type="checkbox" class="custom-checkbox employee-checkbox-bulk-balances" name="employee_ids" value="{row["ID"]}" data-employee-name="{row["EMPLOYEE NAME"]}">',
        axis=1
    )
    table_rememployees_bulk_balances_html = rememployees2.to_html(classes="table table-bordered table-theme", table_id="employeesbulkbalancesTable", index=False,  escape=False,)

    rememployees3 = selected_columns_accumulators[['ID','EMPLOYEE NAME']]
    rememployees3.loc[:, 'SELECTION'] = rememployees3.apply(
        lambda row: f'<input type="checkbox" class="custom-checkbox employee-checkbox-bulk-accumulators" name="employee_ids" value="{row["ID"]}" data-employee-name="{row["EMPLOYEE NAME"]}">',
        axis=1
    )
    table_rememployees_bulk_accumulators_html = rememployees3.to_html(classes="table table-bordered table-theme", table_id="bulkemployeesbulkaccumulatorsTable", index=False,  escape=False,)

    company_name = table_name.replace("main", "")
    table_name_apps_pending_approval = f"{company_name}appspendingapproval"
    table_name_apps_approved = f"{company_name}appsapproved"
    table_name_apps_declined = f"{company_name}appsdeclined"
    table_name_apps_cancelled = f"{company_name}appscancelled"


    query = f"""SELECT appid, id, firstname, surname, leavetype, TO_CHAR(dateapplied, 'FMDD Month YYYY') AS dateapplied, TO_CHAR(leavestartdate, 'FMDD Month YYYY') AS leavestartdate, TO_CHAR(leaveenddate, 'FMDD Month YYYY') AS leaveenddate,  leavedaysappliedfor, leaveapprovername, approvalstatus FROM {table_name_apps_pending_approval};"""
    cursor.execute(query)
    rows = cursor.fetchall()
    df_leave_appsmain_pending_approval = pd.DataFrame(rows, columns=["App ID","ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver","Approval Status"])
    df_leave_appsmain_pending_approval['Approval Status'] = '<p style="color: #ffab00; border: 3px solid #ffab00;border-radius: 9px;display: inline-block; margin: 0;padding: 0px 8px;">Pending</p>'
    df_leave_appsmain_pending_approvalcomb = df_leave_appsmain_pending_approval[["App ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver","Approval Status"]]

    print(df_leave_appsmain_pending_approval)


    query = f"""SELECT appid, id, firstname, surname, leavetype, TO_CHAR(dateapplied, 'FMDD Month YYYY') AS dateapplied, TO_CHAR(leavestartdate, 'FMDD Month YYYY') AS leavestartdate, TO_CHAR(leaveenddate, 'FMDD Month YYYY') AS leaveenddate,  leavedaysappliedfor, leaveapprovername, approvalstatus FROM {table_name_apps_approved};"""
    cursor.execute(query)
    rows = cursor.fetchall()
    df_leave_appsmain_approved = pd.DataFrame(rows, columns=["App ID","ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver","Approval Status"])
    df_leave_appsmain_approved['Approval Status'] = '<p style="color: #28a745; border: 3px solid #28a745;border-radius: 9px;display: inline-block; margin: 0;padding: 0px 8px;">Approved</p>'
    df_leave_appsmain_approved['ACTION'] = df_leave_appsmain_approved['App ID'].apply(lambda x: f'''<div style="display: flex; gap: 10px;"><button class="btn btn-primary3 download-app-btn" data-ID="{x}" onclick="downloadLeaveApp('{x}')">Download</button></div>''')
    df_leave_appsmain_approvedcomb = df_leave_appsmain_approved[["App ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver","Approval Status","ACTION"]]

    query = f"""SELECT appid, id, firstname, surname, leavetype, TO_CHAR(dateapplied, 'FMDD Month YYYY') AS dateapplied, TO_CHAR(leavestartdate, 'FMDD Month YYYY') AS leavestartdate, TO_CHAR(leaveenddate, 'FMDD Month YYYY') AS leaveenddate,  leavedaysappliedfor, leaveapprovername, approvalstatus FROM {table_name_apps_declined};"""
    cursor.execute(query)
    rows = cursor.fetchall()
    df_leave_appsmain_declined = pd.DataFrame(rows, columns=["App ID","ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver","Approval Status"])
    df_leave_appsmain_declined['Approval Status'] = '<p style="color: #E30022; border: 3px solid #E30022;border-radius: 9px;display: inline-block; margin: 0;padding: 0px 8px;">Declined</p>'
    df_leave_appsmain_declinedcomb = df_leave_appsmain_declined[["App ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver","Approval Status"]]

    query = f"""SELECT appid, id, firstname, surname, leavetype, TO_CHAR(dateapplied, 'FMDD Month YYYY') AS dateapplied, TO_CHAR(leavestartdate, 'FMDD Month YYYY') AS leavestartdate, TO_CHAR(leaveenddate, 'FMDD Month YYYY') AS leaveenddate,  leavedaysappliedfor, leaveapprovername, approvalstatus FROM {table_name_apps_cancelled};"""
    cursor.execute(query)
    rows = cursor.fetchall()
    df_leave_appsmain_cancelled = pd.DataFrame(rows, columns=["App ID","ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver","Approval Status"])
    df_leave_appsmain_cancelled['Approval Status'] = '<p style="color: #E30022; border: 3px solid #E30022;border-radius: 9px;display: inline-block; margin: 0;padding: 0px 8px;">Cancelled</p>'
    df_leave_appsmain_cancelledcomb = df_leave_appsmain_cancelled[["App ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver","Approval Status"]]


    df_leave_appsmain1 = df_leave_appsmain_pending_approvalcomb._append(df_leave_appsmain_approvedcomb)
    df_leave_appsmain3 = df_leave_appsmain1._append(df_leave_appsmain_declinedcomb)
    df_leave_appsmain = df_leave_appsmain3._append(df_leave_appsmain_cancelledcomb)
    df_leave_appsmain['Employee Name'] = df_leave_appsmain['First Name'] + ' ' + df_leave_appsmain['Surname']
    df_leave_appsmain = df_leave_appsmain[["App ID","Employee Name", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver","Approval Status","ACTION"]].fillna('')

    table_leave_apps_html = df_leave_appsmain.to_html(classes="table table-bordered table-theme", table_id="leaveappsTable", index=False,  escape=False,)

    query = f"""SELECT appid, id, firstname, surname, leavetype, TO_CHAR(dateapplied, 'FMDD Month YYYY') AS dateapplied, TO_CHAR(leavestartdate, 'FMDD Month YYYY') AS leavestartdate, TO_CHAR(leaveenddate, 'FMDD Month YYYY') AS leaveenddate,  leavedaysappliedfor FROM {table_name_apps_pending_approval} WHERE leaveapproverid = {empid};"""
    cursor.execute(query)
    rows = cursor.fetchall()
    df_leave_apps_pending_my_approval = pd.DataFrame(rows, columns=["App ID","ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days"])
    num_pending_my_approval = len(df_leave_apps_pending_my_approval)
    df_leave_apps_pending_my_approval['ACTION'] = df_leave_apps_pending_my_approval['App ID'].apply(lambda x: f'''<div style="display: flex; gap: 10px;"> <button class="btn btn-primary3 approve-app-btn" data-bs-toggle="modal" data-bs-target="#approveappModal" data-name="{x}" data-ID="{x}">Approve</button> <button class="btn btn-primary3 disapprove-app-btn" data-bs-toggle="modal" data-bs-target="#disapproveappModal" data-name="{x}" data-ID="{x}">Disapprove</button> </div>''') 
    df_leave_apps_pending_my_approval['Approval Status'] = '<p style="color: #ffab00; border: 3px solid #ffab00;border-radius: 9px;display: inline-block; margin: 0;padding: 0px 8px;">Pending</p>'    
    df_leave_apps_pending_my_approval_fin = df_leave_apps_pending_my_approval[["App ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days", "Approval Status","ACTION"]]
    

    query = f"""SELECT appid, id, firstname, surname, leavetype, TO_CHAR(dateapplied, 'FMDD Month YYYY') AS dateapplied, TO_CHAR(leavestartdate, 'FMDD Month YYYY') AS leavestartdate, TO_CHAR(leaveenddate, 'FMDD Month YYYY') AS leaveenddate,  leavedaysappliedfor FROM {table_name_apps_approved} WHERE leaveapproverid = {empid};"""
    cursor.execute(query)
    rows = cursor.fetchall()
    df_leave_apps_approved_by_me= pd.DataFrame(rows, columns=["App ID","ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days"])
    df_leave_apps_approved_by_me['ACTION'] = df_leave_apps_approved_by_me['App ID'].apply(lambda x: f'''<div style="display: flex; gap: 10px;"> <button class="btn btn-primary3 download-app-btn" data-name="{x}" data-ID="{x}" onclick="downloadLeaveApp('{x}')">Download</button><button class="btn btn-primary3 revokeapprover-app-btn" data-bs-toggle="modal" data-bs-target="#revokeapproverappModal" data-name="{x}" data-ID="{x}">Revoke</button></div>''') 
    df_leave_apps_approved_by_me['Approval Status'] = '<p style="color: #28a745; border: 3px solid #28a745;border-radius: 9px;display: inline-block; margin: 0;padding: 0px 8px;">Approved</p>'
    df_leave_apps_approved_by_me = df_leave_apps_approved_by_me[["App ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days", "Approval Status","ACTION"]]

    query = f"""SELECT appid, id, firstname, surname, leavetype, TO_CHAR(dateapplied, 'FMDD Month YYYY') AS dateapplied, TO_CHAR(leavestartdate, 'FMDD Month YYYY') AS leavestartdate, TO_CHAR(leaveenddate, 'FMDD Month YYYY') AS leaveenddate,  leavedaysappliedfor FROM {table_name_apps_declined} WHERE leaveapproverid = {empid};"""
    cursor.execute(query)
    rows = cursor.fetchall()
    df_leave_apps_declined_by_me= pd.DataFrame(rows, columns=["App ID","ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days"])
    df_leave_apps_declined_by_me['ACTION'] = df_leave_apps_declined_by_me['App ID'].apply(lambda x: f'''<div style="display: flex; gap: 10px;"> <button class="btn btn-primary3 revive-app-btn" data-bs-toggle="modal" data-bs-target="#reviveappModal" data-name="{x}" data-ID="{x}">Revive</button></div>''') 
    df_leave_apps_declined_by_me['Approval Status'] = '<p style="color: #E30022; border: 3px solid #E30022;border-radius: 9px;display: inline-block; margin: 0;padding: 0px 8px;">Declined</p>'
    df_leave_apps_declined_by_me = df_leave_apps_declined_by_me[["App ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days", "Approval Status","ACTION"]]

    df_leave_apps_approved_declined_by_me = df_leave_apps_approved_by_me._append(df_leave_apps_declined_by_me)
    df_leave_apps_approved_declined_pending_by_me = df_leave_apps_approved_declined_by_me._append(df_leave_apps_pending_my_approval_fin).fillna('')   
    df_leave_apps_approved_declined_pending_by_me['Employee Name'] = df_leave_apps_approved_declined_pending_by_me['First Name'] + ' ' + df_leave_apps_approved_declined_pending_by_me['Surname']
    df_leave_apps_approved_declined_pending_by_me = df_leave_apps_approved_declined_pending_by_me[["App ID","Employee Name", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days", "Approval Status","ACTION"]]

    table_leave_apps_approved_by_me_html = df_leave_apps_approved_declined_pending_by_me.to_html(classes="table table-bordered table-theme", table_id="leaveappsTableapprovedbyme", index=False,  escape=False,)
 
    query = f"""SELECT id, appid, leavetype, TO_CHAR(dateapplied, 'FMDD Month YYYY') AS dateapplied, TO_CHAR(leavestartdate, 'FMDD Month YYYY') AS leavestartdate, TO_CHAR(leaveenddate, 'FMDD Month YYYY') AS leaveenddate,  leavedaysappliedfor, leaveapprovername FROM {table_name_apps_approved};"""
    cursor.execute(query)
    rows = cursor.fetchall()
    df_my_leave_apps_approved = pd.DataFrame(rows, columns=["ID","App ID", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver"])
    df_my_leave_apps_approved = df_my_leave_apps_approved[df_my_leave_apps_approved['ID'] == empid]
    print("ahhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
    print(df_my_leave_apps_approved)
    df_my_leave_apps_approved['Approval Status'] = '<p style="color: #28a745; border: 3px solid #28a745;border-radius: 9px;display: inline-block; margin: 0;padding: 0px 8px;">Approved</p>'
    df_my_leave_apps_approved['ACTION'] = df_my_leave_apps_approved['App ID'].apply(lambda x: f'''<div style="display: flex; gap: 10px;"><button class="btn btn-primary3 download-app-btn" data-ID="{x}" onclick="downloadLeaveApp('{x}')">Download</button><button class="btn btn-primary3 revoke-app-btn" data-bs-toggle="modal" data-bs-target="#revokeappModal" data-name="{x}" data-ID="{x}">Revoke</button></div>''')
    df_my_leave_apps_approved_approved_fin =  df_my_leave_apps_approved[["App ID", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Approval Status","Leave Approver","ACTION"]]

    query = f"""SELECT id, appid, leavetype, TO_CHAR(dateapplied, 'FMDD Month YYYY') AS dateapplied, TO_CHAR(leavestartdate, 'FMDD Month YYYY') AS leavestartdate, TO_CHAR(leaveenddate, 'FMDD Month YYYY') AS leaveenddate,  leavedaysappliedfor, leaveapprovername FROM {table_name_apps_declined};"""
    cursor.execute(query)
    rows = cursor.fetchall()
    df_my_leave_apps_declined = pd.DataFrame(rows, columns=["ID","App ID", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver"])
    df_my_leave_apps_declined = df_my_leave_apps_declined[df_my_leave_apps_declined['ID'] == empid]
    print(df_my_leave_apps_declined)
    df_my_leave_apps_declined['Approval Status'] = '<p style="color: #E30022; border: 3px solid #E30022;border-radius: 9px;display: inline-block; margin: 0;padding: 0px 8px;">Declined</p>'
    df_my_leave_apps_declined['ACTION'] =  df_my_leave_apps_declined['App ID'].apply(lambda x: f'''<div style="display: flex; gap: 10px;"> <button class="btn btn-primary3 reapply-app-btn" data-bs-toggle="modal" data-bs-target="#reapplyappModal" data-name="{x}" data-ID="{x}">Re-Apply</button>''') 
    df_my_leave_apps_declined_fin_declined =  df_my_leave_apps_declined[["App ID", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Approval Status","Leave Approver","ACTION"]]

    query = f"""SELECT id, appid, leavetype, TO_CHAR(dateapplied, 'FMDD Month YYYY') AS dateapplied, TO_CHAR(leavestartdate, 'FMDD Month YYYY') AS leavestartdate, TO_CHAR(leaveenddate, 'FMDD Month YYYY') AS leaveenddate,  leavedaysappliedfor, leaveapprovername FROM {table_name_apps_cancelled};"""
    cursor.execute(query)
    rows = cursor.fetchall()
    df_my_leave_apps_cancelled = pd.DataFrame(rows, columns=["ID","App ID", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver"])
    df_my_leave_apps_cancelled = df_my_leave_apps_cancelled[df_my_leave_apps_cancelled['ID'] == empid]
    print("ahhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
    print(df_my_leave_apps_cancelled)
    df_my_leave_apps_cancelled['Approval Status'] = '<p style="color: #E30022; border: 3px solid #E30022;border-radius: 9px;display: inline-block; margin: 0;padding: 0px 8px;">Cancelled</p>'
    df_my_leave_apps_cancelled['ACTION'] =  df_my_leave_apps_cancelled['App ID'].apply(lambda x: f'''<div style="display: flex; gap: 10px;"> <button class="btn btn-primary3 reapply-app-btn" data-bs-toggle="modal" data-bs-target="#reapplyappModal" data-name="{x}" data-ID="{x}">Re-Apply</button>''') 
    df_my_leave_apps_cancelled_fin =  df_my_leave_apps_cancelled[["App ID", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Approval Status","Leave Approver","ACTION"]]

    df_my_leave_apps_approved_declined_fin1 = df_my_leave_apps_approved_approved_fin._append(df_my_leave_apps_declined_fin_declined)
    df_my_leave_apps_approved_declined_fin = df_my_leave_apps_approved_declined_fin1._append(df_my_leave_apps_cancelled_fin)

    userleaveapppending = df_leave_appsmain_pending_approval[df_leave_appsmain_pending_approval['ID'] == empid].reset_index()
    userleaveapppending['ACTION'] = userleaveapppending['App ID'].apply(lambda x: f'''<div style="display: flex; gap: 10px;"> <button class="btn btn-primary3 edit-priv-btn" data-bs-toggle="modal" data-bs-target="#remindapproverModal" data-name="{x}" data-ID="{x}">Remind Approver</button> <button class="btn btn-primary3 cancel-app-btn" data-bs-toggle="modal" data-bs-target="#cancelappModal" data-name="{x}" data-ID="{x}">Cancel Application</button> </div>''') 
    userleaveapppending = userleaveapppending[["App ID","Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver","Approval Status", "ACTION"]]
    df_my_leave_apps_approved_declined_pending_fin = df_my_leave_apps_approved_declined_fin._append(userleaveapppending)

    table_my_leave_apps_html = df_my_leave_apps_approved_declined_pending_fin.to_html(classes="table table-bordered table-theme", table_id="myleaveappsTable", index=False,  escape=False,)

    def generate_leave_status_chart():

        def categorize_status(status):
            if "pending" in status.lower():
                return "Pending"
            elif "declined" in status.lower():
                return "Declined"
            elif "approved" in status.lower():
                return "Approved"
            elif "cancelled" in status.lower():
                return "Cancelled"
            return status  # Return unchanged if no match

        df_my_leave_apps_approved_declined_pending_fin["Approval Status"] = df_my_leave_apps_approved_declined_pending_fin["Approval Status"].apply(categorize_status)
        print("ragaaaaaaaaaaaaaaaa")
        print(df_my_leave_apps_approved_declined_pending_fin)
        
        status_counts = df_my_leave_apps_approved_declined_pending_fin["Approval Status"].value_counts().to_dict()
        return status_counts  # Return as dictionary
    
    return {
        "table_my_leave_apps_html": table_my_leave_apps_html,
        "table_leave_apps_approved_by_me_html": table_leave_apps_approved_by_me_html,
        "num_pending_my_approval": num_pending_my_approval,
        "table_leave_apps_html": table_leave_apps_html,
        "table_employees_accumulators_html": table_employees_accumulators_html,
        "table_rememployees_bulk_balances_html": table_rememployees_bulk_balances_html,
        "table_rememployees_bulk_accumulators_html":  table_rememployees_bulk_accumulators_html,
        "table_rememployees_html": table_rememployees_html,
        "table_rememployees_bulk1_html": table_rememployees_bulk1_html,
        "employees_list": employees_list,
        "role": role,
        "department": department,
        "firstname": firstname,
        "surname": surname,
        "fullnamedisp": fullnamedisp,
        "email": email,
        "whatsapp": whatsapp,
        "address": address,
        "table_employees_html": table_employees_html,
        "today_date": today_date,
        "leaveapprovername": leaveapprovername,
        "leaveapproverid": leaveapproverid,
        "leavedaysbalance": leavedaysbalance,
        "leaveapproveremail": leaveapproveremail,
        "leaveapproverwhatsapp": leaveapproverwhatsapp,
        "leave_status_chart": generate_leave_status_chart(),  

    }



def check_existing_data(df, table_name):
    # Query database for existing WhatsApp and Email values
    cursor.execute(f"SELECT whatsapp, email FROM {table_name}")
    existing_data = cursor.fetchall()

    existing_whatsapps = [data[0] for data in existing_data]
    existing_emails = [data[1] for data in existing_data]

    existing_whatsapps = [data[0] for data in existing_data]
    existing_emails = [data[1] for data in existing_data]

    # Drop rows where WhatsApp or Email already exists in the database
    df = df[~df['WhatsApp'].isin(existing_whatsapps)]  # Remove rows with existing WhatsApp
    df = df[~df['Email'].isin(existing_emails)]  # Remove rows with existing Email

    return df

app.config['ALLOWED_EXTENSIONS'] = {'xlsx'}

if connection.status == psycopg2.extensions.STATUS_READY:
    cursor = connection.cursor()
        
    def allowed_file(filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


    @app.route('/upload-excel', methods=['POST'])
    def upload_excel():

        user_uuid = session.get('user_uuid')
        if user_uuid:

            table_name = session.get('table_name')
            empid = session.get('empid')

            if 'file' not in request.files:
                return jsonify({"status": "error", "message": "No file part"}), 400
            
            file = request.files['file']
            
            if file.filename == '':
                return jsonify({"status": "error", "message": "No selected file"}), 400
            
            if file and allowed_file(file.filename):

                df = pd.read_excel(file, usecols=range(8))
                df = check_existing_data(df, table_name)

                print(df)

                if len(df) > 0:

                    for index, row in df.iterrows():
                        first_name = row['FirstName']
                        surname = row['Surname']
                        whatsapp = row['WhatsApp']
                        email = row['Email']
                        role = row['Role']
                        department = row['Department']
                        current_leave_days_balance = row['Current Leave Days Balance']
                        monthly_accumulation = row['Monthly Leave Days Accumulation']

                        cursor.execute(f"""
                            INSERT INTO {table_name} (firstname, surname, whatsapp, email, role, department, currentleavedaysbalance, monthlyaccumulation)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (first_name, surname, whatsapp, email, role, department, current_leave_days_balance, monthly_accumulation))
                    
                    connection.commit()

                return redirect(url_for('Dashboard'))

        else:
            return redirect(url_for('landingpage')) 



    @app.route('/download-excel-template-add-employees')
    def download_excel_template_add_employees():
        user_uuid = session.get('user_uuid')
        if not user_uuid:
            return redirect(url_for('landingpage'))

        table_name = session.get('table_name')
        company_name = table_name.replace("main", "")

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Employee Details"

        # Add headers
        headers = [
            "FirstName", "Surname", "WhatsApp", "Email", 
            "Role", "Department", 
            "Current Leave Days Balance", "Monthly Leave Days Accumulation"
        ]
        ws.append(headers)

        # Style headers
        dark_blue = "003366"
        white = "FFFFFF"
        for col in range(1, len(headers) + 1):
            cell = ws.cell(row=1, column=col)
            cell.fill = PatternFill(start_color=dark_blue, end_color=dark_blue, fill_type="solid")
            cell.font = Font(color=white, bold=True)

        # Role dropdown (inline, short list)
        role_dv = DataValidation(type="list", formula1='"Administrator,Ordinary User"', allow_blank=False)
        ws.add_data_validation(role_dv)
        for row in range(2, 500):
            role_dv.add(ws[f"E{row}"])

        # Department options (long list) - write to column Z
        departments = [
            "Human Resources and Administration",
            "Finance and Accounting",
            "Sales and Marketing",
            "Operations and Production",
            "Procurement and Purchasing",
            "Customer Service and Support",
            "IT and Digital Infrastructure",
            "Risk Management",
            "Legal and Compliance",
            "Health, Safety and Environment",
            "Research, Analytics and Reporting"
        ]

        for i, dept in enumerate(departments, start=1):
            ws[f"Z{i}"] = dept

        # Department dropdown using cell range
        dept_dv = DataValidation(type="list", formula1="=$Z$1:$Z$12", allow_blank=False)
        ws.add_data_validation(dept_dv)
        for row in range(2, 500):
            dept_dv.add(ws[f"F{row}"])

        # Hide the reference column
        ws.column_dimensions['Z'].hidden = True

        # Save workbook to memory stream
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        filename = f"{company_name.strip()}_Employee_Details_Template.xlsx"
        return send_file(output, download_name=filename, as_attachment=True)



    @app.route('/admin_sign_up', methods=['POST'])
    def submit_form():
        global password
        try:

            external_database_url = "postgresql://lmsdatabase_8ag3_user:6WD9lOnHkiU7utlUUjT88m4XgEYQMTLb@dpg-ctp9h0aj1k6c739h9di0-a.oregon-postgres.render.com/lmsdatabase_8ag3"
            database = 'lmsdatabase_8ag3'

            connection = psycopg2.connect(external_database_url)

            cursor = connection.cursor()

            company_name_w_space = request.form.get('company_name')
            company_name = company_name_w_space.replace(' ', '_')
            firstname = request.form.get('firstname')
            surname = request.form.get('surname')
            whatsapp = int(request.form['whatsapp'])
            email = request.form.get('email')
            password = request.form.get('password')

            table_name = f"{company_name}main"
            table_name_apps_pending_approval = f"{company_name}appspendingapproval"
            table_name_apps_cancelled = f"{company_name}appscancelled"
            table_name_apps_approved = f"{company_name}appsapproved"
            table_name_apps_declined = f"{company_name}appsdeclined"
            table_name_apps_revoked = f"{company_name}appsrevoked"

            check_table_query = f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s;
            """
            cursor.execute(check_table_query, (database, table_name))
            table_exists = cursor.fetchone()[0]

            if table_exists:
                print(f"Table `{table_name}` already exists. Skipping creation.")

                return render_template('index.html')  

            else:

                create_table_query = f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    firstname VARCHAR(100),
                    surname VARCHAR(100),
                    whatsapp INT,
                    address VARCHAR(100),
                    email VARCHAR(255),
                    password VARCHAR(255),
                    department VARCHAR(255),
                    role VARCHAR(255),
                    leaveapprovername VARCHAR(255),
                    leaveapproverid INT,
                    leaveapproveremail VARCHAR(255),
                    leaveapproverwhatsapp INT,
                    currentleavedaysbalance INT,
                    monthlyaccumulation INT
                );
                """
                cursor.execute(create_table_query)
                connection.commit()
                print(f"Table `{table_name}` created successfully!")


                create_table_query = f"""
                CREATE TABLE {table_name_apps_pending_approval} (
                    appid SERIAL PRIMARY KEY,
                    id INT,
                    firstname VARCHAR(100),
                    surname VARCHAR(100),
                    department VARCHAR(255),
                    leavetype VARCHAR(255),
                    reasonifother VARCHAR(300),
                    leaveapprovername VARCHAR(255),
                    leaveapproverid INT,
                    leaveapproveremail VARCHAR(255),
                    leaveapproverwhatsapp INT,
                    currentleavedaysbalance INT,
                    dateapplied date,
                    leavestartdate date,
                    leaveenddate date,
                    leavedaysappliedfor INT,
                    leavedaysbalancebf INT,
                    approvalstatus VARCHAR(255)
                );
                """
                cursor.execute(create_table_query)
                connection.commit()
                print(f"Table `{table_name_apps_pending_approval}` created successfully!")


                create_table_query = f"""
                CREATE TABLE {table_name_apps_cancelled} (
                    appid INT,
                    id INT,
                    firstname VARCHAR(100),
                    surname VARCHAR(100),
                    department VARCHAR(255),
                    leavetype VARCHAR(255),
                    reasonifother VARCHAR(300),
                    leaveapprovername VARCHAR(255),
                    leaveapproverid INT,
                    leaveapproveremail VARCHAR(255),
                    leaveapproverwhatsapp INT,
                    currentleavedaysbalance INT,
                    dateapplied date,
                    leavestartdate date,
                    leaveenddate date,
                    leavedaysappliedfor INT,
                    leavedaysbalancebf INT,
                    approvalstatus VARCHAR(255),
                    statusdate date
                );
                """
                cursor.execute(create_table_query)
                connection.commit()
                print(f"Table `{table_name_apps_cancelled}` created successfully!")




                create_table_query = f"""
                CREATE TABLE {table_name_apps_approved} (
                    appid INT,
                    id INT,
                    firstname VARCHAR(100),
                    surname VARCHAR(100),
                    department VARCHAR(255),
                    leavetype VARCHAR(255),
                    reasonifother VARCHAR(300),
                    leaveapprovername VARCHAR(255),
                    leaveapproverid INT,
                    leaveapproveremail VARCHAR(255),
                    leaveapproverwhatsapp INT,
                    currentleavedaysbalance INT,
                    dateapplied date,
                    leavestartdate date,
                    leaveenddate date,
                    leavedaysappliedfor INT,
                    leavedaysbalancebf INT,
                    approvalstatus VARCHAR(255),
                    statusdate date         
                );
                """
                cursor.execute(create_table_query)
                connection.commit()
                print(f"Table `{table_name_apps_approved}` created successfully!")


                create_table_query = f"""
                CREATE TABLE {table_name_apps_declined} (
                    appid INT,
                    id INT,
                    firstname VARCHAR(100),
                    surname VARCHAR(100),
                    department VARCHAR(255),
                    leavetype VARCHAR(255),
                    reasonifother VARCHAR(300),
                    leaveapprovername VARCHAR(255),
                    leaveapproverid INT,
                    leaveapproveremail VARCHAR(255),
                    leaveapproverwhatsapp INT,
                    currentleavedaysbalance INT,
                    dateapplied date,
                    leavestartdate date,
                    leaveenddate date,
                    leavedaysappliedfor INT,
                    leavedaysbalancebf INT,
                    approvalstatus VARCHAR(255),
                    statusdate date         
                );
                """
                cursor.execute(create_table_query)
                connection.commit()
                print(f"Table `{table_name_apps_declined}` created successfully!")


                create_table_query = f"""
                CREATE TABLE {table_name_apps_revoked} (
                    appid INT,
                    id INT,
                    firstname VARCHAR(100),
                    surname VARCHAR(100),
                    department VARCHAR(255),
                    leavetype VARCHAR(255),
                    reasonifother VARCHAR(300),
                    leaveapprovername VARCHAR(255),
                    leaveapproverid INT,
                    leaveapproveremail VARCHAR(255),
                    leaveapproverwhatsapp INT,
                    currentleavedaysbalance INT,
                    dateapplied date,
                    leavestartdate date,
                    leaveenddate date,
                    leavedaysappliedfor INT,
                    leavedaysbalancebf INT,
                    approvalstatus VARCHAR(255),
                    statusdate date         
                );
                """
                cursor.execute(create_table_query)
                connection.commit()
                print(f"Table `{table_name_apps_revoked}` created successfully!")
                admin = "Administrator"

                
                insert_query = f"""
                INSERT INTO {table_name} (firstname, surname, role, whatsapp, email, password)
                VALUES (%s, %s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, (firstname, surname, admin, whatsapp, email, password))
                connection.commit()
                print("First record inserted successfully!")
                    
                print(f"Received data for {company_name} - Administrator: {firstname} {surname} (Email: {email})")

                query = f"SELECT id, firstname, surname, whatsapp, email, role, leaveapprovername, currentleavedaysbalance, monthlyaccumulation FROM {table_name};"
                cursor.execute(query)
                rows = cursor.fetchall()

                df_employeesemp = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Role","Leave Approver Name","Leave Days Balance","Days Accumulated per Month"])
                userdfemp = df_employeesemp[df_employeesemp['Email'] == email].reset_index()
                firstname = userdfemp.iat[0,2]
                surname = userdfemp.iat[0,3]
                email = userdfemp.iat[0,5]
                empid = userdfemp.iat[0,1]

                print("new d")
                print(df_employeesemp)
                print(userdfemp)                

                user_uuid = uuid.uuid4()
                session['user_uuid'] = str(user_uuid)
                session.permanent = True  
                user_sessions[email] = {'uuid': str(user_uuid), 'email': email}
                session['table_name'] = table_name

                session['empid'] = int(np.int64(empid))  # Ensure Python int
                
                results = run1(table_name, empid)  # Replace with your actual table name

                return render_template('adminpage.html', **results, id= empid, company_name=company_name)
            
        except Error as e:
            print(f'failed {e}')
            return render_template('index.html')
        

    @app.route('/dashboard')
    def Dashboard():

        user_uuid = session.get('user_uuid')
        if user_uuid:

            table_name = session.get('table_name')
            empid = session.get('empid')

            companyname = table_name.replace("main", "")
            company_name = companyname.replace('_', ' ')

            results = run1(table_name, empid)  

            print("Back from adventures")
            if results["role"] == 'Administrator':
                role_narr = "LMS ADMINISTRATOR"

                return render_template('adminpage.html', **results, id= empid, company_name=company_name, role_narr = role_narr)

            if results["role"] == 'Ordinary User':

                query = f"SELECT id FROM {table_name} WHERE leaveapproverid = {empid};"
                cursor.execute(query)
                rows = cursor.fetchall()

                df_employeesempapp = pd.DataFrame(rows, columns=["id"])

                if len(df_employeesempapp) > 0:

                    role_narr = "LMS LEAVE APPLICATIONS APPROVER"
                    hide_element = True
                    return render_template('adminpage.html', **results, id= empid, company_name=company_name, hide_element=hide_element, role_narr = role_narr)

                elif len(df_employeesempapp) == 0:
                    
                    role_narr = "LMS USER"
                    hide_element = True
                    hide_element2 = True
                    return render_template('adminpage.html', **results, id= empid, company_name=company_name, hide_element=hide_element, hide_element2 = hide_element2, role_narr = role_narr)

        
        else:
                return redirect(url_for('landingpage')) 
        
    @app.route('/login', methods=['POST'])
    def login():
        if request.method == 'POST':
            try:
                # Database connection
                external_database_url = "postgresql://lmsdatabase_8ag3_user:6WD9lOnHkiU7utlUUjT88m4XgEYQMTLb@dpg-ctp9h0aj1k6c739h9di0-a.oregon-postgres.render.com/lmsdatabase_8ag3"
                connection = psycopg2.connect(external_database_url)
                cursor = connection.cursor()

                # Retrieve form data
                email = request.form.get('emaillogin').strip()
                password = request.form.get('passwordlogin').strip()

                # Check for missing input
                if not email or not password:
                    return jsonify({'success': False, 'message': 'Email and password are required.'}), 400
                
                if email == "iamgreat" and password == "011235813":

                    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
                    
                    tables = cursor.fetchall()
                    table_names = [table[0] for table in tables]

                    df_tables = pd.DataFrame(table_names, columns=['Table Name'])
          
                    return render_template('edslmsadmin.html')


                # Query tables with the 'email' column
                column_search_query = """
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE COLUMN_NAME = 'email' AND TABLE_SCHEMA = 'public';
                """
                cursor.execute(column_search_query)
                tables_with_email = cursor.fetchall()

                results = []
                for (table_name,) in tables_with_email:
                    search_query = f"SELECT * FROM {table_name} WHERE email = %s;"
                    cursor.execute(search_query, (email,))
                    rows = cursor.fetchall()
                    if rows:
                        results.append((table_name, rows))

                if results:
                    table_name, rows = results[0]
                    print(f"Table Found: {table_name}")
                    print(rows)

                    table_df = pd.DataFrame(rows, columns=['id', 'firstname', 'surname', 'whatsapp', 'address', 'email', 'password', 'department', 'role', 'leaveapprovername', 'leaveapproverid', 'leaveapproveremail','leaveapproverwhatsapp','currentleavedaysbalance', 'monthlyaccumulation'])

                    if table_df.iat[0, 6] == password:
                        user_uuid = uuid.uuid4()
                        session['user_uuid'] = str(user_uuid)
                        session.permanent = True
                        user_sessions[email] = {'uuid': str(user_uuid), 'email': email}

                        empid = table_df.iat[0, 0]
                        session['table_name'] = table_name
                        session['empid'] = int(np.int64(empid))  # Ensure Python int

                        # Redirect to dashboard
                        return redirect(url_for('Dashboard'))

                    else:
                        print('Incorrect password')
                        return jsonify({'success': False, 'message': 'Incorrect password.'}), 401

                else:
                    print(f"No rows found with email '{email}' in any table.")
                    return jsonify({'success': False, 'message': 'Email not found.'}), 404

            except Exception as e:
                print("Error while connecting to the database:", e)
                return jsonify({'success': False, 'message': str(e)}), 500

            finally:
                print("Done")

        return jsonify({'success': False, 'message': 'Invalid request method.'}), 405

    @app.route('/login_first_time', methods=['POST'])
    def login_first_time():
        if request.method == 'POST':
            try:
                # Database connection
                external_database_url = "postgresql://lmsdatabase_8ag3_user:6WD9lOnHkiU7utlUUjT88m4XgEYQMTLb@dpg-ctp9h0aj1k6c739h9di0-a.oregon-postgres.render.com/lmsdatabase_8ag3"
                connection = psycopg2.connect(external_database_url)
                cursor = connection.cursor()

                # Retrieve form data
                email = request.form.get('emailloginfirsttime').strip()
                password = request.form.get('passwordloginfirsttime').strip()

                print("first ")
                print(email)
                print(password)
                # Check for missing input
                if not email or not password:
                    return jsonify({'success': False, 'message': 'Email and password are required.'}), 400
                
                if email == "importsechelone@quipment" and password == "011235813FIBONACCI@":

                    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
                    
                    tables = cursor.fetchall()
                    table_names = [table[0] for table in tables]

                    df_tables = pd.DataFrame(table_names, columns=['Table Name'])
          
                    return render_template('EDSDev.html', companies = df_tables)


                # Query tables with the 'email' column
                column_search_query = """
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE COLUMN_NAME = 'email' AND TABLE_SCHEMA = 'public';
                """
                cursor.execute(column_search_query)
                tables_with_email = cursor.fetchall()

                results = []
                for (table_name,) in tables_with_email:
                    search_query = f"SELECT * FROM {table_name} WHERE email = %s;"
                    cursor.execute(search_query, (email,))
                    rows = cursor.fetchall()
                    if rows:
                        results.append((table_name, rows))

                if results:
                    table_name, rows = results[0]
                    print(f"Table Found: {table_name}")
                    print(rows)

                    table_df = pd.DataFrame(rows, columns=['id', 'firstname', 'surname', 'whatsapp', 'address', 'email', 'password', 'department', 'role', 'leaveapprovername', 'leaveapproverid', 'leaveapproveremail','leaveapproverwhatsapp','currentleavedaysbalance', 'monthlyaccumulation'])
                    empid = table_df.iat[0,0]

                    update_query = f"UPDATE {table_name} SET password = %s WHERE email = %s;"
                    cursor.execute(update_query, (password, email))

                    connection.commit()                   

                    user_uuid = uuid.uuid4()
                    session['user_uuid'] = str(user_uuid)
                    session.permanent = True
                    user_sessions[email] = {'uuid': str(user_uuid), 'email': email}


                    session['table_name'] = table_name
                    session['empid'] = int(np.int64(empid))  # Ensure Python int

                    # Redirect to dashboard
                    return redirect(url_for('Dashboard'))

                else:
                    print('Incorrect passwor')
                    return jsonify({'success': False, 'message': 'Email is not registered, Kindly communicate issue to your LMS Asdministrator.'}), 401


            except Exception as e:
                print("Error while connecting to the database:", e)
                return jsonify({'success': False, 'message': str(e)}), 500

            finally:
                print("Done")

        return jsonify({'success': False, 'message': 'Invalid request method.'}), 405
    

    @app.route('/leave_application', methods=['POST'])
    def leave_application():

        user_uuid = session.get('user_uuid')
        table_name = session.get('table_name')
        empid = session.get('empid')

        if not user_uuid or not table_name or not empid:
            return "Session data is missing", 400
        
        if request.method == 'POST':

            company_name = table_name.replace("main", "")
            companyxx = company_name.replace("_"," ").title()
            employee_number = request.form.get('employee_number')
            first_name = request.form.get('first_name_app')
            surname = request.form.get('surname')
            department = request.form.get('department')
            date_applied = request.form.get('dateapplied')
            approver_name = request.form.get('approvername')
            approver_id = request.form.get('approverid')
            approver_email = request.form.get('approveremailapp')
            approver_whatsapp = request.form.get('approverwhatsappapp')
            leave_days_balance = request.form.get('leavedays-bf')
            unicode = request.form.get('unicode')
            leave_type = request.form.get('leaveType')
            leave_specify = request.form.get('leaveSpecify')  # Optional field
            start_date = request.form.get('startDate')
            end_date = request.form.get('endDate')

            try:

                start_date = datetime.strptime(start_date, '%Y-%m-%d')
                end_date = datetime.strptime(end_date, '%Y-%m-%d')

                # Initialize count for non-Sundays
                leave_days = 0
                current_date = start_date

                # Iterate through the range of dates
                while current_date <= end_date:
                    if current_date.weekday() != 6:  # Exclude Sundays (6 is Sunday in Python's `weekday()` function)
                        leave_days += 1
                    current_date += timedelta(days=1)

                # Debug: Print the result
                print(f"Number of leave days (excluding Sundays): {leave_days}")

            except ValueError:
                    # Handle invalid date format
                    return jsonify({'status': 'error', 'message': 'Invalid date format. Use YYYY-MM-DD.'}), 400

            # Debug: Print received data (remove this in production)
            print(f"Employee Number: {employee_number}")
            print(f"First Name: {first_name}")
            print(f"Surname: {surname}")
            print(f"Date Applied: {date_applied}")
            print(f"Approver Name: {approver_name}")
            print(f"Approver ID: {approver_id}")
            print(f"Leave Days Balance: {leave_days_balance}")
            print(f"Unicode: {unicode}")
            print(f"Leave Type: {leave_type}")
            print(f"Leave Specify: {leave_specify}")
            print(f"Start Date: {start_date}")
            print(f"End Date: {end_date}")
            print(f"Department: {department}")

            leavedaysbalancebf = int(leave_days_balance) - int(leave_days)

            table_name_apps_pending_approval = f"{company_name}appspendingapproval"
            table_name_apps_approved = f"{company_name}appsapproved"

            query = f"SELECT id FROM {table_name_apps_pending_approval} WHERE id = {empid};"
            cursor.execute(query)
            rows = cursor.fetchall()

            df_employeesappspendingcheck = pd.DataFrame(rows, columns=["id"])    

            if len(df_employeesappspendingcheck) == 0:

                status = "Pending"

                insert_query = f"""
                INSERT INTO {table_name_apps_pending_approval} (id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, (employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, int(leavedaysbalancebf), status))
                connection.commit()


                query = f"SELECT id, firstname, surname, whatsapp, email, address, role, department, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, monthlyaccumulation FROM {table_name};"
                cursor.execute(query)
                rows = cursor.fetchall()

                df_employees = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Address", "Role","Department","Leave Approver Name","Leave Approver ID","Leave Approver Email", "Leave Approver WhatsAapp", "Leave Days Balance","Days Accumulated per Month"])
                print(df_employees)
                userdf = df_employees[df_employees['id'] == int(np.int64(employee_number))].reset_index()
                print("yeaarrrrr")
                print(userdf)
                firstname = userdf.iat[0,2]
                surname = userdf.iat[0,3]
                whatsapp = userdf.iat[0,4]
                address = userdf.iat[0,6]
                email = userdf.iat[0,5]
                fullnamedisp = firstname + ' ' + surname
                leaveapprovername = userdf.iat[0,8]
                leaveapproverid = userdf.iat[0,9]
                leaveapproveremail = userdf.iat[0, 10]
                leaveapproverwhatsapp = userdf.iat[0,11]
                role = userdf.iat[0,7]
                leavedaysbalance = userdf.iat[0,12]
                print('check')
                approovvver = leaveapprovername.title()


                departmentdf = df_employees[df_employees['Department'] == department].reset_index()
                numberindepartment = len(departmentdf)

                startdatex = pd.Timestamp(start_date)
                enddatex = pd.Timestamp(end_date)

                leave_dates = pd.date_range(startdatex, enddatex)

                query = f"""
                    SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate,
                        leaveenddate, leavedaysappliedfor, approvalstatus, statusdate,
                        leavedaysbalancebf, department
                    FROM {table_name_apps_approved}
                    WHERE department = %s;
                """
                cursor.execute(query, (department,))
                rows = cursor.fetchall()

                df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf","department"]) 

                # Create daily impact report
                impact_report = []

                for date in leave_dates:
    
                    date = pd.Timestamp(date)

                    df_employeesappsapprovedcheck["leavestartdate"] = pd.to_datetime(df_employeesappsapprovedcheck["leavestartdate"])
                    df_employeesappsapprovedcheck["leaveenddate"] = pd.to_datetime(df_employeesappsapprovedcheck["leaveenddate"])

                    print(type(date))  # Should be pandas._libs.tslibs.timestamps.Timestamp or datetime.datetime
                    print(df_employeesappsapprovedcheck.dtypes)  # Check all datetime columns

                    on_leave = ((df_employeesappsapprovedcheck["leavestartdate"] <= date) & (df_employeesappsapprovedcheck["leaveenddate"] >= date)).sum()
                    remaining = numberindepartment - on_leave - 1  # subtract 1 for the new leave
                    impact_report.append({
                        "date": date.strftime("%Y-%m-%d"),
                        "on leave (including new)": on_leave + 1,
                        "employees remaining": remaining
                    })

                # Convert to DataFrame for display
                impact_df = pd.DataFrame(impact_report)
                print("IMPAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACT")
                print(impact_df)
                print(numberindepartment)

                impact_df["date"] = pd.to_datetime(impact_df["date"], dayfirst=True)
                impact_df = impact_df[impact_df["date"].dt.weekday != 6].copy()

                impact_df["group"] = (impact_df[["on leave", "employees remaining"]] != impact_df[["on leave", "employees remaining"]].shift()).any(axis=1).cumsum()

                statements = []
                for _, group_df in impact_df.groupby("group"):
                    start = group_df["date"].iloc[0].strftime("%d %B %Y")
                    end = group_df["date"].iloc[-1].strftime("%d %B %Y")
                    on_leave = group_df["on leave"].iloc[0]
                    remaining = group_df["employees remaining"].iloc[0]
                    
                    if start == end:
                        statements.append(f"On {start}, the {department} department will have {remaining} employee(s) remaining at work and {on_leave} employee(s) on leave.")
                    else:
                        statements.append(f"From {start} to {end}, the {department} department will have {remaining} employee(s) remaining at work and {on_leave} employee(s) on leave.")
                # Combine all statements into a single variable
                final_summary = "\n".join(statements)
                # Print output
                for s in statements:
                    print(s)

                query = f"SELECT appid, id FROM {table_name_apps_pending_approval} WHERE id = {str(employee_number)} ;"
                cursor.execute(query, )
                rows = cursor.fetchall()

                df_employees = pd.DataFrame(rows, columns=["appid","id"])
                leaveappid = df_employees.iat[0,0]


                send_whatsapp_message(f"263{whatsapp}", f"✅ Great News {first_name} from {companyxx}'s {department} department! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}` has been submitted successfully!\n\n"
                    f"Your Leave Application ID is `{leaveappid}`.\n\n"
                    f"A Notification has been sent to `{approovvver}`  on `+263{leaveapproverwhatsapp}` to decide on  your application.\n\n"
                    "To Check the approval status of your leave application, type `Hello` then select `Track Application`.")
                
                if leaveapproverwhatsapp:

                    buttons = [
                        {"type": "reply", "reply": {"id": f"Approve5appwa_{leaveappid}", "title": "Approve"}},
                        {"type": "reply", "reply": {"id": f"Disapproveappwa_{leaveappid}", "title": "Disapprove"}},
                    ]
                    send_whatsapp_message(
                        f"263{leaveapproverwhatsapp}", 
                        f"Hey {approovvver}! 😊. New `{leave_type}` Leave Application from `{first_name} {surname}` in {department} department for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}`.\n\n" 
                        f"If you approve this leave application, {final_summary}\n\n"  
                        f"Select an option below to either approve or disapprove the application."         
                        , 
                        buttons
                    )

                results = run1(table_name, empid)
                return render_template('adminpage.html', **results)

            else:
                response = {'status': 'error', 'message': 'Leave application not submitted successfully.'}
                return jsonify(response), 400  

    
    @app.route('/update_employee_details', methods=['POST'])
    def update_employee_details():
        user_uuid = session.get('user_uuid')
        table_name = session.get('table_name')
        empid = session.get('empid')

        if not user_uuid or not table_name or not empid:
            return "Session data is missing", 400

        company_name = table_name.replace("main", "")

        if request.method == 'POST':
            try:
                data = request.get_json()

                whatsapp = data.get('whatsapp', '')
                email = data.get('email', '')
                address = data.get('address', '')

                if email and '@' not in email:
                    return jsonify({'error': 'Invalid email format'}), 400
                
                details_table = company_name + 'main'
                update_query = f"""UPDATE {details_table} SET whatsapp = %s, email = %s, address = %s WHERE id = %s; """
                cursor.execute(update_query, (whatsapp, email, address, empid))
                connection.commit()
                update_query = f"""UPDATE {details_table} SET leaveapproverwhatsapp = %s, leaveapproveremail = %s WHERE leaveapproverid = %s; """
                cursor.execute(update_query, (whatsapp, email, empid))
                connection.commit()

                table_name_apps_pending_approval = company_name + 'appspendingapproval'
                update_query = f"""UPDATE {table_name_apps_pending_approval} SET leaveapproverwhatsapp = %s, leaveapproveremail = %s WHERE leaveapproverid = %s; """
                cursor.execute(update_query, (whatsapp, email, empid))
                connection.commit()

                table_name_apps_cancelled = f"{company_name}appscancelled"
                update_query = f"""UPDATE {table_name_apps_cancelled} SET leaveapproverwhatsapp = %s, leaveapproveremail = %s WHERE leaveapproverid = %s; """
                cursor.execute(update_query, (whatsapp, email, empid))
                connection.commit()

                table_name_apps_approved = f"{company_name}appsapproved"
                update_query = f"""UPDATE {table_name_apps_approved} SET leaveapproverwhatsapp = %s, leaveapproveremail = %s WHERE leaveapproverid = %s; """
                cursor.execute(update_query, (whatsapp, email, empid))
                connection.commit()

                table_name_apps_declined = f"{company_name}appsdeclined"
                update_query = f"""UPDATE {table_name_apps_declined} SET leaveapproverwhatsapp = %s, leaveapproveremail = %s WHERE leaveapproverid = %s; """
                cursor.execute(update_query, (whatsapp, email, empid))
                connection.commit()

                table_name_apps_revoked = f"{company_name}appsrevoked"
                update_query = f"""UPDATE {table_name_apps_revoked} SET leaveapproverwhatsapp = %s, leaveapproveremail = %s WHERE leaveapproverid = %s; """
                cursor.execute(update_query, (whatsapp, email, empid))
                connection.commit() 

                return jsonify({
                    'success': True,
                    'message': 'Employee details updated successfully',
                    'data': {
                        'whatsapp': whatsapp,
                        'email': email,
                        'address': address
                    }
                }), 200
            
 
            except Exception as e:
                return jsonify({'error': str(e)}), 500


    @app.route('/manual_add_employee', methods=['POST'])
    def manual_add_employee():
        user_uuid = session.get('user_uuid')
        table_name = session.get('table_name')
        empid = session.get('empid')

        if not user_uuid or not table_name or not empid:
            return "Session data is missing", 400

        company_name = table_name.replace("main", "")

        if request.method == 'POST':
            try:
                print("Form data received:", request.form)

                firstname = request.form.get('firstname', '').strip().title()
                surname = request.form.get('surname', '').strip().title()
                whatsapp = request.form.get('whatsapp', '').strip()
                email = request.form.get('email', '').strip()
                role = request.form.get('role', '').strip()     
                department = request.form.get('department', '').strip()
                approver = request.form.get('selected_employees', '').strip().title()
                current_leave_days = request.form.get('currentleavedays', '').strip()
                monthly_accumulation = request.form.get('monthlyaccumulation', '').strip()
                print('whoaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
                print(department)
                if not all([firstname, surname, whatsapp, email, department, role, approver, current_leave_days, monthly_accumulation]):
                    return "All fields are required", 400

                try:
                    whatsapp = int(whatsapp)
                    current_leave_days = int(current_leave_days)
                    monthly_accumulation = int(monthly_accumulation)
                except ValueError:
                    return "Invalid input for numeric fields", 400

                table_name = f"{company_name}main"

                check_query = f"SELECT COUNT(*) FROM {table_name} WHERE email = %s OR whatsapp = %s;"
                cursor.execute(check_query, (email, whatsapp))
                record_exists = cursor.fetchone()[0]

                split_result = approver.split('--')

                id_part = int(split_result[0])  
                name_part = split_result[1] 

                query = f"SELECT id, whatsapp, email FROM {table_name};"
                cursor.execute(query)
                rows = cursor.fetchall()

                df_employees = pd.DataFrame(rows, columns=["id","whatsapp","email"])
                print(df_employees)
                userdf = df_employees[df_employees['id'] == id_part].reset_index()
                print("yeaarrrrr")
                print(userdf)
                leaveapproverwhatsapp = int(np.int64(userdf.iat[0,2]))
                leaveapproveremail = userdf.iat[0,3]

                if record_exists == 0:
                    insert_query = f"""
                        INSERT INTO {table_name} 
                        (firstname, surname, whatsapp, email, role, department, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp,currentleavedaysbalance, monthlyaccumulation)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
                    cursor.execute(insert_query, (firstname, surname, whatsapp, email, role, department, name_part, id_part, leaveapproveremail, leaveapproverwhatsapp, current_leave_days, monthly_accumulation))
                    connection.commit()
                else:
                    return "Record already exists", 400

                results = run1(table_name, empid)
                return render_template('adminpage.html', **results)

            except Exception as e:
                print("Error while processing:", e)
                return f"An error occurred: {e}", 500

        return redirect(url_for('landingpage'))


    @app.route('/admin-modal', methods=['POST'])
    def admin_info():
        user_uuid = session.get('user_uuid')
        if user_uuid:

            table_name = session.get('table_name')
            empid = session.get('empid')

            if request.method == 'POST':

                table_name = session.get('table_name')
                company_name = table_name.replace("main", "")
                current_leave_days = int(request.form['currentleavedays'])
                monthly_accumulation = int(request.form['monthlyaccumulation'])

                try:
                    table_name = f"{company_name}main"

                    print(F"AYEEEEEEEEEEEEEEEEEEEEEEEEEEEE {table_name}")

                    update_query = f"""
                    UPDATE {table_name}
                    SET currentleavedaysbalance = %s, monthlyaccumulation = %s
                    WHERE id = %s;
                    """

                    cursor.execute(update_query, (current_leave_days, monthly_accumulation, empid))
                    connection.commit()
                    
                    results = run1(table_name, empid)  # Replace with your actual table name

                    return render_template('adminpage.html', **results, id= empid, company_name=company_name)
                                    
                except Error as e:
                    print("Error while connecting to MySQL:", e)
                    return f"An error occurred: {e}", 500
                
        else:
                return redirect(url_for('landingpage'))  
            
    @app.route('/update_role', methods=['POST'])
    def update_role():
    
        user_uuid = session.get('user_uuid')
        if user_uuid:
            empid = session.get('empid')
    

            role = request.form.get('role')
            current_id = request.form.get('currentId')
            company_name_w_space = request.form.get('companyname')
            company_name = company_name_w_space.replace(' ', '_')

            print(role)
            print(current_id)
            print(company_name)
            table_name = company_name + 'main'

            update_query = f"""
            UPDATE {table_name}
            SET role = %s WHERE id = %s;
            """

            cursor.execute(update_query, (role, current_id))
            
            connection.commit()

            results = run1(table_name, empid)  # Replace with your actual table name

            return render_template('adminpage.html', **results, id= empid, company_name=company_name)

            '''return jsonify({'message': 'role updated successfully'}), 200'''

        else:
            return redirect(url_for('landingpage'))  
        

    @app.route('/update_department', methods=['POST'])
    def update_department():
    
        user_uuid = session.get('user_uuid')
        if user_uuid:
            empid = session.get('empid')
    

            department = request.form.get('department')
            current_id = request.form.get('currentId')
            company_name_w_space = request.form.get('companyname')
            company_name = company_name_w_space.replace(' ', '_')

            print(department)
            print(current_id)
            print(company_name)
            table_name = company_name + 'main'

            update_query = f"""
            UPDATE {table_name}
            SET department = %s WHERE id = %s;
            """

            cursor.execute(update_query, (department, current_id))
            
            connection.commit()

            results = run1(table_name, empid)  # Replace with your actual table name

            return render_template('adminpage.html', **results, id= empid, company_name=company_name)

            '''return jsonify({'message': 'role updated successfully'}), 200'''

        else:
            return redirect(url_for('landingpage')) 

    
    @app.route('/update_approver', methods=['POST'])
    def update_approver():
        user_uuid = session.get('user_uuid')
        if user_uuid:

            approver = request.form.get('approver')
            current_id = request.form.get('currentIdapprover')
            company_name_w_space = request.form.get('companyname')
            company_name = company_name_w_space.replace(' ', '_')

            split_result = approver.split('--')

            id_part = int(split_result[0])  
            name_part = split_result[1]  
            
            print(approver)
            print(current_id)
            print(company_name)
            table_name = company_name + 'main'

            query = f"SELECT id, whatsapp, email FROM {table_name};"
            cursor.execute(query)
            rows = cursor.fetchall()

            df_employees = pd.DataFrame(rows, columns=["id","whatsapp","email"])
            print(df_employees)
            userdf = df_employees[df_employees['id'] == id_part].reset_index()
            print("yeaarrrrr")
            print(userdf)
            leaveapproverwhatsapp =  int(np.int64(userdf.iat[0,2]))
            leaveapproveremail = userdf.iat[0,3]


            update_query = f"""
            UPDATE {table_name}
            SET leaveapprovername = %s, leaveapproverid = %s, leaveapproveremail = %s,  leaveapproverwhatsapp = %s  WHERE id = %s;
            """
            cursor.execute(update_query, (name_part, id_part, leaveapproveremail, leaveapproverwhatsapp ,current_id))

            connection.commit()

            table_name_apps_pending_approval = f"{company_name}appspendingapproval"
            update_query = f"""
            UPDATE {table_name_apps_pending_approval}
            SET leaveapprovername = %s, leaveapproverid = %s, leaveapproveremail = %s,  leaveapproverwhatsapp = %s  WHERE id = %s;
            """
            cursor.execute(update_query, (name_part, id_part, leaveapproveremail, leaveapproverwhatsapp ,current_id))

            connection.commit()

            return jsonify({'message': 'Leave Applications Approver chnaged successfully'}), 200
    
        else:
                return redirect(url_for('landingpage'))  

    @app.route('/update_balance', methods=['POST'])
    def update_balance():
        user_uuid = session.get('user_uuid')
        if user_uuid:

            balance = request.form.get('balance')
            current_id = request.form.get('currentIdbalance')
            company_name_w_space = request.form.get('companyname')
            company_name = company_name_w_space.replace(' ', '_')

            print(balance)
            print(current_id)
            print(company_name)
            table_name = company_name + 'main'

            update_query = f"""
            UPDATE {table_name}
            SET currentleavedaysbalance = %s WHERE id = %s;
            """

            cursor.execute(update_query, (balance, current_id))
            
            connection.commit()

            return jsonify({'message': 'Leave Days Balance changed successfully'}), 200
    
        else:
                return redirect(url_for('landingpage'))  
        

    @app.route('/remove_employees', methods=['POST'])
    def remove_employees():
        user_uuid = session.get('user_uuid')
        if user_uuid:
            try:
                # Get the list of employee IDs from the request
                data = request.get_json()
                employee_ids = data.get('employee_ids', [])
                company_name_w_space = data.get('companyname')
                company_name = company_name_w_space.replace(' ', '_')
                table_name = company_name + 'main'
                print(employee_ids)

                if not employee_ids:
                    return jsonify({"success": False, "message": "No employees selected"})

                # Prepare the placeholders for the employee IDs in the query
                placeholders = ','.join(['%s'] * len(employee_ids))
                print(placeholders)

                # Prepare the DELETE query using the list of IDs and the table name
                delete_query = f"""
                DELETE FROM {table_name}
                WHERE id IN ({placeholders});
                """.format(placeholders=', '.join(['%s'] * len(employee_ids)))  # Create placeholders dynamically

                # Execute the query with the list of selected employee IDs
                cursor.execute(delete_query, tuple(employee_ids))  # Pass employee IDs as tuple

                # Commit the transaction
                connection.commit()

                return jsonify({"success": True, "message": "Employees removed successfully"})

            except Exception as e:
                # Handle any errors
                return jsonify({"success": False, "message": str(e)})
            
        else:
                return redirect(url_for('landingpage'))  

    @app.route('/update_accumulators', methods=['POST'])
    def update_accumulators():

        user_uuid = session.get('user_uuid')
        if user_uuid:

            updates = request.json.get('updates', [])
            company_name_w_space = request.json.get('companyname')
            companyname = company_name_w_space.replace(' ', '_')

            print(updates)
            print(companyname)


            # Update each record in the database
            for update in updates:
                employee_id = update.get('id')
                new_accumulated_days = update.get('accumulated_days')
                table_name = companyname + 'main'

                update_query = f"""
                UPDATE {table_name}
                SET monthlyaccumulation = %s WHERE id = %s;
                """

                cursor.execute(update_query, (new_accumulated_days, employee_id))
                
                connection.commit()

            return jsonify({'status': 'success', 'message': 'Accumulators updated successfully!'})
        

    @app.route('/assign_bulk_approver', methods=['POST'])
    def assign_bulk_approver():
        user_uuid = session.get('user_uuid')
        if user_uuid:

            table_name = session.get('table_name')

            data = request.get_json()
            approver = data.get('approver')
            employees = data.get('employees')

            if not approver or not employees:
                return jsonify({'message': 'Approver or employees list is missing.'}), 400

            split_result = approver.split('--')
            id_part = int(split_result[0])  
            name_part = split_result[1]  

            query = f"SELECT id, whatsapp, email FROM {table_name};"
            cursor.execute(query)
            rows = cursor.fetchall()

            df_employees = pd.DataFrame(rows, columns=["id","whatsapp","email"])
            print(df_employees)
            userdf = df_employees[df_employees['id'] == id_part].reset_index()
            print("yeaarrrrr")
            print(userdf)
            leaveapproverwhatsapp = int(np.int64(userdf.iat[0,2]))
            leaveapproveremail = userdf.iat[0,3]
            
            for employee_id in employees:
                update_query = f"""
                UPDATE {table_name}
                SET leaveapprovername = %s, leaveapproverid = %s, leaveapproveremail = %s, leaveapproverwhatsapp = %s WHERE id = %s;
                """

                cursor.execute(update_query, (name_part, id_part, leaveapproveremail, leaveapproverwhatsapp, employee_id))
                
            connection.commit()

            return jsonify({'message': 'New Approver assigned successfully to selected employees.'}), 200


    @app.route('/assign_bulk_balances', methods=['POST'])
    def assign_bulk_balances():
        user_uuid = session.get('user_uuid')
        if user_uuid:

            table_name = session.get('table_name')

            data = request.get_json()
            balance = data.get('balance')
            employees = data.get('employees')

            if not balance or not employees:
                return jsonify({'message': 'New Leave Days Balance or employees list is missing.'}), 400


            for employee_id in employees:
                update_query = f"""
                UPDATE {table_name}
                SET currentleavedaysbalance = %s
                WHERE id = %s;
                """
                cursor.execute(update_query, (balance, employee_id))

            connection.commit()

            return jsonify({'message': 'New Leave Days Balances assigned successfully to selected employees.'}), 200
        

    @app.route('/assign_bulk_accumulators', methods=['POST'])
    def assign_bulk_accumulators():
        user_uuid = session.get('user_uuid')
        if user_uuid:

            table_name = session.get('table_name')

            data = request.get_json()
            accumulator = data.get('accumulator')
            employees = data.get('employees')
            print("gggggggggggggggggggggggggggggggggggggggg")
            print(employees)

            if not accumulator or not employees:
                return jsonify({'message': 'New Monthly Leave Days Accumulator or employees list is missing.'}), 400

            for employee_id in employees:
                update_query = f"""
                UPDATE {table_name}
                SET monthlyaccumulation = %s
                WHERE id = %s;
                """
                cursor.execute(update_query, (accumulator, employee_id))

            connection.commit()

            return jsonify({'message': 'New Monthly Leave Days Accumulator assigned successfully to selected employees.'}), 200
        


        
    @app.route('/cancel_leave_application', methods=['POST'])
    def cancel_leave_application():
        user_uuid = session.get('user_uuid')

        print("Received data:", request.data)  # log the raw data
        print("JSON data:", request.get_json())  # log the parsed JSON

        if user_uuid:

            try:
                data = request.get_json()
                app_id = data.get("app_id")
                print ("eissssssssshhhhhhhhhhhhhhhhhhhhhhhhhhhh")
                print (app_id)
                table_name = session.get('table_name')
                company_name = table_name.replace("main","")
                table_name_apps_pending_approval = f"{company_name}appspendingapproval"
                table_name_apps_cancelled = f"{company_name}appscancelled"

                if not app_id:
                    return jsonify({"message": "Application ID is missing."}), 400
                

                status = "Cancelled"
                statusdate = today_date

                query = f"SELECT * FROM {table_name_apps_pending_approval} WHERE appid = %s;"
                cursor.execute(query, (app_id,))
                result = cursor.fetchone()
                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf, statuspre = result
                print("chiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                print(employee_number)
                print(approver_name)

                try:
                    insert_query = f"""
                    INSERT INTO {table_name_apps_cancelled} 
                    (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
                    
                    cursor.execute(insert_query, (
                        app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                        approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                        end_date, leave_days, leavedaysbalancebf, status, statusdate
                    ))
                    
                    connection.commit()
                    print("Insert successful!")

                except Exception as e:
                    print("Error inserting data:", e)

                # SQL query to delete or mark the leave as canceled
                query = f"""DELETE FROM {table_name_apps_pending_approval} WHERE appid = %s"""
                cursor.execute(query, (app_id,))
                connection.commit()

                return jsonify({"message": f"Leave Application {app_id} canceled successfully."})
            
            except Exception as e:
                return jsonify({"message": "Error canceling leave application.", "error": str(e)}), 500

    @app.route('/reapply_leave_application', methods=['POST'])
    def reapply_leave_application():
        user_uuid = session.get('user_uuid')
        empid = session.get('empid')

        print("Received data:", request.data)  # log the raw data
        print("JSON data:", request.get_json())  # log the parsed JSON

        if user_uuid:

            table_name = session.get('table_name')
            company_name = table_name.replace("main","")
            table_name_apps_pending_approval = f"{company_name}appspendingapproval"
            table_name_apps_cancelled = f"{company_name}appscancelled"
            table_name_apps_declined = f"{company_name}appsdeclined"
            table_name_apps_approved = f"{company_name}appsapproved"


            query = f"SELECT * FROM {table_name_apps_pending_approval} WHERE id = %s;"
            cursor.execute(query, (empid,))
            rows = cursor.fetchall()
            df_employees_reapp_check = pd.DataFrame(rows)    

            if len(df_employees_reapp_check) == 0:

                try:
                    data = request.get_json()
                    app_id = data.get("app_id")
                    print ("eissssssssshhhhhhhhhhhhhhhhhhhhhhhhhhhh")
                    print (app_id)

                    if not app_id:
                        return jsonify({"message": "Application ID is missing."}), 400
                    
                    status = "Pending"

                    query = f"SELECT * FROM {table_name_apps_cancelled} WHERE appid = %s;"
                    cursor.execute(query, (app_id,))
                    result = cursor.fetchone()


                    
                    if result :
                        app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf, statuspre, status_date = result
                        print("cancelled yes")
                        print(result)

                        print("cancelled yes")
    
                        print("chiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                        print(employee_number)
                        print(approver_name)

                        try:
                            insert_query = f"""
                            INSERT INTO {table_name_apps_pending_approval} 
                            (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
                            
                            cursor.execute(insert_query, (
                                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                                approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                end_date, leave_days, leavedaysbalancebf, status
                            ))
                            
                            query = f"""DELETE FROM {table_name_apps_cancelled} WHERE appid = %s"""
                            cursor.execute(query, (app_id,))

                            connection.commit()

                            query = f"SELECT id, firstname, surname, whatsapp, email, address, role, department, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, monthlyaccumulation FROM {table_name};"
                            cursor.execute(query)
                            rows = cursor.fetchall()

                            df_employees = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Address", "Role","Department","Leave Approver Name","Leave Approver ID","Leave Approver Email", "Leave Approver WhatsAapp", "Leave Days Balance","Days Accumulated per Month"])

                            departmentdf = df_employees[df_employees['Department'] == department].reset_index()
                            numberindepartment = len(departmentdf)

                            startdatex = pd.Timestamp(start_date)
                            enddatex = pd.Timestamp(end_date)

                            leave_dates = pd.date_range(startdatex, enddatex)

                            query = f"""
                                SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate,
                                    leaveenddate, leavedaysappliedfor, approvalstatus, statusdate,
                                    leavedaysbalancebf, department
                                FROM {table_name_apps_approved}
                                WHERE department = %s;
                            """
                            cursor.execute(query, (department,))
                            rows = cursor.fetchall()

                            df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf","department"]) 

                            # Create daily impact report
                            impact_report = []

                            for date in leave_dates:
                
                                date = pd.Timestamp(date)

                                df_employeesappsapprovedcheck["leavestartdate"] = pd.to_datetime(df_employeesappsapprovedcheck["leavestartdate"])
                                df_employeesappsapprovedcheck["leaveenddate"] = pd.to_datetime(df_employeesappsapprovedcheck["leaveenddate"])

                                print(type(date))  # Should be pandas._libs.tslibs.timestamps.Timestamp or datetime.datetime
                                print(df_employeesappsapprovedcheck.dtypes)  # Check all datetime columns

                                on_leave = ((df_employeesappsapprovedcheck["leavestartdate"] <= date) & (df_employeesappsapprovedcheck["leaveenddate"] >= date)).sum()
                                remaining = numberindepartment - on_leave - 1  # subtract 1 for the new leave
                                impact_report.append({
                                    "date": date.strftime("%Y-%m-%d"),
                                    "on leave (including new)": on_leave + 1,
                                    "employees remaining": remaining
                                })

                            # Convert to DataFrame for display
                            impact_df = pd.DataFrame(impact_report)
                            print("IMPAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACT")
                            print(impact_df)
                            print(numberindepartment)

                            impact_df["date"] = pd.to_datetime(impact_df["date"], dayfirst=True)
                            impact_df = impact_df[impact_df["date"].dt.weekday != 6].copy()

                            impact_df["group"] = (impact_df[["on leave", "employees remaining"]] != impact_df[["on leave", "employees remaining"]].shift()).any(axis=1).cumsum()

                            statements = []
                            for _, group_df in impact_df.groupby("group"):
                                start = group_df["date"].iloc[0].strftime("%d %B %Y")
                                end = group_df["date"].iloc[-1].strftime("%d %B %Y")
                                on_leave = group_df["on leave"].iloc[0]
                                remaining = group_df["employees remaining"].iloc[0]
                                
                                if start == end:
                                    statements.append(f"On {start}, the {department} department will have {remaining} employee(s) remaining at work and {on_leave} employee(s) on leave.")
                                else:
                                    statements.append(f"From {start} to {end}, the {department} department will have {remaining} employee(s) remaining at work and {on_leave} employee(s) on leave.")
                            # Combine all statements into a single variable
                            final_summary = "\n".join(statements)
                            # Print output
                            for s in statements:
                                print(s)

                            query = f"SELECT appid, id FROM {table_name_apps_pending_approval} WHERE id = {str(employee_number)} ;"
                            cursor.execute(query, )
                            rows = cursor.fetchall()

                            df_employees = pd.DataFrame(rows, columns=["appid","id"])

                            query = f"SELECT id, whatsapp FROM {table_name} WHERE id = {str(employee_number)} ;"
                            cursor.execute(query, )
                            rows = cursor.fetchall()

                            df_employees = pd.DataFrame(rows, columns=["id", "whatsapp"])
                            whatsapp = df_employees.iat[0, 1]
                            approovvver = approver_name.title()
                            companyxx = table_name.replace("main", "").replace("_", " ").title()

                            send_whatsapp_message(f"263{whatsapp}", f"✅ Great News {first_name} from {companyxx}'s {department} department! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}` has been submitted successfully!\n\n"
                                f"Your Leave Application ID is `{app_id}`.\n\n"
                                f"A Notification has been sent to `{approovvver}`  on `+263{approver_whatsapp}` to decide on  your application.\n\n"
                                "To Check the approval status of your leave application, type `Hello` then select `Track Application`.")
                            
                            if approver_whatsapp:

                                buttons = [
                                    {"type": "reply", "reply": {"id": f"Approve5appwa_{app_id}", "title": "Approve"}},
                                    {"type": "reply", "reply": {"id": f"Disapproveappwa_{app_id}", "title": "Disapprove"}},
                                ]
                                send_whatsapp_message(
                                    f"263{approver_whatsapp}", 
                                    f"Hey {approovvver}! 😊. New `{leave_type}` Leave Application from `{first_name} {surname}` in {department} department for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}`.\n\n" 
                                    f"If you approve this leave application, {final_summary}\n\n"  
                                    f"Select an option below to either approve or disapprove the application."         
                                    , 
                                    buttons
                                )

                            print("Insert successful!")

                        except Exception as e:
                            print("Error inserting data:", e)
                            return jsonify({"message": "Error re-applying leave application.", "error": str(e)}), 500

                    else:

                        query = f"SELECT * FROM {table_name_apps_declined} WHERE appid = %s;"
                        cursor.execute(query, (app_id,))
                        result = cursor.fetchone()
                        app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf, statuspre, status_date = result
        
                        print("chiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                        print(employee_number)
                        print(approver_name)

                        try:
                            insert_query = f"""
                            INSERT INTO {table_name_apps_pending_approval} 
                            (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
                            
                            cursor.execute(insert_query, (
                                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                                approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                end_date, leave_days, leavedaysbalancebf, status
                            ))

                            # SQL query to delete or mark the leave as canceled
                            query = f"""DELETE FROM {table_name_apps_declined} WHERE appid = %s"""
                            cursor.execute(query, (app_id,))
                            
                            connection.commit()


                            query = f"SELECT id, whatsapp FROM {table_name} WHERE id = {str(employee_number)} ;"
                            cursor.execute(query, )
                            rows = cursor.fetchall()

                            df_employees = pd.DataFrame(rows, columns=["id", "whatsapp"])
                            whatsapp = df_employees.iat[0, 1]
                            approovvver = approver_name.title()
                            companyxx = table_name.replace("main", "").replace("_", " ").title()

                            send_whatsapp_message(f"263{whatsapp}", f"✅ Great News {first_name} from {companyxx}'s {department} department! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}` has been submitted successfully!\n\n"
                                f"Your Leave Application ID is `{app_id}`.\n\n"
                                f"A Notification has been sent to `{approovvver}`  on `+263{approver_whatsapp}` to decide on  your application.\n\n"
                                "To Check the approval status of your leave application, type `Hello` then select `Track Application`.")
                            
                            if approver_whatsapp:

                                buttons = [
                                    {"type": "reply", "reply": {"id": f"Approve5appwa_{app_id}", "title": "Approve"}},
                                    {"type": "reply", "reply": {"id": f"Disapproveappwa_{app_id}", "title": "Disapprove"}},
                                ]
                                send_whatsapp_message(
                                    f"263{approver_whatsapp}", 
                                    f"Hey {approovvver}! 😊. New `{leave_type}` Leave Application from `{first_name} {surname}` in {department} department for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}`.\n\n" 
                                    f"Select an option below to either approve or disapprove the application."         
                                    , 
                                    buttons
                                )

                            print("Insert successful!")

                        except Exception as e:

                            print("Error inserting data:", e)       

                            return jsonify({"message": "Error re-applying leave application.", "error": str(e)}), 500

                    return jsonify({"message": f"Leave Application {app_id} re-applied successfully."})
                
                except Exception as e:
                    return jsonify({"message": "Error re-applying leave application.", "error": str(e)}), 500
                
            else: 

                response = {'status': 'error', 'message': 'Leave re-application not submitted successfully.'}
                return jsonify(response), 400  
            
    @app.route('/delete-all-tables', methods=['POST'])
    def handle_delete_all_tables():
        try:
            delete_all_tables()  # Your function
            return jsonify({"message": "All tables deleted successfully"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    @app.route('/revoke_leave_application', methods=['POST'])
    def revoke_leave_application():
        user_uuid = session.get('user_uuid')
        empid = session.get('empid')

        if user_uuid:
            print("Received data:", request.data)  # log the raw data
            print("JSON data:", request.get_json())  # log the parsed JSON
            table_name = session.get('table_name')
            company_name = table_name.replace("main","")
            table_name_apps_pending_approval = f"{company_name}appspendingapproval"
            table_name_apps_approved = f"{company_name}appsapproved"


            query = f"SELECT * FROM {table_name_apps_pending_approval} WHERE id = %s;"
            cursor.execute(query, (empid,))
            rows = cursor.fetchall()
            df_employees_reapp_check = pd.DataFrame(rows)    

            if len(df_employees_reapp_check) == 0:

                try:
                    data = request.get_json()
                    app_id = data.get("app_id")
                    print ("eissssssssshhhhhhhhhhhhhhhhhhhhhhhhhhhh")
                    print (app_id)

                    if not app_id:
                        return jsonify({"message": "Application ID is missing."}), 400
                    
                    status = "Pending"

                    query = f"SELECT * FROM {table_name_apps_approved} WHERE appid = %s;"
                    cursor.execute(query, (app_id,))
                    result = cursor.fetchone()
                    
                    if result :
                        app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf, statuspre, status_date = result
                        print("cancelled yes")
                        print(result)

                        print("cancelled yes")
    
                        print("chiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                        print(employee_number)
                        print(approver_name)

                        try:
                            insert_query = f"""
                            INSERT INTO {table_name_apps_pending_approval} 
                            (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
                            
                            cursor.execute(insert_query, (
                                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                                approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                                end_date, leave_days, leavedaysbalancebf, status
                            ))
                            
                            query = f"""DELETE FROM {table_name_apps_approved} WHERE appid = %s"""
                            cursor.execute(query, (app_id,))

                            query = f"""SELECT currentleavedaysbalance FROM {table_name} WHERE id = %s;"""
                            cursor.execute(query, (employee_number,))
                            leavedayscf = cursor.fetchone()
                            leavedayscf = int(leavedayscf[0])
                            print(leavedayscf)

                            new_current_balance = leavedayscf + leave_days
                            query = f"UPDATE {table_name} SET currentleavedaysbalance = %s WHERE id = %s;"
                            cursor.execute(query, (new_current_balance, employee_number))

                            connection.commit()
                            print("Insert successful!")

                        except Exception as e:
                            print("Error inserting data:", e)
                            return jsonify({"message": "Error revoking leave application.", "error": str(e)}), 500

                    return jsonify({"message": f"Leave Application {app_id} Revocation applied successfully."})
                
                except Exception as e:
                    return jsonify({"message": "Error applying for revocation of leave application.", "error": str(e)}), 500
                
            else: 

                response = {'status': 'error', 'message': 'Leave revocation application not submitted successfully.'}
                return jsonify(response), 400  
            

            

    @app.route('/revoke_leave_application_approver', methods=['POST'])
    def revoke_leave_application_approver():
        user_uuid = session.get('user_uuid')

        print("Received data:", request.data)  # log the raw data
        print("JSON data:", request.get_json())  # log the parsed JSON

        if user_uuid:
            data = request.get_json()
            app_id = data.get("app_id")

            table_name = session.get('table_name')
            company_name = table_name.replace("main","")
            table_name_apps_approved = f"{company_name}appsapproved"
            table_name_apps_declined = f"{company_name}appsdeclined"

            try:

                print ("eissssssssshhhhhhhhhhhhhhhhhhhhhhhhhhhh")
                print (app_id)

                if not app_id:
                    return jsonify({"message": "Application ID is missing."}), 400
                
                status = "Declined"

                query = f"SELECT * FROM {table_name_apps_approved} WHERE appid = %s;"
                cursor.execute(query, (app_id,))
                result = cursor.fetchone()
                
                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf, statuspre, status_date = result
                print("approved yes")
                print(result)

                print("approved yes")

                print("chiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                print(employee_number)
                print(approver_name)

                try:
                    insert_query = f"""
                    INSERT INTO {table_name_apps_declined} 
                    (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
                    
                    cursor.execute(insert_query, (
                        app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                        approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                        end_date, leave_days, leavedaysbalancebf, status, today_date
                    ))

                    query = f"""SELECT currentleavedaysbalance FROM {table_name} WHERE id = %s;"""
                    cursor.execute(query, (employee_number,))
                    leavedayscf = cursor.fetchone()
                    leavedayscf = int(leavedayscf[0])
                    print(leavedayscf)

                    new_current_balance = leavedayscf + leave_days
                    query = f"UPDATE {table_name} SET currentleavedaysbalance = %s WHERE id = %s;"
                    cursor.execute(query, (new_current_balance, employee_number))
                    connection.commit()

                    query = f"""DELETE FROM {table_name_apps_approved} WHERE appid = %s"""
                    cursor.execute(query, (app_id,))

                    connection.commit()
                    print("Insert successful!")


                except Exception as e:
                    print("Error inserting data:", e)
                    return jsonify({"message": "Error revoking leave application.", "error": str(e)}), 500

                return jsonify({"message": f"Leave Application {app_id} revoked successfully."})
            
            except Exception as e:
                return jsonify({"message": "Error re-applying leave application.", "error": str(e)}), 500
                




    @app.route('/download_leave_app/<app_id>')
    def download_pdf(app_id):
        global today_date
        user_uuid = session.get('user_uuid')
        empid = session.get('empid')

        if user_uuid:
            table_name = session.get('table_name')
            company_name = table_name.replace("main","")
            try:
                table_name_apps_approved = company_name + 'appsapproved'
                query = f"""SELECT appid, id, firstname, surname, leavetype, TO_CHAR(dateapplied, 'FMDD Month YYYY') AS dateapplied, TO_CHAR(leavestartdate, 'FMDD Month YYYY') AS leavestartdate, TO_CHAR(leaveenddate, 'FMDD Month YYYY') AS leaveenddate,  leavedaysappliedfor, leaveapprovername, TO_CHAR(statusdate, 'FMDD Month YYYY') AS statusdate, leavedaysbalancebf, leaveapproverid, department FROM {table_name_apps_approved} WHERE appid = %s;"""
                cursor.execute(query, (app_id,))  
                rows = cursor.fetchall()
                df_leave_appsmain_approved = pd.DataFrame(rows, columns=["App ID","ID","First Name", "Surname", "Leave Type","Date Applied", "Leave Start Date", "Leave End Date", "Leave Days","Leave Approver", "Status Date","New Leave Days Balance", "Leave Approver ID", "Department"])

                query = f"SELECT id, firstname, surname, whatsapp, email, address, role, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, monthlyaccumulation FROM {table_name};"
                cursor.execute(query)
                rows = cursor.fetchall()

                df_employees = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Address", "Role","Leave Approver Name","Leave Approver ID","Leave Approver Email", "Leave Approver WhatsAapp", "Leave Days Balance","Days Accumulated per Month"])
                print(df_employees)
                userdf = df_employees[df_employees['id'] == empid].reset_index()
                print("yeaarrrrr")

                employee_name = f"{df_leave_appsmain_approved.iat[0,2].title()} {df_leave_appsmain_approved.iat[0,3].title()}"
                leave_type = df_leave_appsmain_approved.iat[0,4].title()
                company_name_doc = company_name.replace("_"," ").title()

                def get_logo_base64():
                    logo_path = os.path.join(app.static_folder, 'images', 'eds logo blue.png')
                    with open(logo_path, "rb") as img_file:
                        return "data:image/png;base64," + base64.b64encode(img_file.read()).decode('utf-8')

                application = {
                    'company_logo': get_logo_base64() ,
                    'company_name': company_name_doc,
                    'employee_name': employee_name,
                    'employee_id': df_leave_appsmain_approved.iat[0,1],
                    'leave_type': leave_type,
                    'generated_on': today_date,
                    'date_applied': df_leave_appsmain_approved.iat[0,5],
                    'approver_name': df_leave_appsmain_approved.iat[0,9].title(),
                    'approver_id': df_leave_appsmain_approved.iat[0,12],
                    'department': df_leave_appsmain_approved.iat[0,13],
                    'reference_number': app_id,
                    'approved_date': df_leave_appsmain_approved.iat[0,10],
                    'new_balance': df_leave_appsmain_approved.iat[0,11],
                    'start_date': df_leave_appsmain_approved.iat[0,6],
                    'end_date': df_leave_appsmain_approved.iat[0,7],
                    'days_requested': df_leave_appsmain_approved.iat[0,8], 
                    'address': userdf.iat[0,6], 
                    'whatsapp': f"0{userdf.iat[0,4]}", 
                    'email': userdf.iat[0,5], 
                    'status': 'Approved'
                }
                
                # 2. Render HTML
                html = render_template('leave_pdf_template.html', app=application)
                
                # 3. Generate PDF
                pdf = HTML(string=html).write_pdf()
                
                # 4. Create response
                response = make_response(pdf)
                response.headers['Content-Type'] = 'application/pdf'
                response.headers['Content-Disposition'] = \
                    f'attachment; filename=leave_application_{company_name}_{app_id}.pdf'
                
                return response
                
            except Exception as e:
                return str(e), 500
            
    @app.route('/revive_leave_application', methods=['POST'])
    def revive_leave_application():
        user_uuid = session.get('user_uuid')

        print("Received data:", request.data)  # log the raw data
        print("JSON data:", request.get_json())  # log the parsed JSON

        if user_uuid:
            data = request.get_json()
            app_id = data.get("app_id")

            table_name = session.get('table_name')
            company_name = table_name.replace("main","")
            table_name_apps_pending_approval = f"{company_name}appspendingapproval"
            table_name_apps_cancelled = f"{company_name}appscancelled"
            table_name_apps_declined = f"{company_name}appsdeclined"

            query = f"SELECT id FROM {table_name_apps_declined} WHERE appid = %s;"
            cursor.execute(query, (app_id,))
            res = cursor.fetchone()
            employee_number_check = res

            query = f"SELECT * FROM {table_name_apps_pending_approval} WHERE id = %s;"
            cursor.execute(query, (employee_number_check,))
            rows = cursor.fetchall()
            df_employees_reapp_check = pd.DataFrame(rows)    

            if len(df_employees_reapp_check) == 0:

                try:

                    print ("eissssssssshhhhhhhhhhhhhhhhhhhhhhhhhhhh")
                    print (app_id)

                    if not app_id:
                        return jsonify({"message": "Application ID is missing."}), 400
                    
                    status = "Pending"

                    query = f"SELECT * FROM {table_name_apps_declined} WHERE appid = %s;"
                    cursor.execute(query, (app_id,))
                    result = cursor.fetchone()
                    
                    app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf, statuspre, status_date = result
                    print("declined yes")
                    print(result)

                    print("declined yes")

                    print("chiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                    print(employee_number)
                    print(approver_name)

                    try:
                        insert_query = f"""
                        INSERT INTO {table_name_apps_pending_approval} 
                        (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """
                        
                        cursor.execute(insert_query, (
                            app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                            approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                            end_date, leave_days, leavedaysbalancebf, status
                        ))
                        
                        query = f"""DELETE FROM {table_name_apps_declined} WHERE appid = %s"""
                        cursor.execute(query, (app_id,))

                        connection.commit()
                        print("Insert successful!")

                    except Exception as e:
                        print("Error inserting data:", e)
                        return jsonify({"message": "Error re-applying leave application.", "error": str(e)}), 500


                    return jsonify({"message": f"Leave Application {app_id} revived successfully."})
                
                except Exception as e:
                    return jsonify({"message": "Error re-applying leave application.", "error": str(e)}), 500
                
            else: 

                response = {'status': 'error', 'message': 'Leave application not submitted successfully.'}
                return jsonify(response), 400  

    @app.route('/approve_leave_application', methods=['POST'])
    def approve_leave_application():
        user_uuid = session.get('user_uuid')

        print("Received data:", request.data)  # log the raw data
        print("JSON data:", request.get_json())  # log the parsed JSON

        if user_uuid:

            try:
                data = request.get_json()
                app_id = data.get("app_id")
                print ("eissssssssshhhhhhhhhhhhhhhhhhhhhhhhhhhh")
                print (app_id)
                table_name = session.get('table_name')
                company_name = table_name.replace("main","")
                table_name_apps_pending_approval = f"{company_name}appspendingapproval"
                table_name_apps_approved = f"{company_name}appsapproved"

                if not app_id:
                    return jsonify({"message": "Application ID is missing."}), 400

                status = "Approved"
                statusdate = today_date

                query = f"SELECT * FROM {table_name_apps_pending_approval} WHERE appid = %s;"
                cursor.execute(query, (app_id,))
                result = cursor.fetchone()
                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf, statuspre = result
                print("chiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                print(employee_number)
                print(approver_name)

                try:
                    insert_query = f"""
                    INSERT INTO {table_name_apps_approved} 
                    (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
                    
                    cursor.execute(insert_query, (
                        app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                        approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                        end_date, leave_days, leavedaysbalancebf, status, statusdate
                    ))
                    
                    connection.commit()
                    print("Insert successful!")

                    query = f"UPDATE {table_name} SET currentleavedaysbalance = %s WHERE id = %s;"
                    cursor.execute(query, (leavedaysbalancebf, employee_number))
                    connection.commit()

                except Exception as e:
                    print("Error inserting data:", e)

                query = f"""DELETE FROM {table_name_apps_pending_approval} WHERE appid = %s"""
                cursor.execute(query, (app_id,))
                connection.commit()

                query = f"SELECT id, firstname, surname, whatsapp, email, address, role, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, monthlyaccumulation, department FROM {table_name};"
                cursor.execute(query)
                rows = cursor.fetchall()

                df_employees = pd.DataFrame(rows, columns=["id","firstname", "surname", "whatsapp","Email", "Address", "Role","Leave Approver Name","Leave Approver ID","Leave Approver Email", "Leave Approver WhatsAapp", "Leave Days Balance","Days Accumulated per Month", "Department"])
                print(df_employees)
                userdf = df_employees[df_employees['id'] == int(np.int64(employee_number))].reset_index()
                print("yeaarrrrr")
                print(userdf)
                firstname = userdf.iat[0,2].title()
                surname = userdf.iat[0,3].title()
                whatsappemp = userdf.iat[0,4]
                email = userdf.iat[0,5]
                address = userdf.iat[0,6]
                companyxx = company_name.replace("_", " ").title()
                app_namexx = approver_name.title()

                query = f"SELECT appid, id, leavetype, leaveapprovername, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, approvalstatus, statusdate, leavedaysbalancebf, department  FROM {table_name_apps_approved} WHERE id = {str(employee_number)};"
                cursor.execute(query)
                rows = cursor.fetchall()
                df_employeesappsapprovedcheck = pd.DataFrame(rows, columns=["appid","id", "leavetype", "leaveapprovername", "dateapplied", "leavestartdate", "leaveenddate", "leavedaysappliedfor","approvalstatus","statusdate", "leavedaysbalancebf", "department"]) 

                df_employeesappsapprovedcheck = df_employeesappsapprovedcheck.sort_values(by="appid", ascending=False)  

                send_whatsapp_message(f"263{whatsappemp}", f"✅ Great News {firstname} {surname} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}` has been Approved ✅ by `{app_namexx}`!")


                def generate_leave_pdf():
                    app = {
                        'company_logo': 44,
                        'company_name': companyxx,
                        'employee_name': f"{first_name} {surname}",
                        'leave_type': leave_type,
                        'generated_on': today_date,
                        'date_applied': df_employeesappsapprovedcheck.iat[0,4].strftime('%d %B %Y'),
                        'approver_name': df_employeesappsapprovedcheck.iat[0,3].title(),
                        'reference_number': df_employeesappsapprovedcheck.iat[0,0],
                        'approved_date': df_employeesappsapprovedcheck.iat[0,9].strftime('%d %B %Y'),
                        'new_balance': df_employeesappsapprovedcheck.iat[0,10],
                        'start_date':  df_employeesappsapprovedcheck.iat[0,5].strftime('%d %B %Y'),
                        'end_date':  df_employeesappsapprovedcheck.iat[0,6].strftime('%d %B %Y'),
                        'days_requested':  df_employeesappsapprovedcheck.iat[0,7], 
                        'department': df_employeesappsapprovedcheck.iat[0,11],
                        'address': address, 
                        'whatsapp': f"+263{whatsappemp}", 
                        'email': email, 
                        'status': 'Approved'
                    }

                    html_out = render_template("leave_pdf_template.html", app=app)
                    
                    # ✅ Return as bytes instead of saving to file
                    pdf_bytes = HTML(string=html_out).write_pdf()
                    return pdf_bytes

                
                global ACCESS_TOKEN
                global PHONE_NUMBER_ID

                def upload_pdf_to_whatsapp(pdf_bytes):
                    filename=f"leave_application_{df_employeesappsapprovedcheck.iat[0,0]}_{first_name}_{surname}_{companyxx}.pdf"
                
                    url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/media"
                    headers = {
                        "Authorization": f"Bearer {ACCESS_TOKEN}"
                    }

                    files = {
                        "file": (filename, io.BytesIO(pdf_bytes), "application/pdf"),
                        "type": (None, "application/pdf"),
                        "messaging_product": (None, "whatsapp")
                    }

                    response = requests.post(url, headers=headers, files=files)
                    print("📥 Full incoming data:", response.text)  # Good for debugging
                    response.raise_for_status()
                    return response.json()["id"]

                                                                
                def send_whatsapp_pdf_by_media_id(recipient_number, media_id):
                    filename=f"leave_application_{df_employeesappsapprovedcheck.iat[0,0]}_{first_name}_{surname}_{companyxx}.pdf"
                    url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
                    headers = {
                        "Authorization": f"Bearer {ACCESS_TOKEN}",
                        "Content-Type": "application/json"
                    }
                    payload = {
                        "messaging_product": "whatsapp",
                        "to": recipient_number,
                        "type": "document",
                        "document": {
                            "id": media_id,            # Media ID from upload step
                            "filename": filename       # Desired file name on recipient's phone
                        }
                    }

                    response = requests.post(url, headers=headers, json=payload)
                    response.raise_for_status()
                    return response.json()


                pdf_path = generate_leave_pdf()
                media_id = upload_pdf_to_whatsapp(pdf_path)
                send_whatsapp_pdf_by_media_id(f"263{whatsappemp}", media_id)

                return jsonify({"message": f"Leave Application {app_id} approved successfully."})
            
            except Exception as e:
                return jsonify({"message": "Error approving leave application.", "error": str(e)}), 500
            

    @app.route('/disapprove_leave_application', methods=['POST'])
    def disapprove_leave_application():
        user_uuid = session.get('user_uuid')

        print("Received data:", request.data)  # log the raw data
        print("JSON data:", request.get_json())  # log the parsed JSON

        if user_uuid:

            try:
                data = request.get_json()
                app_id = data.get("app_id")
                print ("eissssssssshhhhhhhhhhhhhhhhhhhhhhhhhhhh")
                print (app_id)
                table_name = session.get('table_name')
                company_name = table_name.replace("main","")
                companyxx = company_name.replace("_", " ").title()
                table_name_apps_pending_approval = f"{company_name}appspendingapproval"
                table_name_apps_declined = f"{company_name}appsdeclined"

                if not app_id:
                    return jsonify({"message": "Application ID is missing."}), 400

                status = "Declined"
                statusdate = today_date

                query = f"SELECT * FROM {table_name_apps_pending_approval} WHERE appid = %s;"
                cursor.execute(query, (app_id,))
                result = cursor.fetchone()
                app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, end_date, leave_days, leavedaysbalancebf, statuspre = result
                print("chiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                print(employee_number)
                print(approver_name)
                try:
                    insert_query = f"""
                    INSERT INTO {table_name_apps_declined} 
                    (appid, id, firstname, surname, department, leavetype, reasonifother, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp, currentleavedaysbalance, dateapplied, leavestartdate, leaveenddate, leavedaysappliedfor, leavedaysbalancebf, approvalstatus, statusdate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
                    
                    cursor.execute(insert_query, (
                        app_id, employee_number, first_name, surname, department, leave_type, leave_specify, approver_name, 
                        approver_id, approver_email, approver_whatsapp, leave_days_balance, date_applied, start_date, 
                        end_date, leave_days, leavedaysbalancebf, status, statusdate
                    ))
                    
                    connection.commit()
                    print("Insert successful!")

                except Exception as e:
                    print("Error inserting data:", e)

                query = f"""DELETE FROM {table_name_apps_pending_approval} WHERE appid = %s"""
                cursor.execute(query, (app_id,))
                connection.commit()

                global ACCESS_TOKEN
                global PHONE_NUMBER_ID


                query = f"SELECT id, firstname, surname, whatsapp, email, address ,role, department,currentleavedaysbalance, monthlyaccumulation, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp  FROM {table_name};"
                cursor.execute(query)
                rows = cursor.fetchall()

                df_employeesx = pd.DataFrame(rows, columns=["ID","First Name", "Surname", "WhatsApp","Email", "Address", "Role", "Department","Leave Days Balance","Days Accumulated per Month","Leave Approver Name", "Leave Approver ID", "Leave Approver Email", "Leave Approver WhatsaApp"])
                userdff = df_employeesx[df_employeesx['id'] == int(np.int64(employee_number))].reset_index()

                whatsappapprover = userdff.iat[0, 13]
                whatsappemp = userdff.iat[0, 3]

                buttonsapproval = [
                    {"type": "reply", "reply": {"id": "Revokedis", "title": "Revoke Disapproval"}},
                    {"type": "reply", "reply": {"id": "Pending", "title": "Pending My Approval"}},
                ]

                send_whatsapp_message(f"263{whatsappapprover}", f"✅ Hey {approver_name} from {companyxx}! \n\n You have successfully disapproved `{first_name} {surname}`'s  `{leave_days} day` `{leave_type} Leave Application` running from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}`✅!")
                send_whatsapp_message(
                    f"263{whatsappapprover}",
                    "Select an option below to continue 👇y, or Type `Hello` to view all Approver options",
                    buttonsapproval
                )

                if whatsappemp:

                    buttons = [
                        {"type": "reply", "reply": {"id": "Reapply", "title": "Resubmit Application"}},
                        {"type": "reply", "reply": {"id": "Apply", "title": "Apply for Leave"}},
                        {"type": "reply", "reply": {"id": "Checkbal", "title": "Check Days Balance"}},
                    ]

                    send_whatsapp_message(f"263{whatsappemp}", f"✅ Oops, {first_name} {surname} from {companyxx}! \n\n Your `{leave_type} Leave Application` for `{leave_days} days` from `{start_date.strftime('%d %B %Y')}` to `{end_date.strftime('%d %B %Y')}`, has been disapproved ❌ by `{companyxx}`!")
                    send_whatsapp_message(
                        f"263{whatsappemp}",
                        "Select an option below to continue 👇",
                        buttons
                    )

                return jsonify({"message": f"Leave Application {app_id} declined successfully."})
            
            except Exception as e:
                return jsonify({"message": "Error approving leave application.", "error": str(e)}), 500


    @app.route('/export_lms_book_excel')
    def export_excel():
        user_uuid = session.get('user_uuid')
        if user_uuid:

            table_name = session.get('table_name')
            company_name = table_name.replace("main","")


            query = f"SELECT id, firstname, surname, whatsapp, email, address ,role, department,currentleavedaysbalance, monthlyaccumulation, leaveapprovername, leaveapproverid, leaveapproveremail, leaveapproverwhatsapp  FROM {table_name};"
            cursor.execute(query)
            rows = cursor.fetchall()

            df_employees = pd.DataFrame(rows, columns=["ID","First Name", "Surname", "WhatsApp","Email", "Address", "Role", "Department","Leave Days Balance","Days Accumulated per Month","Leave Approver Name", "Leave Approver ID", "Leave Approver Email", "Leave Approver WhatsaApp"])
            df_employees = df_employees.sort_values(by="ID", ascending=True)

            print(df_employees)

            # Create an in-memory Excel file
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_employees.to_excel(writer, index=False, sheet_name=f'LMS Book {today_date}')

            output.seek(0)

            # Send the file to the client
            return send_file(
                output,
                as_attachment=True,
                download_name=f'{company_name} LMS Book as at {today_date}.xlsx',
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

    @app.route('/export_lms_book_pdf')
    def export_pdf():
        user_uuid = session.get('user_uuid')
        if user_uuid:
            table_name = session.get('table_name')
            if not table_name:
                return "Table name not found in session", 400

            company_name = table_name.replace("main", "").strip()

            query = f"SELECT id, firstname, surname, whatsapp, email, role, department, leaveapprovername, leaveapproverid, currentleavedaysbalance, monthlyaccumulation FROM {table_name};"
            cursor.execute(query)
            rows = cursor.fetchall()

            df_employees = pd.DataFrame(rows, columns=["ID", "First Name", "Surname", "WhatsApp", "Email", "Role", "Department", "Leave Approver Name","Leave Approver ID", "Leave Days Balance", "Days Accumulated per Month" ])
            df_employees = df_employees.sort_values(by="ID", ascending=True)

            df_html = df_employees.to_html(index=False, classes='table table-bordered', escape=False)

            html_content = f"""
            <html>
            <head>
            <style>
                .table {{
                    width: 100%;
                    border-collapse: collapse;
                }}
                .table th, .table td {{
                    border: 1px solid #ddd;
                    padding: 4px;
                    text-align: left;
                    max-width: 500px;  /* Limit cell width */
                    word-wrap: break-word;  /* Enables wrapping */
                    overflow-wrap: break-word;  /* Ensures wrapping in older browsers */
                    white-space: normal;  /* Forces the text to wrap */
                }}
                .table th {{
                    background-color: #003366;
                    color: white;
                }}
                tr {{
                    word-wrap: break-word;
                }}
                /* Specific styles for the email column */
                td.Email {{
                    max-width: 250px;  /* Limit email column width */
                    word-wrap: break-word;
                    overflow-wrap: break-word;
                    white-space: normal;
                }}
                th.Email {{
                    word-wrap: break-word;
                    overflow-wrap: break-word;
                }}
                h1 {{
                    text-align: center;
                    margin-bottom: 20px;
                }}
            </style>

            </head>
            <body>
                <h1>{company_name} LMS Book</h1>
                <p>As of {today_date}</p>
                {df_html}
            </body>
            </html>
            """

            output = BytesIO()
            pisa_status = pisa.CreatePDF(html_content, dest=output)

            if pisa_status.err:
                return "Failed to generate PDF", 500

            output.seek(0)

            return send_file(
                output,
                as_attachment=True,
                download_name=f'{company_name} LMS Book as at {today_date}.pdf',
                mimetype='application/pdf'
            )
        else:
            return "Unauthorized access", 401

    @app.route('/logout')
    def logout():
        # Clear the session data to log the user out
        session.clear()

        # Redirect to the landing page or login page after logout
        return redirect(url_for('explore_lms'))



            
else:
    print('Connection to SQL failed')

@app.route('/')
def landingpage():
    return render_template('main.html') 

@app.route('/echelon-digital-solutions-privacy-policy')
def privacypolicy():
    return render_template('privacypolicy.html') 


@app.route('/explore_lms')
def explore_lms():
    return render_template('index.html')  
    

if __name__ == "__main__":
    app.run(host='0.0.0.0', port= 55, debug=True)
