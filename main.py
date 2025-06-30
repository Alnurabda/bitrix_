import os
import json
import pytz
from datetime import datetime
import pandas as pd
from flask import Flask, jsonify, request
from google.cloud import storage
from bitrix_client import BitrixClient
import requests

app = Flask(__name__)
storage_client = storage.Client()

def clear_daily_update(bucket_name: str, prefix: str):
    bucket = storage_client.bucket(bucket_name)
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith('.parquet'):
            blob.delete()

def upload_to_gcs(bucket_name: str, blob_name: str, data, content_type: str):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type=content_type)

def get_last_30_days_range():
    now = datetime.now(pytz.utc)
    start = now - pd.Timedelta(days=2)
    fmt = '%Y-%m-%dT%H:%M:%S%z'
    return {
        'start': start.strftime(fmt),
        'end': now.strftime(fmt),
        'file_date': now.strftime('%Y_%m_%d')
    }

def build_field_meta(fields: list) -> dict:
    meta = {}
    for field in fields:
        field_code = field.get("FIELD_NAME") or field.get("XML_ID")
        if not field_code:
            continue
        meta[field_code] = {
            "label": field.get("EDIT_FORM_LABEL", {}).get("ru", field_code),
            "type": field["USER_TYPE_ID"],
            "is_multiple": field.get("MULTIPLE", "N") == "Y",
            "enum": {e["ID"]: e["VALUE"] for e in field.get("LIST", [])} if field["USER_TYPE_ID"] == "enumeration" else {}
        }
    return meta
 
    
def decode_uf_field(value, meta: dict):
    # 1) если множественное — всегда собираем в список
    if meta.get("is_multiple", False):
        vals = value if isinstance(value, list) else ([value] if value is not None else [])
        return ", ".join(str(v) for v in vals if v is not None)

    # 2) перечисления — отображаем ID → label
    if meta["type"] == "enumeration":
        return meta["enum"].get(value, value)

    # 3) булевы — приводим "Y"/"N" → True/False
    if meta["type"] == "boolean":
        return True if value == "Y" else False

    # 4) всё остальное
    return value



def flatten_items(items: list, field_meta: dict, field_mapping: dict, standard_fields: list):
    result = []
    label_types = {"enumeration", "boolean", "crm_status", "crm", "crm_company", "crm_contact", "employee"}

    for item in items:
        row = {f: item.get(f) for f in standard_fields}
        for field_code, meta in field_meta.items():
            raw_value = item.get(field_code)
            if field_code in field_mapping:
                new_name = field_mapping[field_code]
                row[new_name] = decode_uf_field(raw_value, meta)
            else:
                row[field_code] = raw_value
        result.append(row)
    return result

@app.route('/', methods=['GET'])
def run_pipeline():
    try:
        cfg = {
            'BITRIX_LEAD_URL': os.environ['BITRIX_LEAD_URL'],
            'BITRIX_LEAD_USERFIELDS_URL': os.environ['BITRIX_LEAD_USERFIELDS_URL'],
            'BITRIX_DEAL_URL': os.environ['BITRIX_DEAL_URL'],
            'BITRIX_DEAL_USERFIELDS_URL': os.environ['BITRIX_DEAL_USERFIELDS_URL'],
            'BITRIX_CONTACT_LIST_URL': os.environ['BITRIX_CONTACT_LIST_URL'],
            'BITRIX_STATUS_LIST_URL': os.environ['BITRIX_STATUS_LIST_URL'],
            'BITRIX_COMPANY_LIST_URL': os.environ['BITRIX_COMPANY_LIST_URL'],
            'BITRIX_DEALCATEGORY_LIST_URL': os.environ['BITRIX_DEALCATEGORY_LIST_URL'],            
            'BITRIX_USER_LIST_URL': os.environ['BITRIX_USER_LIST_URL'],
            'GCS_RAW_BUCKET': os.environ['GCS_RAW_BUCKET'],
            'GCS_STAGING_BUCKET': os.environ['GCS_STAGING_BUCKET'],
            'PAGE_SIZE': int(os.getenv('PAGE_SIZE', 50))
        }


        dr = get_last_30_days_range()
        fd = dr['file_date']
        result = {'leads': 0, 'deals': 0, 'contacts': 0, 'status_list': 0, 'company_list': 0,'deal_category_list': 0, 'user_list': 0}
        now = datetime.now()
        y, m = now.strftime('%Y'), now.strftime('%m')

        # 1. Загрузка метаинформации полей
        lead_fields = requests.get(cfg['BITRIX_LEAD_USERFIELDS_URL']).json().get('result', [])
        deal_fields = requests.get(cfg['BITRIX_DEAL_USERFIELDS_URL']).json().get('result', [])

        upload_to_gcs(cfg['GCS_RAW_BUCKET'], f'bitrix/leads/meta/{fd}_leads_bitrix24.json',
                      json.dumps(lead_fields, ensure_ascii=False, indent=2), 'application/json')
        upload_to_gcs(cfg['GCS_RAW_BUCKET'], f'bitrix/deals/meta/{fd}_deals_bitrix24.json',
                      json.dumps(deal_fields, ensure_ascii=False, indent=2), 'application/json')

        # 2. Обработка лидов
        lead_client = BitrixClient(cfg['BITRIX_LEAD_URL'], cfg['PAGE_SIZE'])
        leads = lead_client.fetch(dr['start'], dr['end'])
        result['leads'] = len(leads)

        if leads:
            field_meta_leads = build_field_meta(lead_fields)
            decoded_leads = flatten_items(
                leads, field_meta_leads, lead_client.get_bq_field_mapping(),
                ["ID", "TITLE", "HONORIFIC", "NAME", "SECOND_NAME", "LAST_NAME", "COMPANY_TITLE", "COMPANY_ID", "CONTACT_ID", "IS_RETURN_CUSTOMER", "BIRTHDATE", "SOURCE_ID", "SOURCE_DESCRIPTION", "STATUS_ID", "STATUS_DESCRIPTION", "POST", "COMMENTS", "CURRENCY_ID", "OPPORTUNITY", "IS_MANUAL_OPPORTUNITY", "HAS_PHONE", "HAS_EMAIL", "HAS_IMOL", "ASSIGNED_BY_ID", "CREATED_BY_ID", "MODIFY_BY_ID", "DATE_CREATE", "DATE_MODIFY", "DATE_CLOSED", "STATUS_SEMANTIC_ID", "OPENED", "ORIGINATOR_ID", "ORIGIN_ID", "MOVED_BY_ID", "MOVED_TIME", "ADDRESS", "ADDRESS_2", "ADDRESS_CITY", "ADDRESS_POSTAL_CODE", "ADDRESS_REGION", "ADDRESS_PROVINCE", "ADDRESS_COUNTRY", "ADDRESS_COUNTRY_CODE", "ADDRESS_LOC_ADDR_ID", "UTM_SOURCE", "UTM_MEDIUM", "UTM_CAMPAIGN", "UTM_CONTENT", "UTM_TERM", "LAST_COMMUNICATION_TIME", "LAST_ACTIVITY_BY", "LAST_ACTIVITY_TIME"]
            )
            df = pd.DataFrame(decoded_leads).where(pd.notnull, None)
            pq = df.to_parquet(engine='pyarrow')
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/leads/archive/{fd}_leads_bitrix24.parquet', pq, 'application/octet-stream')
            clear_daily_update(cfg['GCS_STAGING_BUCKET'], 'bitrix/leads/daily_update/')
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/leads/daily_update/{fd}_leads_bitrix24.parquet', pq, 'application/octet-stream')

        # 3. Обработка сделок
        deal_client = BitrixClient(cfg['BITRIX_DEAL_URL'], cfg['PAGE_SIZE'])
        deals = deal_client.fetch(dr['start'], dr['end'])
        result['deals'] = len(deals)

        if deals:
            field_meta_deals = build_field_meta(deal_fields)
            decoded_deals = flatten_items(
                deals, field_meta_deals, deal_client.get_bq_field_mapping(),
                ["ID", "TITLE", "TYPE_ID", "STAGE_ID", "PROBABILITY", "CURRENCY_ID", "OPPORTUNITY", "IS_MANUAL_OPPORTUNITY", "TAX_VALUE", "LEAD_ID", "COMPANY_ID", "CONTACT_ID", "QUOTE_ID", "BEGINDATE", "CLOSEDATE", "ASSIGNED_BY_ID", "CREATED_BY_ID", "MODIFY_BY_ID", "DATE_CREATE", "DATE_MODIFY", "OPENED", "CLOSED", "COMMENTS", "ADDITIONAL_INFO", "LOCATION_ID", "CATEGORY_ID", "STAGE_SEMANTIC_ID", "IS_NEW", "IS_RECURRING", "IS_RETURN_CUSTOMER", "IS_REPEATED_APPROACH", "SOURCE_ID", "SOURCE_DESCRIPTION", "ORIGINATOR_ID", "ORIGIN_ID", "MOVED_BY_ID", "MOVED_TIME", "LAST_ACTIVITY_TIME", "UTM_SOURCE", "UTM_MEDIUM", "UTM_CAMPAIGN", "UTM_CONTENT", "UTM_TERM", "PARENT_ID_1078", "PARENT_ID_1088", "PARENT_ID_1098", "PARENT_ID_1102", "PARENT_ID_1116", "PARENT_ID_1122", "PARENT_ID_1126", "PARENT_ID_1138", "PARENT_ID_1142", "PARENT_ID_1152", "PARENT_ID_1156", "LAST_COMMUNICATION_TIME", "LAST_ACTIVITY_BY"
]
            )
            df = pd.DataFrame(decoded_deals).where(pd.notnull, None)
            pq = df.to_parquet(engine='pyarrow')
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/deals/archive/{fd}_deals_bitrix24.parquet', pq, 'application/octet-stream')
            clear_daily_update(cfg['GCS_STAGING_BUCKET'], 'bitrix/deals/daily_update/')
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/deals/daily_update/{fd}_deals_bitrix24.parquet', pq, 'application/octet-stream')

        # 4. Обработка контактов 
        contact_client = BitrixClient(cfg['BITRIX_CONTACT_LIST_URL'], cfg['PAGE_SIZE'])
        contacts = contact_client.fetch(dr['start'], dr['end'])
        result['contacts'] = len(contacts)

        if contacts:
            # RAW JSON архив
            upload_to_gcs(cfg['GCS_RAW_BUCKET'], f'bitrix/contacts/archive/{y}/{m}/{fd}_contacts_bitrix24.json',
                          json.dumps(contacts, ensure_ascii=False, indent=2), 'application/json')

            # Преобразование в Parquet
            df = pd.DataFrame(contacts).where(pd.notnull, None)
            pq = df.to_parquet(engine='pyarrow')

            # Архив и daily_update
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/contacts/archive/{y}/{m}/{fd}_contacts_bitrix24.parquet', pq, 'application/octet-stream')
            clear_daily_update(cfg['GCS_STAGING_BUCKET'], 'bitrix/contacts/daily_update/')
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/contacts/daily_update/{fd}_contacts_bitrix24.parquet', pq, 'application/octet-stream')

        # 5. Справочник статусов
        status_list = requests.get(cfg['BITRIX_STATUS_LIST_URL']).json().get('result', [])
        upload_to_gcs(cfg['GCS_RAW_BUCKET'], f'bitrix/metadata/{fd}_status_bitrix24.json',
                      json.dumps(status_list, ensure_ascii=False, indent=2), 'application/json')

        if status_list:
            df = pd.DataFrame(status_list).where(pd.notnull, None)
            pq = df.to_parquet(engine='pyarrow')
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/status_list/archive/{y}/{m}/{fd}_status_bitrix24.parquet', pq, 'application/octet-stream')
            clear_daily_update(cfg['GCS_STAGING_BUCKET'], 'bitrix/status_list/daily_update/')
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/status_list/daily_update/{fd}_status_bitrix24.parquet', pq, 'application/octet-stream')

        # 6. Компании      
        company_list = BitrixClient(cfg['BITRIX_COMPANY_LIST_URL'], cfg['PAGE_SIZE'])
        company_list = company_list.fetch(dr['start'], dr['end'])
        result['company_list'] = len(company_list)

        if company_list:
            # RAW JSON архив
            upload_to_gcs(cfg['GCS_RAW_BUCKET'], f'bitrix/company_list/archive/{y}/{m}/{fd}_company_list_bitrix24.json',
                          json.dumps(company_list, ensure_ascii=False, indent=2), 'application/json')

            # Преобразование в Parquet
            df = pd.DataFrame(company_list).where(pd.notnull, None)
            pq = df.to_parquet(engine='pyarrow')

            # Архив и daily_update
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/company_list/archive/{y}/{m}/{fd}_company_list_bitrix24.parquet', pq, 'application/octet-stream')
            clear_daily_update(cfg['GCS_STAGING_BUCKET'], 'bitrix/company_list/daily_update/')
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/company_list/daily_update/{fd}_company_list_bitrix24.parquet', pq, 'application/octet-stream')

        # 7. Категории сделок
        deal_category_list = requests.get(cfg['BITRIX_DEALCATEGORY_LIST_URL']).json().get('result', [])
        upload_to_gcs(cfg['GCS_RAW_BUCKET'], f'bitrix/deal_category_list/{fd}_deal_category_bitrix24.json',
                      json.dumps(deal_category_list, ensure_ascii=False, indent=2), 'application/json')

        if deal_category_list:
            df = pd.DataFrame(deal_category_list).where(pd.notnull, None)
            pq = df.to_parquet(engine='pyarrow')
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/deal_category_list/archive/{y}/{m}/{fd}_deal_category_bitrix24.parquet', pq, 'application/octet-stream')
            clear_daily_update(cfg['GCS_STAGING_BUCKET'], 'bitrix/deal_category_list/daily_update/')
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/deal_category_list/daily_update/{fd}_deal_category_bitrix24.parquet', pq, 'application/octet-stream')

        # 8. Пользователи 
        user_list = BitrixClient(cfg['BITRIX_USER_LIST_URL'], cfg['PAGE_SIZE'])
        user_list = user_list.fetch(dr['start'], dr['end'])
        result['user_list'] = len(user_list)

        if user_list:
            # RAW JSON архив
            upload_to_gcs(cfg['GCS_RAW_BUCKET'], f'bitrix/user_list/archive/{y}/{m}/{fd}_user_list_bitrix24.json',
                          json.dumps(contacts, ensure_ascii=False, indent=2), 'application/json')

            # Преобразование в Parquet
            df = pd.DataFrame(user_list).where(pd.notnull, None)
            pq = df.to_parquet(engine='pyarrow')

            # Архив и daily_update
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/user_list/archive/{y}/{m}/{fd}_user_list_bitrix24.parquet', pq, 'application/octet-stream')
            clear_daily_update(cfg['GCS_STAGING_BUCKET'], 'bitrix/user_list/daily_update/')
            upload_to_gcs(cfg['GCS_STAGING_BUCKET'], f'bitrix/user_list/daily_update/{fd}_user_list_bitrix24.parquet', pq, 'application/octet-stream')

         

        
        return jsonify({
            'status': 'success',
            'processed_counts': result,
            'total_leads': result['leads'],
            'total_deals': result['deals'],
            'total_contacts': result['contacts'],
            'total_user_list': result['user_list'],
            'company_list': result['company_list'],
            'total_deal_category_list': result['deal_category_list'],
            'total_status_list': result['status_list']
        }), 200

        

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True)
