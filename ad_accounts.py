import requests
import os
from google.cloud import bigquery
from datetime import datetime, timezone

# KHÔNG hardcode credentials path nữa - sẽ được set từ GitHub Actions
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\MinhDuoc\Fb Ads\application_default_credentials.json'

class FacebookAdsToBigQuery:
    def __init__(self, access_token, project_id, dataset_id, table_id):
        self.access_token = access_token
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.api_version = "v24.0"
        
    def fetch_ad_account(self, account_id):
        url = f"https://graph.facebook.com/{self.api_version}/act_{account_id}"
        params = {
            'fields': ','.join([
                'account_id', 'name', 'account_status',
                'amount_spent', 'balance', 'currency', 'spend_cap',
                'created_time', 'timezone_name',
                'business_name', 'business_city', 'business_country_code',
                'disable_reason', 'is_personal', 'is_prepay_account'
            ]),
            'access_token': self.access_token
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            account_data = response.json()
            print(f"Fetched account: {account_data.get('name', account_id)}")
            return account_data
        except Exception as e:
            print(f"Error fetching account: {e}")
            return None
    
    def transform_data(self, account):
        if not account:
            return None
            
        current_timestamp = datetime.now(timezone.utc).isoformat()
        
        return {
            'account_id': account.get('account_id', ''),
            'name': account.get('name', ''),
            'account_status': account.get('account_status', 0),
            'amount_spent': float(account.get('amount_spent', 0)),
            'balance': float(account.get('balance', 0)),
            'currency': account.get('currency', ''),
            'spend_cap': float(account.get('spend_cap', 0)) if account.get('spend_cap') else None,
            'created_time': account.get('created_time', ''),
            'timezone_name': account.get('timezone_name', ''),
            'business_name': account.get('business_name', ''),
            'business_city': account.get('business_city', ''),
            'business_country_code': account.get('business_country_code', ''),
            'disable_reason': account.get('disable_reason', ''),
            'is_personal': account.get('is_personal', False),
            'is_prepay_account': account.get('is_prepay_account', False),
            'updated_at': current_timestamp
        }
    
    def upsert_to_bigquery(self, data):
        if not data:
            print("No data to upsert")
            return
            
        try:
            client = bigquery.Client(project=self.project_id)
            table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
            temp_table_id = f"{self.table_id}_temp"
            temp_table_ref = f"{self.project_id}.{self.dataset_id}.{temp_table_id}"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            
            job = client.load_table_from_json([data], temp_table_ref, job_config=job_config)
            job.result()
            print("Loaded data to temp table")
            
            merge_query = f"""
            MERGE `{table_ref}` T
            USING `{temp_table_ref}` S
            ON T.account_id = S.account_id
            WHEN MATCHED THEN
              UPDATE SET
                name = S.name,
                account_status = S.account_status,
                amount_spent = S.amount_spent,
                balance = S.balance,
                currency = S.currency,
                spend_cap = S.spend_cap,
                created_time = S.created_time,
                timezone_name = S.timezone_name,
                business_name = S.business_name,
                business_city = S.business_city,
                business_country_code = S.business_country_code,
                disable_reason = S.disable_reason,
                is_personal = S.is_personal,
                is_prepay_account = S.is_prepay_account,
                updated_at = S.updated_at
            WHEN NOT MATCHED THEN
              INSERT (
                account_id, name, account_status, amount_spent, balance,
                currency, spend_cap, created_time, timezone_name,
                business_name, business_city, business_country_code,
                disable_reason, is_personal, is_prepay_account, updated_at
              )
              VALUES (
                S.account_id, S.name, S.account_status, S.amount_spent, S.balance,
                S.currency, S.spend_cap, S.created_time, S.timezone_name,
                S.business_name, S.business_city, S.business_country_code,
                S.disable_reason, S.is_personal, S.is_prepay_account, S.updated_at
              )
            """
            
            query_job = client.query(merge_query)
            query_job.result()
            print(f"Upserted data into {table_ref}")
            
            client.delete_table(temp_table_ref, not_found_ok=True)
            print("Cleaned up temp table")
            
        except Exception as e:
            print(f"Error upserting to BigQuery: {e}")
            try:
                client.delete_table(temp_table_ref, not_found_ok=True)
            except:
                pass
            raise
    
    def run(self, account_id):
        print("Facebook Ads to BigQuery - Upsert Mode")
        print("-" * 60)
        
        account = self.fetch_ad_account(account_id)
        
        if not account:
            print("No account data")
            return
        
        account_data = self.transform_data(account)
        print("Transformed 1 record")
        
        self.upsert_to_bigquery(account_data)
        print("-" * 60)
        print("Pipeline completed")


def main():
    # Đọc tất cả giá trị từ environment variables
    FB_ACCESS_TOKEN = os.environ.get('FB_ACCESS_TOKEN')
    PROJECT_ID = os.environ.get('PROJECT_ID', 'atino-vietnam')
    DATASET_ID = os.environ.get('DATASET_ID', 'san_xuat')
    TABLE_ID = os.environ.get('TABLE_ID', 'ad_accounts')
    ACCOUNT_ID = os.environ.get('ACCOUNT_ID', '1125850342079893')
    
    # Kiểm tra FB_ACCESS_TOKEN có tồn tại không
    if not FB_ACCESS_TOKEN:
        raise ValueError("FB_ACCESS_TOKEN environment variable is required")
    
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Table: {TABLE_ID}")
    print(f"Account: {ACCOUNT_ID}")
    print("-" * 60)
    
    pipeline = FacebookAdsToBigQuery(
        access_token=FB_ACCESS_TOKEN,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID
    )
    
    pipeline.run(account_id=ACCOUNT_ID)


if __name__ == "__main__":
    main()