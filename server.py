import time
import pandas as pd
import numpy as np
from plaid import Client
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload
from googleapiclient.discovery import build

DEFAULT_INSTITUTION_ID = 'ins_1'
# Set Plaid API access data and get Client object
client = Client(
    client_id='5ece97fc33df8d00137c4a43',
    secret='c70bb94dccf11c154b80ed8a1c0213',
    public_key='f571792e76a5251f1892b3e5b1bc04',
    environment='sandbox'
)

# For upload files to google
GOOGLE_SCOPES = ['https://www.googleapis.com/auth/drive']
GOOGLE_SERVICE_ACCOUNT_FILE = '/Users/kateosti/Projects/new/quickstart/python/Quickstart.json'
google_credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_SERVICE_ACCOUNT_FILE, scopes=GOOGLE_SCOPES
)
service = build('drive', 'v3', credentials=google_credentials)


def google_upload(filepath, name):
    folder_id = '1DzhQWufGGqHKcGyZSKkxAa3nvofPPzRF4tInLDW_xyI'
    name = name
    file_path = filepath
    file_metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.spreadsheet',
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_path, resumable=True)
    r = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return r


def main(
        start_date='2020-01-01',
        end_date='2020-05-01',
        search_query="Bank of America",
):
    # Search for first id search_query
    response = client.Institutions.search(search_query)
    search_result = [{institute['name']: institute['institution_id']} for institute in response['institutions']]

    # Let's take first institute, name of which exactly matching to SEARCH_INSTITUTIONS_QUERY
    try:
        institution_id = [
            institute[search_query]
            for institute in search_result
            if list(institute.keys())[0]==search_query
        ][0]
    except IndexError:
        print(
            "Didn't find financial institutes matching this search query. Set INSTITUTION_ID to DEFAULT_INSTITUTION_ID"
        )
        institution_id = DEFAULT_INSTITUTION_ID

    # Generate a public_token for a given institution ID
    # and set of initial products
    create_response = client.Sandbox.public_token.create(institution_id, ['transactions'])

    # The generated public_token can now be
    # exchanged for an access_token
    exchange_response = client.Item.public_token.exchange(create_response['public_token'])

    time.sleep(5)

    # Get transactions by created access token
    response = client.Transactions.get(exchange_response['access_token'], start_date=start_date, end_date=end_date)
    transactions = response['transactions']

    time.sleep(5)

    # the transactions in the response are paginated, so make multiple calls while increasing the offset to
    # retrieve all transactions
    while len(transactions) < response['total_transactions']:
        response = client.Transactions.get(
            exchange_response['access_token'],
            start_date=start_date,
            end_date=end_date,
            offset=len(transactions)
        )
        transactions.extend(response['transactions'])

    # Create df_transactions_raw_data dataFrame for sending to Google Sheets
    df_transactions_raw_data = pd.DataFrame(transactions)
    # Create DataFrame from raw transactions and get columns only we need
    df_transactions = df_transactions_raw_data[['category', 'date', 'amount']]

    # Create column 'month' with specified format
    df_transactions['month'] = df_transactions['date'].apply(lambda s: s[:-3])

    # "Amount" is the settled dollar value.
    # Positive values when money moves out of the account; negative values when money moves in.
    # For example, purchases are positive; credit card payments, direct deposits, refunds are negative.
    # So, in this case we need to invert sign for it:
    #   - transactions like credit card payments must be positive;
    #   - transactions like purchases must be negative.
    df_transactions['Income/Expense'] = df_transactions['amount'].apply(lambda s: 'Expense' if s > 0 else 'Income')
    df_transactions['amount'] = df_transactions['amount'].apply(lambda s: -s)

    # Split categories for two parts: first category will be parent, and all other become children.
    df_transactions['Category 1'] = df_transactions['category'].apply(lambda s: s[0])
    df_transactions['Category 2'] = df_transactions['category'].apply(lambda s: s[1] if len(s)>1 else None)

    # Remove temporary columns
    df_transactions.drop(['category', 'date'], axis=1, inplace=True)

    pivot_table = df_transactions.pivot_table(
        values=['amount'],
        index=['Income/Expense', 'Category 1', 'Category 2'],
        columns='month',
        aggfunc=np.sum,
        margins=True,
        margins_name='Grand total'
    )

    # Create Expense subtotals
    pivot_expense_source = pivot_table.loc['Expense']
    pivot_expense_result = pd.concat([
        d.append(d.sum().rename(('', 'Total')))
        for k, d in pivot_expense_source.groupby(level=0)
    ])
    pivot_expense_result = pivot_expense_result.append(
        pivot_expense_source.sum().rename(('Total', ''))
    )

    # Create Income subtotals
    pivot_income_source = pivot_table.loc['Income']
    pivot_income_result = pd.concat([
        d.append(d.sum().rename(('', 'Total')))
        for k, d in pivot_income_source.groupby(level=0)
    ])
    pivot_income_result = pivot_income_result.append(
        pivot_income_source.sum().rename(('Total', ''))
    )

    # Concatenate all fragments
    pivot_result = pd.concat([
        pd.concat([pivot_income_result], keys=['Income'], names=['Income/Expense']),
        pd.concat([pivot_expense_result], keys=['Expense'], names=['Income/Expense']),
        pd.concat([pivot_table.loc['Grand total']], keys=['Grand total'], names=['Income/Expense'])
    ])

    # Rename months columns to format we need
    pivot_result.rename(
        columns={
            col: datetime.strptime(col, '%Y-%m').strftime('%b %Y')
            for col in pivot_result.columns.levels[1]
            if col != 'Grand total'
        },
        inplace=True
    )

    # Substitute NaN to '-' in result table
    pivot_result.fillna('-', inplace=True)

    # need openpyxl
    excel_writer = pd.ExcelWriter('ost_test.xlsx')
    df_transactions_raw_data.to_excel(excel_writer, 'Raw data')
    pivot_result.to_excel(excel_writer, 'Result sheet')
    excel_writer.save()

    google_upload('./ost_test.xlsx', 'Quickstart')

    return None


if __name__ == "__main__":
    main()
