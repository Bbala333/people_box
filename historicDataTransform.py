import pandas as pd
from datetime import datetime

# To read the csv file
df = pd.read_csv('/content/input.csv')

# To convert date columns to date time
date_columns = ['Date of Joining', 'Date of Exit', 'Compensation 1 date', 'Compensation 2 date',
                'Review 1 date', 'Review 2 date', 'Engagement 1 date', 'Engagement 2 date']
df[date_columns] = df[date_columns].apply(pd.to_datetime)

# To Define a function to transform data into historical records
def transform_to_historical(df):
    historical_records = []

    for index, row in df.iterrows():
        employee_code = row['Employee Code']
        manager_employee_code = row['Manager Employee Code']
        start_date = row['Date of Joining']
        end_date = row['Date of Exit'] if pd.notnull(row['Date of Exit']) else datetime(2100, 1, 1)

        # Add initial record
        historical_records.append({
            'Employee Code': employee_code,
            'Manager Employee Code': manager_employee_code,
            'Last Compensation': None,
            'Compensation': row['Compensation'],
            'Last Pay Raise Date': None,
            'Variable Pay': None,
            'Tenure in Org': None,
            'Performance Rating': None,
            'Engagement Score': None,
            'Effective Date': start_date,
            'End Date': end_date
        })

        # Add records for compensation changes
        if pd.notnull(row['Compensation 1 date']):
            historical_records.append({
                'Employee Code': employee_code,
                'Manager Employee Code': manager_employee_code,
                'Last Compensation': row['Compensation'],
                'Compensation': row['Compensation 1'],
                'Last Pay Raise Date': row['Compensation 1 date'],
                'Variable Pay': None,
                'Tenure in Org': None,
                'Performance Rating': None,
                'Engagement Score': None,
                'Effective Date': row['Compensation 1 date'],
                'End Date': row['Compensation 2 date'] if pd.notnull(row['Compensation 2 date']) else datetime(2100, 1, 1)
            })

        if pd.notnull(row['Compensation 2 date']):
            historical_records.append({
                'Employee Code': employee_code,
                'Manager Employee Code': manager_employee_code,
                'Last Compensation': row['Compensation 1'],
                'Compensation': row['Compensation 2'],
                'Last Pay Raise Date': row['Compensation 2 date'],
                'Variable Pay': None,
                'Tenure in Org': None,
                'Performance Rating': None,
                'Engagement Score': None,
                'Effective Date': row['Compensation 2 date'],
                'End Date': datetime(2100, 1, 1)
            })

        # Add records for performance reviews
        if pd.notnull(row['Review 1 date']):
            historical_records.append({
                'Employee Code': employee_code,
                'Manager Employee Code': manager_employee_code,
                'Effective Date': row['Review 1 date'],
                'End Date': row['Review 2 date'] if pd.notnull(row['Review 2 date']) else datetime(2100, 1, 1),
                'Performance Rating': row['Review 1'],
                'Last Compensation': row['Compensation 2'],
                'Compensation': row['Compensation 2'],
                'Last Pay Raise Date': row['Compensation 2 date'],
                'Variable Pay': None,
                'Tenure in Org': None,
                'Engagement Score': None
            })

        if pd.notnull(row['Review 2 date']):
            historical_records.append({
                'Employee Code': employee_code,
                'Manager Employee Code': manager_employee_code,
                'Effective Date': row['Review 2 date'],
                'End Date': datetime(2100, 1, 1),
                'Performance Rating': row['Review 2'],
                'Last Compensation': row['Compensation 2'],
                'Compensation': row['Compensation 2'],
                'Last Pay Raise Date': row['Compensation 2 date'],
                'Variable Pay': None,
                'Tenure in Org': None,
                'Engagement Score': None
            })

        # Add records for engagement
        if pd.notnull(row['Engagement 1 date']):
            historical_records.append({
                'Employee Code': employee_code,
                'Manager Employee Code': manager_employee_code,
                'Effective Date': row['Engagement 1 date'],
                'End Date': row['Engagement 2 date'] if pd.notnull(row['Engagement 2 date']) else datetime(2100, 1, 1),
                'Engagement Score': row['Engagement 1'],
                'Last Compensation': row['Compensation 2'],
                'Compensation': row['Compensation 2'],
                'Last Pay Raise Date': row['Compensation 2 date'],
                'Variable Pay': None,
                'Tenure in Org': None,
                'Performance Rating': None
            })

        if pd.notnull(row['Engagement 2 date']):
            historical_records.append({
                'Employee Code': employee_code,
                'Manager Employee Code': manager_employee_code,
                'Effective Date': row['Engagement 2 date'],
                'End Date': datetime(2100, 1, 1),
                'Engagement Score': row['Engagement 2'],
                'Last Compensation': row['Compensation 2'],
                'Compensation': row['Compensation 2'],
                'Last Pay Raise Date': row['Compensation 2 date'],
                'Variable Pay': None,
                'Tenure in Org': None,
                'Performance Rating': None
            })

    return pd.DataFrame(historical_records)

# Transform data into historical format
historical_records_df = transform_to_historical(df)

# Save historical records to a new CSV file
historical_records_df.to_csv('/content/output/historical_data.csv', index=False)
