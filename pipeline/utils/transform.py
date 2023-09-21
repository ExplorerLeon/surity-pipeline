import pandas as pd
import datetime

def transformation(df):
    # Get company name
    # first_non_null_index = df[df.iloc[:, 0].notnull()].index[0]
    # company_name = df.iloc[first_non_null_index, 0]
    # # Print company name
    # print(company_name)

    # # Get the current date and time
    # current_time = datetime.datetime.now()
    # # Print the current time
    # print(current_time)

    # # Create a dictionary to represent the row data
    # row_data = {
    #     'ID': [10,11,12],  # Replace with the actual ID if available
    #     'CompanyName': [company_name,company_name,company_name],
    #     'Date': [current_time,current_time,current_time]
    #     }

    # # Create dataframe of dictionary
    # df = pd.DataFrame.from_dict(row_data, orient='index').T

    # new test
    df["timestamp"] = datetime.datetime.now()

    return df
