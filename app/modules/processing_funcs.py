import pandas as pd
import json
from app.modules.data_fetching_func import Fraudtracker
from app.send_report import upload_to_sharepoint
from config import config
from datetime import datetime



def get_response(message):
    return {
        "predictions": message,
        "status": 200
    }

def get_fraud_report():
    conn = Fraudtracker(config.disb_date,
                        config.host_m,
                        config.port_m,
                        config.database_m,
                        config.user_m,
                        config.password_m,
                        config.host_o,
                        config.port_o,
                        config.database_o,
                        config.user_o,
                        config.password_o)

    # Getting the Data
    print("fetching mambu and oktupus data.............")
    mambu_data = conn.mambu_data()
    okt_data = conn.Okt_devicetracking_data()

    # Preprocess and Merge Oktopus & Mambu Data
    data = conn.merge_df(mambu_data, okt_data)
    data.drop_duplicates(inplace=True)
    print(data.shape)
    data['disbursement_date'] = pd.to_datetime(data['disbursement_date'])
    data = data[data['disbursement_date'] <= conn.disb_date]
    print(data.shape)

    # Case 1 - Same Name, Different BVN
    sndbvn = conn.SNDBVN(data)
    # Case 2 - Same BVN > 1 Loan
    sbvng1loan = conn.SBVNG1LOAN(sndbvn)
    # Case 3 - Same Phone Number, > 1 Loan
    spng1loan = conn.SPNG1LOAN(sbvng1loan)
    # Case 4 -  Same Device ID, > 1 Loan
    sdidg1loan = conn.SDIDG1LOAN(spng1loan)
    # Case 5 - Same Email Address, > 1 Loan
    seag1loan = conn.SEAG1LOAN(sdidg1loan)
    # Case 6
    sdobg1loan = conn.SDOBG1LOAN(seag1loan)
    # Case 7
    sang1loan = conn.SANG1LOAN(sdobg1loan)
    # Case 8
    mrbwbs = conn.MRBWBS(sang1loan)

    # Output Sheet 1
    cases = conn.get_frauddf()
    # Output Sheet 2
    final = conn.fraudCasesDataMerge(cases, data)
    # Output Sheet 3
    fraud_dist = conn.fraudDistribution(cases)
    # Output Sheet4
    fcaac = conn.fraudCasesAggAmountCount(final)
    # Output Sheet 5
    data_on_disb_date = conn.get_data_on_disb_date(data)

    today_date = datetime.now().strftime("%Y-%m-%d")  # Format: YYYY-MM-DD
    file = f"Fraud-Report-{today_date}.xlsx"

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(file, engine="xlsxwriter")

    # Write each dataframe to a different worksheet.
    cases.to_excel(writer, sheet_name="Cases")
    final.to_excel(writer, sheet_name="Alpha_Data")
    fraud_dist.to_excel(writer, sheet_name="Fraud_Distribution")
    fcaac.to_excel(writer, sheet_name="Cases_by_Volume_Value")
    data_on_disb_date.to_excel(writer, sheet_name="data_on_disb_date")
    writer.close()
    upload_to_sharepoint(file)
    return get_response("fraud report generated......")
