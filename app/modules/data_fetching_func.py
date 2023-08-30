import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime, timedelta


class Fraudtracker:

    def __init__(self, disb_date, mambu_host: str, mambu_port: str, mambu_database_name: str, mambu_user: str,
                 mambu_password: str, okt_host: str, okt_port: str, okt_database_name: str, okt_user: str,
                 okt_password: str):

        yesterday = datetime.now() - timedelta(days=1)
        self.disb_date = yesterday.strftime("%Y-%m-%d")
        self.mambu_host = mambu_host
        self.mambu_port = mambu_port
        self.mambu_database_name = mambu_database_name
        self.mambu_user = mambu_user
        self.mambu_password = mambu_password
        self.okt_host = okt_host
        self.okt_port = okt_port
        self.okt_database_name = okt_database_name
        self.okt_user = okt_user
        self.okt_password = okt_password
        self.fraud_df = pd.DataFrame(columns=['id', 'Fraud_Case'])

    # Get mambu data
    def mambu_data(self):

        conn = mysql.connector.connect(
            host=self.mambu_host,
            port=self.mambu_port,
            database=self.mambu_database_name,
            user=self.mambu_user,
            password=self.mambu_password
        )

        query = f'''SELECT 
        loan.id,
        cl.ID as BVN,
        CAST(cl.FIRSTNAME as CHAR) AS FIRSTNAME,
        cl.MIDDLENAME, 
        CAST(cl.LASTNAME as CHAR) AS LASTNAME,
        CAST(disb.disbursementdate AS DATE) AS disbursement_date,
        CAST(COALESCE(LoanChannel.value, Channel_Loan_V3.value) AS CHAR) AS Loan_Channel,
        CAST(loan.LOANNAME AS CHAR) AS LOANNAME,
        loan.LOANAMOUNT,
        CAST(loan.accountstate as CHAR) AS accountstate,
        CAST(cl.MOBILEPHONE1 as CHAR) AS MOBILEPHONE1,
        cl.BIRTHDATE,
        cl.EMAILADDRESS,
        cast(coalesce(repayment_bankv3.value, repayment_bank.value) as char) as repayment_bank,
        cast(repayment_no.value as char) as repayment_Acct_No,
        cast(bs_bank.value as char) as bs_bank,
        cast(bs_name.value as char) as bs_Acct_Name,
        cast(bs_no.VALUE as char) as bs_acct_no
        FROM
        mambu.loanaccount AS loan
            LEFT JOIN
        mambu.disbursementdetails AS disb ON loan.disbursementdetailskey = disb.encodedkey
            LEFT JOIN
        mambu.client AS cl ON loan.ACCOUNTHOLDERKEY = cl.ENCODEDKEY
            LEFT JOIN
        mambu.loanproduct AS loanproduct ON loan.PRODUCTTYPEKEY = loanproduct.ENCODEDKEY
        LEFT JOIN
        mambu.customfieldvalue AS repayment_bank ON repayment_bank.parentkey = loan.encodedkey
            AND repayment_bank.customfieldkey = '8a1a32474efe333c014f0e4fed476bea'
        LEFT JOIN
        mambu.customfieldvalue AS repayment_bankv3 ON repayment_bankv3.parentkey = loan.encodedkey
            AND repayment_bankv3.customfieldkey = '8a9f8f937d3852d7017d3880ce3a2931'
        LEFT JOIN
        mambu.customfieldvalue AS repayment_no ON repayment_no.parentkey = loan.encodedkey
            AND repayment_no.customfieldkey = '8a858faa56dffac60156e655f98d4c66'
        LEFT JOIN
        mambu.customfieldvalue AS bs_name ON bs_name.parentkey = loan.encodedkey
            AND bs_name.customfieldkey = '8a9f8ede7b0b1dd9017b0b5b5a524799'
        LEFT JOIN
        mambu.customfieldvalue AS bs_bank ON bs_bank.parentkey = loan.encodedkey
            AND bs_bank.customfieldkey = '8a9f8ede7b0b1dd9017b0b58678c4420'
        LEFT JOIN
        mambu.customfieldvalue AS bs_no ON bs_no.parentkey = loan.encodedkey
            AND bs_no.customfieldkey = '8a9f8ede7b0b1dd9017b0b5a7287467c'
        LEFT JOIN
        mambu.customfieldvalue AS Channel_Loan_V3 ON Channel_Loan_V3.parentkey = loan.encodedkey
            AND Channel_Loan_V3.customfieldkey = '8a9f8e367d372595017d38c219b41e6f'
            LEFT JOIN
        mambu.customfieldvalue AS LoanChannel ON LoanChannel.parentkey = loan.encodedkey
                AND LoanChannel.customfieldkey = '8a9f8eb070e2ca3e0170e3bafcf03ae7'

        WHERE
        loanproduct.id IN ('001_InstantPL0' , '001_instantselm',
            '002_instantselw',
            '001_instantpl',
            '001_INSTANTLOANS',
            '003_Instant2W')
        AND CAST(disb.disbursementdate AS DATE) IS NOT NULL 
        and  loan.accountstate like '%ACTIVE%'
        and coalesce(loan.accountsubstate, 'na') <> 'WITHDRAWN'

        GROUP BY loan.id
        
        '''
        cur = conn.cursor()

        result = pd.read_sql(query, conn)
        result['id'] = result['id'].astype(str)

        return result

    # Get Oktopus device traking data
    def Okt_devicetracking_data(self):

        conn = mysql.connector.connect(
            host=self.okt_host,
            port=self.okt_port,
            database=self.okt_database_name,
            user=self.okt_user,
            password=self.okt_password
        )

        query = f'''SELECT * FROM oktopus.device_tracking
                '''
        cur = conn.cursor()

        result = pd.read_sql(query, conn)
        result['id'] = result['id'].astype(str)

        return result

    # For merging both Oktopus device track.and mambu data
    def merge_df(self, mambu_data, Okt_devicetracking_data):

        Okt_devicetracking_data.rename({'mambu_client_id': 'BVN'}, axis=1, inplace=True)
        Okt_devicetracking_data['BVN'] = Okt_devicetracking_data['BVN'].astype(str)
        mambu_data['BVN'] = mambu_data['BVN'].astype(str)

        data = mambu_data.merge(Okt_devicetracking_data, on='BVN', how='left')

        return data

    # Get Fraud Dataframe
    def get_frauddf(self):
        return self.fraud_df

    # To get the disbursement data on a specific date
    def get_data_on_disb_date(self, data):
        ''' get the latest disbursement data on one day '''
        data['disbursement_date'] = pd.to_datetime(data['disbursement_date'])
        data['created_at'] = pd.to_datetime(data['created_at'])

        check_date_data = data[data['disbursement_date'] == self.disb_date]
        return check_date_data

    # Fraud case1: Same Name, Different BVN
    def SNDBVN(self, data):
        data['firstname'] = data['FIRSTNAME'].str.lower()
        data['lastname'] = data['LASTNAME'].str.lower()
        data['name'] = np.where(data['MIDDLENAME'].isna(), data['firstname'] + ' ' + data['lastname'],
                                data['firstname'] + ' ' + data['MIDDLENAME'] + ' ' + data['lastname'])
        data['name'] = data['name'].str.lower()
        data['count'] = data.groupby(['name']).cumcount().add(1)
        data_sorted = data.sort_values('name')

        # converts string to date
        data['disbursement_date'] = pd.to_datetime(data['disbursement_date'])

        # get the latest disbursement data on one day
        check_date_data = self.get_data_on_disb_date(data)

        # check if names disbursed on the specified `disb_date`` appears in previous disbursement with account state Active and Active-in-arrears
        namelist_SMDBVN = set(check_date_data['name']).intersection(data[
                                                                        (data['disbursement_date'] < self.disb_date) & (
                                                                            data['accountstate'].isin(
                                                                                ['ACTIVE', 'ACTIVE_IN_ARREARS']))][
                                                                        'name'])

        result_df = data[data['name'].isin(list(namelist_SMDBVN))]

        for i in result_df['id_x']:
            new_row = {'id': i, 'Fraud_Case': 'Same Name, Different BVN'}
            self.fraud_df = self.fraud_df.append(new_row, ignore_index=True)

        return data

    # Fraud case 2: Same BVN > 1 Loan
    def SBVNG1LOAN(self, data):
        check_date_data = self.get_data_on_disb_date(data)

        fraud_bvn_list = set(check_date_data['BVN']).intersection(data[(data['disbursement_date'] < self.disb_date) & (
            data['accountstate'].isin(['ACTIVE', 'ACTIVE_IN_ARREARS']))]['BVN'])
        result_df = data[data['BVN'].isin(list(fraud_bvn_list))]

        for i in result_df['id_x']:
            new_row = {'id': i, 'Fraud_Case': 'Same BVN, > 1 Loan'}
            self.fraud_df = self.fraud_df.append(new_row, ignore_index=True)

        return data

    # Fraud case 3: Same Phone Number, > 1 Loan
    def SPNG1LOAN(self, data):
        check_date_data = self.get_data_on_disb_date(data)

        # check if mobilenumber exists from oldest data to date
        fraud_mobileno_list = set(check_date_data['MOBILEPHONE1']).intersection(data[(data[
                                                                                          'disbursement_date'] < self.disb_date) & (
                                                                                         data['accountstate'].isin(
                                                                                             ['ACTIVE',
                                                                                              'ACTIVE_IN_ARREARS']))][
                                                                                    'MOBILEPHONE1'])
        result_df = data[data['MOBILEPHONE1'].isin(list(fraud_mobileno_list))]

        for i in result_df['id_x']:
            new_row = {'id': i, 'Fraud_Case': 'Same Phone Number, > 1 Loan'}
            self.fraud_df = self.fraud_df.append(new_row, ignore_index=True)

        return data

    # Fraud case 4: Same Device ID, > 1 Loan
    def SDIDG1LOAN(self, data):

        ## created_at was added to indicate when the bug on device ID was resolved
        check_date_data = data[(data['disbursement_date'] == self.disb_date) & (data['created_at'] > '2023-07-07')]
        fraud_device_ids = set(check_date_data['device_id']).intersection(data[(data[
                                                                                    'disbursement_date'] < self.disb_date) & (
                                                                                   data['accountstate'].isin(['ACTIVE',
                                                                                                              'ACTIVE_IN_ARREARS']))][
                                                                              'device_id'])
        result_df = data[data['device_id'].isin(list(fraud_device_ids))]

        for i in result_df['id_x']:
            new_row = {'id': i, 'Fraud_Case': 'Same Device ID, > 1 Loan'}
            self.fraud_df = self.fraud_df.append(new_row, ignore_index=True)

        return data

    # --------------------------------------------------------------------------------

    # Fraud case 5: Same Email Address, > 1 Loan
    def SEAG1LOAN(self, data):

        data['EMAILADDRESS'] = data['EMAILADDRESS'].apply(str.lower)
        check_date_data = self.get_data_on_disb_date(data)

        # check if email address exists from oldest data to date
        fraud_email_list = set(check_date_data['EMAILADDRESS']).intersection(data[(data[
                                                                                       'disbursement_date'] < self.disb_date) & (
                                                                                      data['accountstate'].isin(
                                                                                          ['ACTIVE',
                                                                                           'ACTIVE_IN_ARREARS']))][
                                                                                 'EMAILADDRESS'])
        result_df = data[data['EMAILADDRESS'].isin(list(fraud_email_list))]

        for i in result_df['id_x']:
            new_row = {'id': i, 'Fraud_Case': 'Same Email Address, > 1 Loan'}
            self.fraud_df = self.fraud_df.append(new_row, ignore_index=True)

        return data

    # Fraud case 6: Same DOB, 1 Loan
    def SDOBG1LOAN(self, data):

        check_date_data = self.get_data_on_disb_date(data)

        # check if dob exists from oldest data to date
        fraud_dob_list = set(check_date_data['BIRTHDATE']).intersection(data[(data[
                                                                                  'disbursement_date'] < self.disb_date) & (
                                                                                 data['accountstate'].isin(
                                                                                     ['ACTIVE', 'ACTIVE_IN_ARREARS']))][
                                                                            'BIRTHDATE'])
        result_df = data[data['BIRTHDATE'].isin(list(fraud_dob_list))]

        for i in result_df['id_x']:
            new_row = {'id': i, 'Fraud_Case': 'Same DOB, > 1 Loan'}
            self.fraud_df = self.fraud_df.append(new_row, ignore_index=True)

        return data

    # Fraud case 7: Same Account Number, > 1 Loan
    def SANG1LOAN(self, data):

        check_date_data = self.get_data_on_disb_date(data)

        # check if account no exists from oldest data to date
        fraud_acctno_list = set(check_date_data['bs_acct_no']).intersection(data[(data[
                                                                                      'disbursement_date'] < self.disb_date) & (
                                                                                     data['accountstate'].isin(
                                                                                         ['ACTIVE',
                                                                                          'ACTIVE_IN_ARREARS']))][
                                                                                'bs_acct_no'])
        result_df = data[data['bs_acct_no'].isin(list(fraud_acctno_list))]

        for i in result_df['id_x']:
            new_row = {'id': i, 'Fraud_Case': 'Same BS Account Number, > 1 Loan'}
            self.fraud_df = self.fraud_df.append(new_row, ignore_index=True)

        return data

    # Fraud case 8: Match Repayment Bank with Bank Statement
    def MRBWBS(self, data):

        check_date_data = self.get_data_on_disb_date(data)

        data['repayment_bank'] = data['repayment_bank'].str.strip()
        data['repayment_bank_refined'] = data['repayment_bank'].replace({'Access Bank': 'Access',
                                                                         'Access Bank Diamond': 'Access',
                                                                         'ACCESS BANK': 'Access',
                                                                         'ACCESS BANK PLC': 'Access',
                                                                         'Access Bank': 'Access',
                                                                         'Access Bank Diamond': 'Access',
                                                                         'Access bank': 'Access',
                                                                         'Eco Bank': 'Eco Bank',
                                                                         'Ecobank Nigeria': 'Eco Bank',
                                                                         'FCMB': 'FCMB',
                                                                         'Fcmb': 'FCMB',
                                                                         'First City Monument Bank': 'FCMB',
                                                                         'GTBank': 'Guaranty Trust Bank',
                                                                         'GT': 'Guaranty Trust Bank',
                                                                         'GTB': 'Guaranty Trust Bank',
                                                                         'GT Bank': 'Guaranty Trust Bank',
                                                                         'Globus Bank': 'Globus Bank',
                                                                         'Heritage Bank': 'Heritage Bank',
                                                                         'Jaiz Bank': 'Jaiz Bank',
                                                                         'Keystone Bank': 'Keystone Bank',
                                                                         'Polaris Bank': 'Polaris Bank',
                                                                         'Polaris Bank Limited': 'Polaris Bank',
                                                                         'Providus Bank': 'Providus',
                                                                         'Skye Bank': 'Polaris Bank',
                                                                         'Stanbic-IBTC': 'Stanbic IBTC',
                                                                         'Stanbic IBTC Bank': 'Stanbic IBTC',
                                                                         'Stanbic-IBTC Bank': 'Stanbic IBTC',
                                                                         'Standard Chartered': 'Standard Chartered',
                                                                         'Sterling Bank': 'Sterling Bank',
                                                                         'ALAT': 'Wema',
                                                                         'ALAT by WEMA': 'Wema',
                                                                         'Wema Bank': 'Wema',
                                                                         'KudaBank': 'Kuda',
                                                                         'Kuda Bank': 'Kuda',
                                                                         'UBA': 'UBA',
                                                                         'United Bank For Africa': 'UBA',
                                                                         'UBA PLC': 'UBA',
                                                                         'Union Bank': 'Union Bank',
                                                                         'Unity Bank': 'Unity Bank',
                                                                         'Union Bank of Nigeria': 'Union Bank',
                                                                         'First-City': 'First City',
                                                                         'Skye': 'Polaris',
                                                                         'ZENITH BANK': 'ZENITH BANK',
                                                                         'Zenith Bank': 'ZENITH BANK',
                                                                         'zenith bank': 'ZENITH BANK'})

        data['bs_bank'] = data['bs_bank'].str.strip()
        data['bs_bank_refined'] = data['bs_bank'].replace({'Access Bank': 'Access',
                                                           'Access Bank Diamond': 'Access',
                                                           'ACCESS BANK': 'Access',
                                                           'ACCESS BANK PLC': 'Access',
                                                           'Access Bank': 'Access',
                                                           'Access Bank Diamond': 'Access',
                                                           'Access bank': 'Access',
                                                           'Eco Bank': 'Eco Bank',
                                                           'Ecobank Nigeria': 'Eco Bank',
                                                           'FCMB': 'FCMB',
                                                           'Fcmb': 'FCMB',
                                                           'First City Monument Bank': 'FCMB',
                                                           'GTBank': 'Guaranty Trust Bank',
                                                           'GT': 'Guaranty Trust Bank',
                                                           'GTB': 'Guaranty Trust Bank',
                                                           'GT Bank': 'Guaranty Trust Bank',
                                                           'Globus Bank': 'Globus Bank',
                                                           'Heritage Bank': 'Heritage Bank',
                                                           'Jaiz Bank': 'Jaiz Bank',
                                                           'Keystone Bank': 'Keystone Bank',
                                                           'Polaris Bank': 'Polaris Bank',
                                                           'Polaris Bank Limited': 'Polaris Bank',
                                                           'Providus Bank': 'Providus',
                                                           'Skye Bank': 'Polaris Bank',
                                                           'Stanbic-IBTC': 'Stanbic IBTC',
                                                           'Stanbic IBTC Bank': 'Stanbic IBTC',
                                                           'Stanbic-IBTC Bank': 'Stanbic IBTC',
                                                           'Standard Chartered': 'Standard Chartered',
                                                           'Sterling Bank': 'Sterling Bank',
                                                           'ALAT': 'Wema',
                                                           'ALAT by WEMA': 'Wema',
                                                           'Wema Bank': 'Wema',
                                                           'KudaBank': 'Kuda',
                                                           'Kuda Bank': 'Kuda',
                                                           'UBA': 'UBA',
                                                           'United Bank For Africa': 'UBA',
                                                           'UBA PLC': 'UBA',
                                                           'Union Bank': 'Union Bank',
                                                           'Unity Bank': 'Unity Bank',
                                                           'Union Bank of Nigeria': 'Union Bank',
                                                           'First-City': 'First City',
                                                           'Skye': 'Polaris',
                                                           'ZENITH BANK': 'ZENITH BANK',
                                                           'Zenith Bank': 'ZENITH BANK',
                                                           'zenith bank': 'ZENITH BANK'})

        def check_bs_bank(row):
            bypass_pdt = ['Instant SEL Weekly', '2 Week Instant Loan']

            if row['LOANNAME'] in bypass_pdt and row['bs_bank_refined'] == None:
                return 'ByPass'

            elif row['repayment_bank_refined'] == row['bs_bank_refined']:
                return 'Match'

            else:
                return 'No Match'

        data['MRBWBS'] = data.apply(check_bs_bank, axis=1)

        result_df = data.loc[(data['MRBWBS'] == "No Match") & (data['disbursement_date'] == self.disb_date)]

        for i in result_df['id_x']:
            new_row = {'id': i, 'Fraud_Case': 'No Match in Repayment Bank & BS Bank'}
            self.fraud_df = self.fraud_df.append(new_row, ignore_index=True)

        return data

    def fraudDistribution(self, data):
        output = pd.crosstab(data['id'], data['Fraud_Case'])
        output['No_of_Fraudcases'] = output.sum(axis=1)
        return output

    def fraudCasesAggAmountCount(self, data):
        output = data.groupby('Fraud_Case').agg({'LOANAMOUNT': ['sum', 'count']})
        return output

    def fraudCasesDataMerge(self, output, data):
        return pd.merge(output, data, how='left', left_on='id', right_on='id_x')
