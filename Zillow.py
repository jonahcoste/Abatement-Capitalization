# Set working directory
abspath = '/Volumes/ExternalHD2/PhilCap'

import pandas as pd
import numpy as np
import gc

#Get layout tables
layout_ZTrans = pd.read_excel('/Volumes/ExternalHD2/LayoutPy2.xlsx', sheet_name='ZTrans')
layout_ZAsmt = pd.read_excel('/Volumes/ExternalHD2/LayoutPy2.xlsx', sheet_name='ZAsmt')


#Columns to load
AsmtMainCols = [0,1,2,3,27,29,38,39,68,69,70, 71, 72, 73, 74]
AsmtBuildingCols = [0,1,2,3,4,5,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42]
AsmtBuildingAreasCols = [0,1,3,4]
TransPropertyInfoCols= [0,46,64]
TransMainCols = [0,4,6,16,17,18,22,24,25,30,31,32,33]
#AsmtHistCols = [1,38,39,75]

# Function for reading a ZAsmt table with ALL rows and column criterion
def read_ZAsmt_long(state_code, table_name, col_indices):
    path = "/Volumes/ExternalHD2/Current/{}/ZAsmt\{}.txt".format(state_code, table_name)
    #path = "/content/drive/My Drive/Colab Notebooks/Zillow/{}/ZAsmt\{}.txt".format(state_code, table_name)
    layout_temp = layout_ZAsmt.loc[layout_ZAsmt.TableName=='ut{}'.format(table_name), :].reset_index()
    names=layout_temp['FieldName'][col_indices]
    dtype=layout_temp['PandasDataType'][col_indices].to_dict()
    encoding='ISO-8859-1'
    sep = '|'
    header=None
    quoting=3

    
    return pd.read_csv(path, quoting=quoting, names=names, dtype=dtype, encoding=encoding, sep=sep, header=header, usecols=col_indices)

# Function for reading a ZAsmt table with row and column criteria
def read_ZAsmt(state_code, table_name, col_indices, row_crit_field, row_crit_content):
    path = "/Volumes/ExternalHD2/Current/{}/ZAsmt\{}.txt".format(state_code, table_name)
    layout_temp = layout_ZAsmt.loc[layout_ZAsmt.TableName=='ut{}'.format(table_name), :].reset_index()
    names=layout_temp['FieldName'][col_indices]
    dtype=layout_temp['PandasDataType'][col_indices].to_dict()
    encoding='ISO-8859-1'
    sep = '|'
    header=None
    quoting=3
    chunksize=500000

    iter = pd.read_csv(path, quoting=quoting, names=names, dtype=dtype, encoding=encoding, sep=sep, header=header, usecols=col_indices, iterator=True, chunksize=chunksize)
    return pd.concat([chunk[(chunk[row_crit_field].isin(row_crit_content))] for chunk in iter])

# Function for reading a Ztrans table with ALL rows and column criterion
def read_ZTrans_long(state_code, table_name, col_indices):
    path = "/Volumes/ExternalHD2/Current/{}/ZTrans\{}.txt".format(state_code, table_name)
    #path = "/content/drive/My Drive/Colab Notebooks/Zillow/{}/ZTrans\{}.txt".format(state_code, table_name)
    layout_temp = layout_ZTrans.loc[layout_ZTrans.TableName=='ut{}'.format(table_name), :].reset_index()
    names=layout_temp['FieldName'][col_indices]
    dtype=layout_temp['PandasDataType'][col_indices].to_dict()
    encoding='ISO-8859-1'
    sep = '|'
    header=None
    quoting=3

    return pd.read_csv(path, quoting=quoting, names=names, dtype=dtype, encoding=encoding, sep=sep, header=header, usecols=col_indices)


# Function for reading a Ztrans table with row and column criteria
def read_ZTrans(state_code, table_name, col_indices, row_crit_field, row_crit_content):
    path = "/Volumes/ExternalHD2/Current/{}/ZTrans\{}.txt".format(state_code, table_name)
    layout_temp = layout_ZTrans.loc[layout_ZTrans.TableName=='ut{}'.format(table_name), :].reset_index()
    names=layout_temp['FieldName'][col_indices]
    dtype=layout_temp['PandasDataType'][col_indices].to_dict()
    encoding='ISO-8859-1'
    sep = '|'
    header=None
    quoting=3
    chunksize=500000

    iter = pd.read_csv(path, quoting=quoting, names=names, dtype=dtype, encoding=encoding, sep=sep, header=header, usecols=col_indices, iterator=True, chunksize=chunksize)
    return pd.concat([chunk[(chunk[row_crit_field].isin(row_crit_content))] for chunk in iter])

#StateList = ['01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '44', '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56'] 

#StateList = ['01', '02', '04', '05', '06', '08'] 
#StateList = ['09', '10', '11', '12', '13', '15', '16', '17', '18', '19', '20', '21', '22', '23']
#StateList = ['24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41']
#StateList = ['42', '44', '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56']

#StateList = ['11', '01']

StateList = ['42']
CountyCodes= ['42017', '42029', '42045', '42091', '42101']

for i in StateList:
    State = i
    
    #Load main assesment data
    AsmtMain = read_ZAsmt(State,'Main', AsmtMainCols, 'FIPS', CountyCodes)

    # Keep only one record for each ImportPropertyID. 
    # ImportParcelID is the unique identifier of a parcel. Multiple entries for the same ImportParcelID are due to updated records.
    # The most recent record is identified by the greatest LoadID. 
    #   **** This step may not be necessary for the published dataset as we intend to only publish the updated records, but due dilligence demands we check. 

    #AsmtMain = AsmtMain.sort_values('LoadID',ascending=False).drop_duplicates(subset=['ImportParcelID'])
    #AsmtMain = AsmtMain.drop(['LoadID'], axis=1)
    #pd.DataFrame([[AsmtMain.shape[0], AsmtMain.shape[1]]], columns=['# rows', '# columns'])

    AsmtMain['LotSizeAcresTemp'] = AsmtMain['LotSizeAcres']*43560
    AsmtMain['LotSize'] = AsmtMain[['LotSizeAcresTemp', 'LotSizeSquareFeet']].max(axis=1)
    #AsmtMain= AsmtMain[~AsmtMain.LotSize.gt(87120) & ~AsmtMain.NoOfBuildings.gt(1)]
    AsmtMain= AsmtMain[~AsmtMain.NoOfBuildings.gt(1)]
    AsmtMain = AsmtMain.drop(['LotSizeAcresTemp', 'NoOfBuildings'], axis=1)
    AsmtRows = AsmtMain['RowID'].drop_duplicates().tolist()

    #### Load most property attributes
    AsmtBuilding = read_ZAsmt(State,'Building', AsmtBuildingCols, 'RowID', AsmtRows)
    #pd.DataFrame([[AsmtBuilding.shape[0], AsmtBuilding.shape[1]]], columns=['# rows', '# columns'])

    #Filter for building use, and build date
    UseCodes =                                  ['RR101',  # SFR
                                                'RR999',  # Inferred SFR
                                                'RR000',  # General Residential
                                              # 'RR102',  # Rural Residence   (includes farm/productive land?)
                                                'RR104',  # Townhouse
                                                'RR105',  # Cluster Home
                                                'RR106',  # Condominium
                                                'RR107',  # Cooperative
                                                'RR108',  # Row House
                                                'RR109',  # Planned Unit Development
                                                'RR113',  # Bungalow
                                                'RR116',  # Patio Home
                                                'RR119',  # Garden Home
                                                'RR120'
                                                ] # Landominium]

    AsmtBuilding = AsmtBuilding[AsmtBuilding.PropertyLandUseStndCode.isin(UseCodes) & (AsmtBuilding.YearBuilt.notnull() | AsmtBuilding.EffectiveYearBuilt.notnull())]
        #AsmtBuilding = AsmtBuilding[AsmtBuilding.PropertyLandUseStndCode.isin(UseCodes) & (AsmtBuilding.YearBuilt.ge(1964) | AsmtBuilding.EffectiveYearBuilt.ge(1964))]

    #Remove properties with multiple buildings
    AsmtBuilding = AsmtBuilding[~AsmtBuilding.RowID.isin(AsmtBuilding[AsmtBuilding.BuildingOrImprovementNumber.astype(int).gt(1)]['RowID'])] 
    #AsmtBuilding = AsmtBuilding.drop(['BuildingOrImprovementNumber'], axis=1)
    



    #Merge attributes with main table
    AsmtMain = AsmtMain.merge(AsmtBuilding, how = 'inner', on = 'RowID')
    del AsmtBuilding
    AsmtRows = AsmtMain['RowID'].drop_duplicates().tolist()
    
    
    #Load square footage data
    AsmtBuildingAreas = read_ZAsmt(State,'BuildingAreas', AsmtBuildingAreasCols, 'RowID', AsmtRows)
    #pd.DataFrame([[AsmtBuildingAreas.shape[0], AsmtBuildingAreas.shape[1]]], columns=['# rows', '# columns'])
    #Filter for building area types and limit to properties from previos filters
    BldgAreaCodes =                         ['BAL',  # Building Area Living
                                         'BAF',  # Building Area Finished
                                         'BAE',  # Effective Building Area
                                         'BAG',  # Gross Building Area
                                         'BAJ',  # Building Area Adjusted
                                         'BAT',  # Building Area Total
                                         'BLF'] # Building Area Finished Living
    AsmtBuildingAreas = AsmtBuildingAreas[AsmtBuildingAreas.BuildingAreaStndCode.isin(BldgAreaCodes)]   
    #pd.DataFrame([[AsmtBuildingAreas.shape[0], AsmtBuildingAreas.shape[1]]], columns=['# rows', '# columns'])
    # Counties report different breakdowns of building square footage and/or call similar concepts by different names.
    # The structure of this table is to keep all entries reported by the county as they are given. See 'Bldg Area' table in documentation.
    # The goal of this code is to determine the total square footage of each property. 
    # We assume a simple logic to apply across all counties here. Different logic may be as or more valid.
    # The logic which generates square footage reported on our sites is more complex, sometimes county specific, and often influenced by user interaction and update. 
    AsmtBuildingAreas = AsmtBuildingAreas.sort_values('BuildingAreaSqFt', ascending=False).drop_duplicates(subset=['RowID', 'BuildingOrImprovementNumber'])
    #pd.DataFrame([[AsmtBuildingAreas.shape[0], AsmtBuildingAreas.shape[1]]], columns=['# rows', '# columns'])
    #Merge square footage data with main table
    AsmtMain = AsmtMain.merge(AsmtBuildingAreas, how = 'left', on = ['RowID', 'BuildingOrImprovementNumber'])
    del AsmtBuildingAreas
    pd.DataFrame([[AsmtMain.shape[0], AsmtMain.shape[1]]], columns=['# rows', '# columns'])

    Parcels = AsmtMain['ImportParcelID'].drop_duplicates().tolist()

    #Load info for joining to assesment data
    TransPropertyInfo = read_ZTrans(State,'PropertyInfo', TransPropertyInfoCols, 'ImportParcelID', Parcels)

    # Keep only one record for each TransID and PropertySequenceNumber. 
    # TransID is the unique identifier of a transaction, which could have multiple properties sequenced by PropertySequenceNumber. 
    # Multiple entries for the same TransID and PropertySequenceNumber are due to updated records.
    # The most recent record is identified by the greatest LoadID. 
    #   **** This step may not be necessary for the published dataset as we intend to only publish most updated record. 
    #TransPropertyInfo = TransPropertyInfo.sort_values('LoadID',ascending=False).drop_duplicates(subset=['TransId', 'PropertySequenceNumber'])
    #TransPropertyInfo = TransPropertyInfo.drop(['LoadID'], axis=1)
    #pd.DataFrame([[TransPropertyInfo.shape[0], TransPropertyInfo.shape[1]]], columns=['# rows', '# columns'])

    # Drop transactions without info needed for join
    # Drop transactions of multiple parcels (transIDs associated with PropertySequenceNumber > 1)
    TransPropertyInfo = TransPropertyInfo[TransPropertyInfo.ImportParcelID.notnull() & ~TransPropertyInfo.TransId.isin(TransPropertyInfo[TransPropertyInfo.PropertySequenceNumber.astype(int).gt(1)]['TransId'])]  
    TransPropertyInfo = TransPropertyInfo.drop(['PropertySequenceNumber'], axis=1)
    Transactions = TransPropertyInfo['TransId'].drop_duplicates().tolist()

    #Load main transaction data
    TransMain = read_ZTrans(State,'Main', TransMainCols, 'TransId', Transactions)

    # Keep only one record for each TransID. 
    # TransID is the unique identifier of a transaction. 
    # Multiple entries for the same TransID are due to updated records.
    # The most recent record is identified by the greatest LoadID. 
    #   **** This step may not be necessary for the published dataset as we intend to only publish most updated record.
    #TransMain = TransMain.sort_values('LoadID',ascending=False).drop_duplicates(subset=['TransId'])
    #TransMain = TransMain.drop(['LoadID'], axis=1)
    #pd.DataFrame([[TransMain.shape[0], TransMain.shape[1]]], columns=['# rows', '# columns'])

    #Filter for document type, and identify arms length transactions
    DataCodes = ['D', 'H']
    PartialCodes = ['Y', 'M', 'P', 'F', 'U']
    SalesCodes = ['NA', 'CU', 'CM', 'CN', 'CP', 'DL', 'EP', 'ST']
    DocCodes = ['DELU', 'EXDE', 'FCDE', 'FDDE', 'QCDE', 'TXDE', 'TRFC', 'NTSL']
    PropUseCodes = ['AG', 'CI', 'CM', 'GV', 'IM', 'IN', 'MB', 'MH', 'RC', 'UL', 'VL']

    TransMain = TransMain[TransMain.DataClassStndCode.isin(DataCodes) & ~TransMain.IntraFamilyTransferFlag.isin(PartialCodes) & ~TransMain.PartialInterestTransferStndCode.eq('Y') & ~TransMain.TransferTaxExemptFlag.eq('Y') & TransMain.SalesPriceAmount.ge(10000) & TransMain.SalesPriceAmount.le(10000000) & ~TransMain.SalesPriceAmountStndCode.isin(SalesCodes) & ~TransMain.DocumentTypeStndCode.isin(DocCodes) & (TransMain.AssessmentLandUseStndCode.isin(UseCodes) | TransMain.AssessmentLandUseStndCode.isnull() | TransMain.AssessmentLandUseStndCode.eq(' ')) & ~TransMain.PropertyUseStndCode.isin(PropUseCodes) ]
    TransMain = TransMain.drop(['PartialInterestTransferStndCode', 'IntraFamilyTransferFlag', 'TransferTaxExemptFlag'], axis=1)

    #Create date field with Zillow suggested logic. Create prior and next year fields for joining tax data
    TransMain['Date'] = np.where(~TransMain.DocumentDate.isnull() & ~TransMain.DocumentDate.eq(' '), TransMain['DocumentDate'], np.where(~TransMain.SignatureDate.isnull() & TransMain.SignatureDate.eq(' '), TransMain['SignatureDate'], np.where(~TransMain.RecordingDate.isnull() & ~TransMain.RecordingDate.eq(' '), TransMain['RecordingDate'], '0')))
    #TransMain = TransMain.drop(['DocumentDate', 'SignatureDate', 'RecordingDate'], axis=1)
    TransMain['Year'] = TransMain.Date.str[:4]
    TransMain = TransMain.astype({'Year': 'int32'})
    TransMain = TransMain[TransMain.Year.ge(1994) & TransMain.Year.le(2021)]

    #Merge IportParcelID into main transaction table
    TransMain = TransMain.merge(TransPropertyInfo, how = 'inner', on = 'TransId')
    del TransPropertyInfo

    #Merge with assesment data
    AsmtMain =  AsmtMain[AsmtMain.ImportParcelID.isin(TransMain['ImportParcelID'])]
    AsmtMain = AsmtMain.merge(TransMain, how='inner', on='ImportParcelID')
    del TransMain

    #Create Age variable and filter for only properties 0-30 years old sold
    AsmtMain['YearB'] = AsmtMain[['YearBuilt', 'EffectiveYearBuilt']].max(axis=1)
    AsmtMain['Age'] = AsmtMain['Year'] - AsmtMain['YearB']
    AsmtMain =  AsmtMain[~(AsmtMain.YearRemodeled.ge(AsmtMain.Year-10)&AsmtMain.YearRemodeled.le(AsmtMain.Year))]
    AsmtMain =  AsmtMain[AsmtMain.Age.ge(0) & AsmtMain.YearB.ge(1901)]
    #AsmtMain =  AsmtMain[AsmtMain.Age.ge(0)]
    
    #Remove multiple sales in same year
    #AsmtMain = AsmtMain.drop_duplicates(subset=['RowID', 'Year'], keep= False)
    #Remove properties with only one sale
    #AsmtMain = AsmtMain[AsmtMain.duplicated(subset= ['RowID'], keep=False)]
    
    #Keep only cities with at least 25000 observations
    AsmtMain['PropertyCity'] = AsmtMain['PropertyCity'].str.upper()
    #n_by_city = AsmtMain.groupby('PropertyCity')['RowID'].count()
    #n_by_city = n_by_city[n_by_city.ge(25000)]
    #AsmtMain = AsmtMain[AsmtMain.PropertyCity.isin(n_by_city.index.values)]

    #Write file to csv
    newfile = abspath + "/v6/" + State + ".csv"
    AsmtMain.to_csv(newfile, index=False)
    del AsmtMain

    gc.collect()


