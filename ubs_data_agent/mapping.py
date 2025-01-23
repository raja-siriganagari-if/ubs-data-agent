import pandas as pd
import json
import glob
from ubs_data_agent.logger import get_logger

logger = get_logger(__name__)

def load_csv_to_sqlite(csv_path, table_name, conn):
    df = pd.read_csv(csv_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)

#region old sqlite code 

# def efx_to_ibdl_mapping(paths: dict):

#     # Create SQLite connection
#     conn = sqlite3.connect(':memory:')

#     # Step 1: Load CSVs into SQLite
#     load_csv_to_sqlite(paths['efx'], "efx", conn)
#     load_csv_to_sqlite(paths['ibdl'], "ibdl", conn)

#     # Step 2: Create a new column in ibdl concatenating the joining columns
#     identifying_columns = [
#         "combined_column",
#         "multiplied_column",
#         "uppercase_column",
#         "col5",
#         "timestamp"
#     ]

#     conn.execute(f"ALTER TABLE ibdl ADD COLUMN identifying_key TEXT;") 


#     concat_expr = " || '--' || ".join(identifying_columns)
#     update_query = f"""
#     UPDATE ibdl 
#     SET identifying_key = {concat_expr};
#     """
#     conn.execute(update_query)

#     result = pd.read_sql_query("SELECT * FROM ibdl", conn)

#     print(result)

#     # # Column mappings
#     # transformations = {
#     #     "combined_column": "efx.col1 || '-' || efx.col2",
#     #     "multiplied_column": "efx.col3 * 2",
#     #     "uppercase_column": "UPPER(efx.col4)"
#     # }
#     # id_columns = list(transformations.keys())

#     # # Step 2: Create a transformed view for the original table
#     # transformed_columns = ", ".join([f"{sql} AS {alias}" for alias, sql in transformations.items()])
#     # create_view_query = f"""
#     # CREATE VIEW efx_transformed AS
#     # SELECT efx.*, {transformed_columns}
#     # FROM efx
#     # """
#     # conn.execute(create_view_query)

#     # read_view_query = f"""
#     # SELECT *
#     # FROM efx_transformed
#     # """

#     # # Step 3: Join transformed original with the modified table
#     # join_condition = " AND ".join([f"efx_transformed.{col} = ibdl.{col}" for col in id_columns])
#     # query = f"""
#     # SELECT efx_transformed.*, ibdl.*, ibdl.timestamp AS modified_timestamp
#     # FROM efx_transformed
#     # INNER JOIN ibdl ON {join_condition}
#     # """
#     # result = pd.read_sql_query(query, conn)

#     # print("Columns in result: ", result.columns)

#     # # Step 4: Prepare the JSON structure
#     # final_output = []
#     # for _, row in result.iterrows():
#     #     # Extract timestamp, original, and modified data
#     #     entry = {
#     #         "timestamp": row["modified_timestamp"],
#     #         "efx": {col: row[col] for col in result.columns if col.startswith("efx.")},
#     #         "ibdl": {col: row[col] for col in result.columns if col.startswith("ibdl.") and col != "modified_timestamp"}
#     #     }
#     #     final_output.append(entry)


#     # # Step 5: Export result to JSON
#     # with open(paths['output'], 'w') as f:
#     #     json.dump(final_output, f, indent=4)

#endregion


def load_data_from_path(path: str) -> pd.DataFrame:
    filepaths = glob.glob(path)
    logger.info("CSV files loading: ", filepaths)
    dfs = []
    for file in filepaths:
        df = pd.read_csv(file)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def efx_to_ibdl_mapping(config: dict):

    # Step 1: Load both CSVs into dataframes
    efx_df = load_data_from_path(config['paths']['efx_data'])
    ibdl_df = load_data_from_path(config['paths']['ibdl_data'])
    
    id_columns = config['id_columns']
    ibdl_df['id_key'] = ibdl_df[id_columns].astype(str).agg('--'.join, axis=1)
    ibdl_df = ibdl_df.add_prefix("ibdl_")

    # Column mappings
    def orderIntIdRule(row):
        if row['instrumentType'] == 'SWAP':
            return str(row['orderId']) + '_' + str(int(row['legId'])+1)
        elif row['instrumentType'] == 'BLOCK':
            comp = ''
            if row['directionMatchesRequest'] == 'false':
                comp = 's' if row['blockLegSide'] == 'BUY' else 'b'
            else:
                comp = 'b' if row['blockLegSide'] == 'BUY' else 's'
            orderIntId = '_'.join([str(x) for x in [row['orderId'], comp, row['legId'], row['allocationId'], row['quoteId']]])
            return orderIntId
        else:
            return row['orderId']

    def buySellIndicatorRule(row):
        sym = str(row['sym'])
        fxpm = "PM" if str(row['sym'])[1:3] in ["AU", "AG", "PT", "PD"] else "FX"
        currOrder = ''
        if ord(sym[:1]) < ord(sym[3:4]):
            currOrder = 'left'
        elif ord(sym[:1]) > ord(sym[3:4]):
            currOrder = 'right'
        else:
            if ord(sym[1:2]) < ord(sym[4:5]):
                currOrder = 'left'
            elif ord(sym[1:2]) > ord(sym[4:5]):
                currOrder = 'right'
            else:
                if ord(sym[2:3]) < ord(sym[5:6]):
                    currOrder = 'left'
                elif ord(sym[2:3]) > ord(sym[5:6]):
                    currOrder = 'right'
                else:
                    currOrder = 'equal'
            
        flip_ini = -1
        if fxpm == "FX":
            if (currOrder == "left" and str(row['quantityUnit']) == "BASE") or (currOrder == "right" and str(row['quantityUnit']) == "COUNTER"):
                flip_ini =  1
            else:
                flip_ini = 0
        else:
            flip_ini = 1

        side_modified1 = ''

        side = str(row['side'])

        if side == "BUY":
            side_modified1 = "BUYI"
        elif side == "SELL":
            side_modified1 = "SELL"
        else:
            side_modified1 = ""
        
        flip = -1
        if str(row['instrumentType']) == 'SWAP':
            if int(row['legId']) % 2 == 0:
                flip = 1 - flip_ini
            else:
                flip = flip_ini
        else:
            flip = flip_ini

        side_modified = ''
        if flip == 1:
            if side_modified1 == "BUYI":
                side_modified = "SELL"
            elif side_modified1 == "SELL":
                side_modified == "BUYI"
            else:
                side_modified = ""
        else:
            side_modified = side_modified1

        return 1 if side_modified == "BUYI" else 0

    def eventTypeRule(row):
        eventType = str(row['eventType'])
        eventTypeTransformations = {
            "NEW": "NEWO",
            "CONFIRMED": "CONF",
            "CANCELLED": "CAMO",
            "REJECTED": "REMO",
            "TRADE": "FILL",
            "TRADE_ACK": "PARF"
        }
        return eventTypeTransformations[eventType] if eventType in eventTypeTransformations else ""

    def datetimeRule(row):
        return row['datetime']
    
    def instrumentIdRule(row):
        fxpm = "PM" if str(row['sym'])[1:3] in ["AU", "AG", "PT", "PD"] else "FX"
        symclean2 = 'X' + str(row['sym'])[1:] if fxpm == "PM" else str(row['sym'])

        third_comp = str(row['legSettlementDate']).replace('.', '') if str(row['spotSettlementDate']) == "0nd" else str(row['spotSettlementDate']).replace('.', '')
        return fxpm + 'SPOT' + symclean2 + third_comp

    transformations = {
        "orderIntId": orderIntIdRule,
        "buySellIndicator": buySellIndicatorRule,
        "eventType": eventTypeRule,
        "datetime": datetimeRule,
        "instrumentId": instrumentIdRule
    }

    common_columns = set(efx_df.columns) - set(ibdl_df.columns)

    for col_name, transform_func in transformations.items():
        efx_df[col_name] = efx_df.apply(transform_func, axis = 1)


    efx_df['id_key'] = efx_df[id_columns].astype(str).agg('--'.join, axis=1)
    columns_to_drop = set(id_columns) - common_columns
    print("CTD", columns_to_drop)
    efx_df = efx_df.drop(columns=columns_to_drop, axis = 1)
    efx_df = efx_df.add_prefix("efx_")

    # print(efx_df)

    combined_df = pd.merge(efx_df, ibdl_df, left_on='efx_id_key', right_on='ibdl_id_key')

    print(combined_df)

    # result = []

    # for _, row in combined_df.iterrows():

    #     record = {"datetime": row["efx_datetime"]}

    #     for col in row.index:
    #         if "id_key" in col:
    #             continue
    #         system_name, col_name = col.split("_", 1)
    #         if system_name not in record:
    #             record[system_name] = {}
    #         record[system_name][col_name] = row[col]


    #     result.append(record)

    
    # # Step 5: Export result to JSON
    # with open(config['paths']['output'], 'w') as f:
    #     json.dump(result, f)

