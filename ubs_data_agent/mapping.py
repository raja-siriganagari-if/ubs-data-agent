import pandas as pd
import json
import glob
from ubs_data_agent.logger import get_logger
import itertools

logger = get_logger(__name__)

def load_data_from_path(path: str) -> pd.DataFrame:
    filepaths = glob.glob(path)
    logger.info(f"CSV files loading for paths: {', '.join(filepaths)}")
    dfs = []
    for file in filepaths:
        df = pd.read_csv(file)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def efx_to_ibdl_mapping(config: dict):

    # Step 1: Load both CSVs into dataframes
    efx_df = load_data_from_path(config['paths']['efx_data'])
    ibdl_df = load_data_from_path(config['paths']['ibdl_data'])

    logger.info("Loaded CSV files into dataframes")
    
    id_columns = list(config['id_columns'])
    ibdl_df['id_key'] = ibdl_df[id_columns].astype(str).agg('--'.join, axis=1)

    block_id_key_generated = False
    try:
        id_columns_block = id_columns.copy()
        id_columns_block[id_columns.index('orderIntId')] = 'quoteId'
        ibdl_df['id_key_block'] = ibdl_df[id_columns_block].astype(str).agg('--'.join, axis=1)
        block_id_key_generated = True
    except:
        logger.error(f"Exception while creating id_key_block for block trades")
    

    ibdl_df = ibdl_df.add_prefix("ibdl_")

    logger.info(f"Created ID keys for IBDL using cols: {', '.join(id_columns)}")
    logger.debug(f"IBDL head: \n{ibdl_df.head()}")

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

        return side_modified

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

    for col_name, transform_func in transformations.items():
        efx_df[col_name+"_efx_temp"] = efx_df.apply(transform_func, axis = 1)

    id_columns_transformed = [col + "_efx_temp" for col in id_columns]
    efx_df['id_key'] = efx_df[id_columns_transformed].astype(str).agg('--'.join, axis=1)
    logger.info(f"Deleting these newly generated columns from source DF: {id_columns_transformed}")
    efx_df = efx_df.drop(columns=id_columns_transformed, axis = 1)
    efx_df = efx_df.add_prefix("efx_")

    logger.debug(f"EFX head: \n{efx_df.head()}")


    combined_df = pd.merge(efx_df, ibdl_df, left_on='efx_id_key', right_on='ibdl_id_key')

    if block_id_key_generated:
        combined_df_block = pd.merge(efx_df, ibdl_df, left_on='efx_id_key', right_on='ibdl_id_key_block')

    result = []

    combined_rows = None
    if block_id_key_generated:
        combined_rows = itertools.chain(combined_df.iterrows(), combined_df_block.iterrows())
    else:
        combined_rows = combined_df.iterrows()


    for _, row in combined_rows:

        record = {"timestamp": row["efx_datetime"]}

        for col in row.index:
            if "id_key" in col:
                continue
            system_env, col_name = col.split("_", 1)
            system_mapping = "source" if system_env == "efx" else "target"
            if system_mapping not in record:
                record[system_mapping] = {}
            record[system_mapping][col_name] = row[col]
            record[system_mapping]['env'] = system_env


        result.append(record)

    logger.info(f"Number of matches between EFX and IBDL: {len(result)}")
    
    with open(config['paths']['output'], 'w') as f:
        logger.info(f"Writing records to file: {config['paths']['output']}")
        f.write("[\n")
        cookie = None
        for record in result:
            f.write(json.dumps(record))
            cookie = f.tell()
            f.write(",\n")
        
        if cookie:
            f.seek(cookie)
        f.write("\n]\n")

    logger.info("Finished writing records")

