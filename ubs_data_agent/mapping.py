import json
import glob
from ubs_data_agent.logger import get_logger
import itertools
import polars as pl

logger = get_logger(__name__)

def load_data_from_path(path: str) -> pl.DataFrame:
    filepaths = glob.glob(path)
    logger.info(f"CSV files loading for paths: {', '.join(filepaths)}")
    dfs = []
    for file in filepaths:
        df = pl.read_csv(file)
        dfs.append(df)

    return pl.concat(dfs)


def source_to_target_mapping(config: dict, source: str, target: str):

    logger.debug(f"Config here is {config}")

    # Step 1: Load both CSVs into dataframes
    source_df = load_data_from_path(config['paths']['source_data'])
    target_df = load_data_from_path(config['paths']['target_data'])

    logger.info("Loaded CSV files into dataframes")
    
    id_columns = list(config['id_columns'])
    target_df = target_df.with_columns(
        pl.concat_str(id_columns, separator='--').alias('id_key')
    )

    block_id_key_generated = 'quoteId' in target_df.columns


    if block_id_key_generated:
        id_columns_block = id_columns.copy()
        id_columns_block[id_columns.index('orderIntId')] = 'quoteId'
        target_df = target_df.with_columns(
            pl.concat_str(id_columns_block, separator='--').alias('id_key_block')
        )

    target_df = target_df.rename({col: f"target_{col}" for col in target_df.columns})

    logger.debug(f"Target dtypes: \n{target_df.dtypes}")

    logger.info(f"Created ID keys for Target using cols: {', '.join(id_columns)}")
    logger.debug(f"Target head: \n{target_df}")
    
    for col in id_columns:
        try:
            logger.info(f"Creating rule from config for column {col}")
            exec(config['rules'][col])
        except Exception as e:
            logger.error(e)

    transformations = { col: locals()[col+'Rule'] for col in id_columns}

    for col_name, transform_func in transformations.items():
        source_df = source_df.with_columns(
            pl.struct(source_df.columns).map_elements(transform_func).alias(col_name+"_source_temp")
        )

    id_columns_transformed = [col + "_source_temp" for col in id_columns]
    source_df = source_df.with_columns(
            pl.concat_str(id_columns_transformed, separator='--').alias('id_key')
    )
    logger.info(f"Deleting these newly generated columns from source DF: {id_columns_transformed}")
    source_df = source_df.drop(id_columns_transformed)
    source_df = source_df.rename({col: f"source_{col}" for col in source_df.columns})
    logger.debug(f"Source head: \n{source_df}")


    combined_df = source_df.join(target_df, left_on='source_id_key', right_on='target_id_key')

    if block_id_key_generated:
        combined_df_block = source_df.join(target_df, left_on='source_id_key', right_on='target_id_key_block')

    logger.debug(f"{combined_df}")


    result = []

    combined_rows = None
    if block_id_key_generated:
        combined_rows = itertools.chain(combined_df.iter_rows(named=True), combined_df_block.iter_rows(named=True))
    else:
        combined_rows = combined_df.iter_rows(named=True)


    for row in combined_rows:

        record = {"timestamp": row['source_'+config['sourceTimeStampCol']]}

        for col in row.keys():
            if "id_key" in col:
                continue
            system_env, col_name = col.split("_", 1)
            system_mapping = source.lower() if system_env == "source" else target.lower()
            if system_env not in record:
                record[system_env] = {}
            record[system_env][col_name] = row[col]
            record[system_env]['env'] = system_mapping


        result.append(record)

    logger.info(f"Number of matches between Source and Target: {len(result)}")

    logger.debug(f'{json.dumps(result)}')
    
    with open(config['paths']['output'], 'w') as f:
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

