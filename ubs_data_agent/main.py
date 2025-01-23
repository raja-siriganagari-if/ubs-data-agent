from ubs_data_agent.config import get_config
from ubs_data_agent.mapping import efx_to_ibdl_mapping
from ubs_data_agent.logger import get_logger

def main():
    try:
        logger = get_logger(__name__)

        logger.info("Loading configuration")
        config = get_config()
        logger.info("Configuration loaded succesfully")

        if 'efx_to_ibdl' in config['mappings']:
            logger.info("EFX to IBDL mapping is present. Applying the mapping on given data") 
            efx_to_ibdl_mapping(config['mappings']['efx_to_ibdl'])

    except Exception as e:
        print("Error:", e)
        e.with_traceback()

if __name__ == "__main__":
    main()