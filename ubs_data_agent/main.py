from ubs_data_agent.config import get_config
from ubs_data_agent.mapping import source_to_target_mapping
from ubs_data_agent.logger import get_logger

def main():
    try:
        logger = get_logger(__name__)

        logger.info("Loading configuration")
        config = get_config()
        logger.info("Configuration loaded succesfully")

        mappings = config['mappings']
        for mapping in mappings:
            logger.info(f"Mapping found for {mappings[mapping]['source']} and {mappings[mapping]['target']}. Applying transformations")
            source_to_target_mapping(mappings[mapping], source=mappings[mapping]['source'], target=mappings[mapping]['target'])

    except Exception as e:
        print("Error:", e)
        e.with_traceback()

if __name__ == "__main__":
    main()