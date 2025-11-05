# mover.py

# Use to debug the pipeline. Mover monitors an
# input directory, and when it finds a file there
# it moves it to an output directory.
import logging
import os
from pathlib import Path
from pipeline.plumbing import Pipe, Filter

class Mover(Filter):
    def __init__(self, pipe: Pipe):
        super().__init__(pipe)

    def validate_token(self, token) -> bool:
        status: bool = True

        if Path(token.content["source_file"]).exists() is False:
            logging.error(f"source file does not exist: {token.content['source_file']}")
            self.log_to_token(
                token,
                "ERROR",
                f"source file does not exist: {token.content['source_file']}",
            )
            status = False

        if Path(token.content["destination_file"]).exists() is True:
            logging.error(f"destination file already exists: {token.content['destination_file']}")
            self.log_to_token(token, "ERROR", "destination file already exists")
            status = False

        return status

    def process_token(self, token) -> bool:
        completed: bool = False
        # move the file
        try:
            Path(token.content["source_file"]).rename(Path(token.content["destination_file"]))
            self.log_to_token(token, level="INFO", message="moved file")
            completed = True
        except (FileNotFoundError, PermissionError) as e:
            self.log_to_token(token, level="ERROR", message=f"could not move file: {e}")
            completed = False

        return completed


if __name__ == "__main__":
    from pipeline.config_loader import load_config
    from pipeline.logging_config import configure_logging
    import argparse


    if "PIPELINE_CONFIG" not in os.environ:
        print("Please set the PIPELINE_CONFIG environment variable.")
        sys.exit(1)

    if "DECRYPTION_PASSPHRASE" not in os.environ:
        print("Please set the DECRYPTION_PASSPHRASE environment variable.")
        sys.exit(1)


    config_path: str = os.environ.get("PIPELINE_CONFIG", "config.yml")
    config: dict = load_config(config_path)
        
    # Set up logging
    log_level = getattr(logging, config.get("global", {}).get("log_level", "INFO").upper())
    configure_logging(log_level)


    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    pipe: Pipe = Pipe(Path(args.input), Path(args.output))

    mover: Mover = Mover(pipe)
    logging.info("starting mover")
    mover.run_forever()
