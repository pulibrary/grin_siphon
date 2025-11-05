import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from pipeline.plumbing import Filter, Pipe, Token

class Decryptor(Filter):
    """
    Pipeline filter that decrypts downloaded tarball files.

    The Decryptor filter uses GPG to decrypt encrypted tarball files downloaded
    from GRIN. It requires the DECRYPTION_PASSPHRASE environment variable to be set.

    Attributes:
        passphrase (str): GPG decryption passphrase from environment
    """

    def __init__(self, pipe: Pipe) -> None:
        passphrase = os.environ.get("DECRYPTION_PASSPHRASE")
        if not passphrase:
            raise RuntimeError("DECRYPTION_PASSPHRASE not set in environment")
        super().__init__(pipe)
        self.passphrase = passphrase

    def infile(self, token) -> Path:
        """Get the path to the encrypted input file for this token.

        Args:
            token (Token): Token containing barcode and processing bucket

        Returns:
            Path: Path to the .tar.gz.gpg file to decrypt
        """
        input_path = Path(token.content["processing_bucket"])
        input_filename: Path = Path(token.content["barcode"]).with_suffix(".tar.gz.gpg")
        return input_path / input_filename

    def outfile(self, token) -> Path:
        """Get the path for the decrypted output file for this token.

        Args:
            token (Token): Token containing barcode and processing bucket

        Returns:
            Path: Path where the decrypted .tgz file will be saved
        """
        output_path: Path = Path(token.content["processing_bucket"])
        output_filename: Path = Path(token.content["barcode"]).with_suffix(".tgz")
        return output_path / output_filename

    def validate_token(self, token) -> bool:
        """Validate that the encrypted source file exists for decryption.

        Args:
            token (Token): Token to validate

        Returns:
            bool: True if the encrypted source file exists, False otherwise
        """
        status: bool = True

        if self.infile(token).exists() is False:
            logging.error(f"source file does not exist: {self.infile(token)}")
            self.log_to_token(token, "ERROR", f"source file does not exist: {self.infile(token)}")
            status = False

        return status

    def process_token(self, token: Token) -> bool:
        """Decrypt the encrypted tarball file using GPG.

        Runs the gpg command to decrypt the .tar.gz.gpg file and save it as
        a .tgz file. Updates the token with decryption status.

        Args:
            token (Token): Token containing file paths and metadata

        Returns:
            bool: True if decryption succeeded, False if it failed
        """
        logging.info(f"processing token {token.content['barcode']}")
        successflg = False
        result = subprocess.run(
            [
                "gpg",
                "--batch",
                "--yes",
                "--passphrase",
                os.environ["DECRYPTION_PASSPHRASE"],
                "--decrypt",
                "--output",
                str(self.outfile(token)),
                str(self.infile(token)),
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            successflg = False
            token.content["decryption_status"] = "fail"
            self.log_to_token(token, "WARNING", "Decryption failed")
        else:
            successflg = True
            token.content["decryption_status"] = "success"
            self.infile(token).unlink()
            token.put_prop("when_decrypted", str(datetime.now(timezone.utc)))
            self.log_to_token(token, "INFO", "Decryption successful")

        return successflg


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
    logging.info("starting decryptor")
    decryptor = Decryptor(pipe)
    decryptor.run_forever()
