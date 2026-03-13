import os
import subprocess
import yaml
import signal
import sys
import logging
from pipeline.config_loader import load_config
from pipeline.plumbing import Pipeline

config_path: str = os.environ.get("PIPELINE_CONFIG", "config.yml")
config: dict = load_config(config_path)

# Set up logging
log_level = getattr(logging, config.get("global", {}).get("log_level", "INFO").upper())

logging.basicConfig(level=log_level)


class Orchestrator:
    """
    Launches and manages filter processes for the pipeline.

    The Orchestrator is responsible for starting, stopping, and monitoring
    individual filter processes that make up the pipeline stages. Each filter
    runs as a separate subprocess, allowing for parallel processing and
    fault isolation.

    Attributes:
        processes (list): List of tuples containing (filter_name, subprocess.Popen)
        pipeline (Pipeline): Pipeline instance for bucket management
    """

    def __init__(self) -> None:
        self.processes = []
        self.pipeline = Pipeline(config)

    def start_filters(self):
        """Start all configured filter processes.

        Iterates through the filters defined in the configuration and starts
        each one as a separate subprocess.
        """
        for filt in config.get("filters", []):
            self.start_filter(filt)

    def start_filter_by_name(self, filter_name: str) -> bool:
        """Start a specific filter by name from configuration.

        Args:
            filter_name (str): Name of the filter to start

        Returns:
            bool: True if filter was found and started, False otherwise
        """
        # Check if filter is already running
        for name, proc in self.processes:
            if name == filter_name and proc.poll() is None:
                logging.warning("Filter '%s' is already running", filter_name)
                return False

        # Find filter in configuration
        for filt in config.get("filters", []):
            if filt["name"] == filter_name:
                self.start_filter(filt)
                return True

        logging.warning("Filter '%s' not found in configuration", filter_name)
        return False

    def start_filter(self, filt):
        """Start a single filter process.

        Args:
            filt (dict): Filter configuration containing script path, arguments,
                        and pipe configuration.
        """
        global_cfg = config.get("global", {})
        extra_env = {}

        # Inject global config values as environment variables for filter subprocesses
        if val := global_cfg.get("processing_bucket"):
            extra_env["DOWNLOAD_BUCKET"] = val
            extra_env["LOCAL_DIR"] = val
        if val := global_cfg.get("finished_bucket"):
            extra_env["FINISHED_BUCKET"] = val
        if val := global_cfg.get("object_store"):
            extra_env["OBJECT_STORE"] = val

        # Inject filter-specific config values
        if val := filt.get("decryption_passphrase"):
            extra_env["DECRYPTION_PASSPHRASE"] = val

        # poll_interval: per-filter override takes precedence over global
        poll_interval = filt.get("poll_interval", global_cfg.get("poll_interval", 60))
        extra_env["POLL_INTERVAL"] = str(poll_interval)

        # Resolve bucket names to actual directory paths
        in_bucket = str(self.pipeline.bucket(filt["pipe"]["in"]))
        out_bucket = str(self.pipeline.bucket(filt["pipe"]["out"]))

        # Build command line for filter subprocess
        cmd = [
            sys.executable,
            filt["script"],
            "--input",
            in_bucket,
            "--output",
            out_bucket,
        ]

        # Add any filter-specific environment variables
        if filt.get("args"):
            for k, v in filt.get("args").items():
                extra_env[k] = v

        logging.info("Starting filter: %s", " ".join(cmd))

        # Start the filter process with combined environment
        proc = subprocess.Popen(cmd, env={**os.environ, **extra_env})
        # Track the process for lifecycle management
        self.processes.append((filt["name"], proc))

    def stop_filter_by_name(self, filter_name: str, timeout: int = 60) -> bool:
        """Stop a specific filter process by name.

        Sends SIGTERM to allow graceful shutdown. If the process doesn't
        terminate within the timeout period, it will be forcefully killed
        with SIGKILL.

        Args:
            filter_name (str): Name of the filter to stop
            timeout (int): Seconds to wait for graceful shutdown before
                          force killing. Defaults to 60 seconds to allow
                          filters to complete their current token processing.

        Returns:
            bool: True if filter was found and stopped, False otherwise
        """
        for i, (name, proc) in enumerate(self.processes):
            if name == filter_name:
                logging.info("Stopping filter: %s", name)
                proc.terminate()
                try:
                    proc.wait(timeout=timeout)
                    logging.info("Filter '%s' exited gracefully", name)
                except subprocess.TimeoutExpired:
                    logging.warning("Filter '%s' did not exit within %d seconds, force killing", name, timeout)
                    proc.kill()
                del self.processes[i]
                return True
        logging.warning("Filter '%s' not found in running processes", filter_name)
        return False

    def stop_filters(self, timeout: int = 60):
        """Stop all running filter processes gracefully.

        Sends SIGTERM to each process to trigger graceful shutdown, allowing
        them to complete their current token processing. If a process doesn't
        terminate within the timeout, it will be forcefully killed with SIGKILL.

        Args:
            timeout (int): Seconds to wait for graceful shutdown before
                          force killing. Defaults to 60 seconds.
        """
        for name, proc in self.processes:
            logging.info("Stopping filter: %s", name)
            proc.terminate()
            try:
                proc.wait(timeout=timeout)
                logging.info("Filter '%s' exited gracefully", name)
            except subprocess.TimeoutExpired:
                logging.warning("Filter '%s' did not exit within %d seconds, force killing", name, timeout)
                proc.kill()
        self.processes.clear()

    def status(self):
        """Print the status of all filter processes.

        Shows whether each filter is running or has exited, along with
        the exit code for terminated processes.
        """
        for name, proc in self.processes:
            status = "running" if proc.poll() is None else f"exited ({proc.returncode})"
            print(f"Filter '{name}': {status}")

    def reload_config(self):
        logging.info("Reloading configuration...")
        load_config(config_path)

    def add_filter(self, filt):
        logging.info("Adding new filter: %s", filt["name"])
        config["filters"].append(filt)
        self.start_filter(filt)

    def list_filters(self):
        """List all available filters from configuration."""
        print("\nConfigured filters:")
        for filt in config.get("filters", []):
            status = "not running"
            for name, proc in self.processes:
                if name == filt["name"]:
                    status = "running" if proc.poll() is None else f"exited ({proc.returncode})"
                    break
            print(f"  - {filt['name']}: {status}")

    def repl(self):
        """Run the interactive command interface for orchestrator management.

        Provides commands for starting, stopping, restarting filters, checking
        status, and dynamically adding new filters.
        """
        print("\nOrchestrator REPL. Type 'help' for commands.")
        while True:
            try:
                cmd = input("orchestrator> ").strip()
                if cmd == "exit":
                    print("Exiting orchestrator.")
                    self.stop_filters()
                    break
                elif cmd == "status":
                    self.status()
                elif cmd == "list":
                    self.list_filters()
                elif cmd == "stop":
                    self.stop_filters()
                elif cmd.startswith("stop "):
                    filter_name = cmd[5:].strip()
                    if self.stop_filter_by_name(filter_name):
                        print(f"Stopped filter: {filter_name}")
                    else:
                        print(f"Failed to stop filter: {filter_name}")
                elif cmd == "start":
                    self.start_filters()
                elif cmd.startswith("start "):
                    filter_name = cmd[6:].strip()
                    if self.start_filter_by_name(filter_name):
                        print(f"Started filter: {filter_name}")
                    else:
                        print(f"Failed to start filter: {filter_name}")
                elif cmd == "restart":
                    self.stop_filters()
                    self.start_filters()
                elif cmd.startswith("restart "):
                    filter_name = cmd[8:].strip()
                    if self.stop_filter_by_name(filter_name):
                        if self.start_filter_by_name(filter_name):
                            print(f"Restarted filter: {filter_name}")
                        else:
                            print(f"Stopped {filter_name} but failed to restart")
                    else:
                        print(f"Failed to restart filter: {filter_name}")
                elif cmd == "reload":
                    self.stop_filters()
                    self.reload_config()
                    self.start_filters()
                elif cmd.startswith("add "):
                    try:
                        new_filter = yaml.safe_load(cmd[4:])
                        self.add_filter(new_filter)
                    except Exception as e:
                        logging.error("Failed to parse new filter: %s", e)
                elif cmd == "help":
                    print(
                        "Available commands:\n"
                        "  status              - Show status of running filters\n"
                        "  list                - List all configured filters\n"
                        "  start               - Start all filters\n"
                        "  start <name>        - Start a specific filter\n"
                        "  stop                - Stop all filters\n"
                        "  stop <name>         - Stop a specific filter\n"
                        "  restart             - Restart all filters\n"
                        "  restart <name>      - Restart a specific filter\n"
                        "  reload              - Reload config and restart all filters\n"
                        "  add <yaml>          - Add and start a new filter\n"
                        "  exit                - Exit orchestrator\n"
                        "  help                - Show this help message"
                    )
                else:
                    print(f"Unknown command: {cmd}")
            except (KeyboardInterrupt, EOFError):
                print("\nExiting orchestrator.")
                self.stop_filters()
                break

    def run(self, repl: bool = True):
        """Run the orchestrator with optional REPL interface.

        Args:
            repl (bool): Whether to start the interactive REPL interface.
                        Defaults to True.
        """

        def shutdown_handler(signum, frame):
            print("\nShutting down Orchestrator...")
            self.stop_filters()
            sys.exit(0)

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        self.start_filters()
        if repl is True:
            self.repl()


if __name__ == "__main__":
    # if 'GPG_PASSPHRASE' not in os.environ:
    #     print("Please set the GPG_PASSPHRASE environment variable.")
    #     sys.exit(1)
    if "PIPELINE_CONFIG" not in os.environ:
        print("Please set the PIPELINE_CONFIG environment variable.")
        sys.exit(1)

    # config_file = 'config.yml.gpg'
    # config_file = 'config.yml'

    # orchestrator = Orchestrator(config_file)
    orchestrator = Orchestrator()
    orchestrator.run(repl=True)
