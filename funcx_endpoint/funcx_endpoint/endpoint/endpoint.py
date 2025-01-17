from __future__ import annotations

import json
import logging
import os
import pathlib
import re
import shutil
import signal
import sys
import typing
import uuid
from string import Template

import daemon
import daemon.pidfile
import psutil
import texttable
from funcx_common.response_errors.error_base import FuncxResponseError
from funcx_common.response_errors.error_classes import (
    EndpointInUseError,
    EndpointLockedError,
)
from globus_sdk import GlobusAPIError, NetworkError

from funcx.sdk.client import FuncXClient
from funcx_endpoint.endpoint import default_config as endpoint_default_config
from funcx_endpoint.endpoint.interchange import EndpointInterchange
from funcx_endpoint.endpoint.register_endpoint import register_endpoint
from funcx_endpoint.endpoint.result_store import ResultStore
from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.logging_config import setup_logging

log = logging.getLogger(__name__)


_DEFAULT_FUNCX_DIR = str(pathlib.Path.home() / ".funcx")


class Endpoint:
    """
    Endpoint is primarily responsible for configuring, launching and stopping
    the Endpoint.
    """

    def __init__(self, funcx_dir=None, debug=False):
        """Initialize the Endpoint

        Parameters
        ----------

        funcx_dir: str
            Directory path to the root of the funcx dirs. Usually ~/.funcx.

        debug: Bool
            Enable debug logging. Default: False
        """
        self.debug = debug
        self.funcx_dir = funcx_dir if funcx_dir is not None else _DEFAULT_FUNCX_DIR
        self.name = "default"

        self.fx_client = None

    @property
    def _endpoint_dir(self):
        return os.path.join(self.funcx_dir, self.name)

    @staticmethod
    def _config_file_path(endpoint_dir: pathlib.Path) -> pathlib.Path:
        return endpoint_dir / "config.py"

    @staticmethod
    def update_config_file(
        ep_name: str,
        original_path: pathlib.Path,
        target_path: pathlib.Path,
        multi_tenant: bool,
    ):
        endpoint_config = Template(original_path.read_text()).substitute(name=ep_name)

        if endpoint_config.find("multi_tenant=") != -1:
            # If the option is already in the config, merely update the value
            endpoint_config = re.sub(
                "multi_tenant=(True|False)",
                f"multi_tenant={multi_tenant is True}",
                endpoint_config,
            )
        elif multi_tenant:
            # If the option isn't pre-existing, add only if set to True
            endpoint_config = endpoint_config.replace(
                "\nconfig = Config(\n", "\nconfig = Config(\n    multi_tenant=True,\n"
            )

        target_path.write_text(endpoint_config)

    @staticmethod
    def init_endpoint_dir(
        endpoint_dir: pathlib.Path,
        endpoint_config: pathlib.Path | None = None,
        multi_tenant=False,
    ):
        """Initialize a clean endpoint dir

        :param endpoint_dir pathlib.Path Path to the endpoint configuration dir
        :param endpoint_config str Path to a config file to be used instead
         of the funcX default config file
        :param multi_tenant bool Whether the endpoint is a multi-user endpoint
        """
        log.debug(f"Creating endpoint dir {endpoint_dir}")
        endpoint_dir.mkdir(parents=True, exist_ok=True)

        config_target_path = Endpoint._config_file_path(endpoint_dir)

        if endpoint_config is None:
            endpoint_config = pathlib.Path(endpoint_default_config.__file__)

        Endpoint.update_config_file(
            endpoint_dir.name,
            endpoint_config,
            config_target_path,
            multi_tenant,
        )

    @staticmethod
    def configure_endpoint(
        conf_dir: pathlib.Path, endpoint_config: str | None, multi_tenant: bool = False
    ):
        ep_name = conf_dir.name
        if conf_dir.exists():
            print(f"config dir <{ep_name}> already exists")
            raise Exception("ConfigExists")

        templ_conf_path = pathlib.Path(endpoint_config) if endpoint_config else None
        Endpoint.init_endpoint_dir(conf_dir, templ_conf_path, multi_tenant)
        config_path = Endpoint._config_file_path(conf_dir)
        if multi_tenant:
            print(f"Created multi-tenant profile for <{ep_name}>")
        else:
            print(f"Created profile for <{ep_name}>")
        print(
            f"\n    Configuration file: {config_path}\n"
            "\nUse the `start` subcommand to run it:\n"
            f"\n    $ funcx-endpoint start {ep_name}"
        )

    @staticmethod
    def check_endpoint_json(endpoint_json, endpoint_uuid):
        if os.path.exists(endpoint_json):
            with open(endpoint_json) as fp:
                log.debug("Connection info loaded from prior registration record")
                reg_info = json.load(fp)
                endpoint_uuid = reg_info["endpoint_id"]
        elif not endpoint_uuid:
            endpoint_uuid = str(uuid.uuid4())
        return endpoint_uuid

    @staticmethod
    def get_funcx_client(config: Config | None) -> FuncXClient:
        if config:
            funcx_client_options = {
                "funcx_service_address": config.funcx_service_address,
                "environment": config.environment,
            }

            return FuncXClient(**funcx_client_options)
        else:
            return FuncXClient()

    def start_endpoint(
        self,
        name,
        endpoint_uuid,
        endpoint_config: Config,
        log_to_console: bool,
        no_color: bool,
    ):
        self.name = name

        endpoint_dir = self._endpoint_dir
        endpoint_json = os.path.join(endpoint_dir, "endpoint.json")

        # This is to ensure that at least 1 executor is defined
        if not endpoint_config.executors:
            raise Exception(
                f"Endpoint config file at {endpoint_dir} is missing "
                "executor definitions"
            )

        fx_client = Endpoint.get_funcx_client(endpoint_config)

        endpoint_uuid = Endpoint.check_endpoint_json(endpoint_json, endpoint_uuid)

        log.info(f"Starting endpoint with uuid: {endpoint_uuid}")

        pid_file = os.path.join(endpoint_dir, "daemon.pid")
        pid_check = Endpoint.check_pidfile(pid_file)
        # if the pidfile exists, we should return early because we don't
        # want to attempt to create a new daemon when one is already
        # potentially running with the existing pidfile
        if pid_check["exists"]:
            if pid_check["active"]:
                log.info("Endpoint is already active")
                sys.exit(-1)
            else:
                log.info(
                    "A prior Endpoint instance appears to have been terminated without "
                    "proper cleanup. Cleaning up now."
                )
                Endpoint.pidfile_cleanup(pid_file)

        result_store = ResultStore(endpoint_dir=endpoint_dir)

        # Create a daemon context
        # If we are running a full detached daemon then we will send the output to
        # log files, otherwise we can piggy back on our stdout
        if endpoint_config.detach_endpoint:
            stdout: typing.TextIO = open(
                os.path.join(endpoint_dir, endpoint_config.stdout), "a+"
            )
            stderr: typing.TextIO = open(
                os.path.join(endpoint_dir, endpoint_config.stderr), "a+"
            )
        else:
            stdout = sys.stdout
            stderr = sys.stderr

        try:
            context = daemon.DaemonContext(
                working_directory=endpoint_dir,
                umask=0o002,
                pidfile=daemon.pidfile.PIDLockFile(pid_file),
                stdout=stdout,
                stderr=stderr,
                detach_process=endpoint_config.detach_endpoint,
            )

        except Exception:
            log.exception(
                "Caught exception while trying to setup endpoint context dirs"
            )
            exit(-1)

        # place registration after everything else so that the endpoint will
        # only be registered if everything else has been set up successfully
        reg_info = None
        try:
            reg_info = register_endpoint(
                fx_client,
                endpoint_uuid,
                endpoint_dir,
                self.name,
                endpoint_config.multi_tenant,
            )
        # if the service sends back an error response, it will be a FuncxResponseError
        except FuncxResponseError as e:
            # an example of an error that could conceivably occur here would be
            # if the service could not register this endpoint with the forwarder
            # because the forwarder was unreachable
            if e.http_status_code < 500:
                raise

            log.exception("Caught exception while attempting endpoint registration")
            log.critical(
                "Endpoint registration will be retried in the new endpoint daemon "
                "process. The endpoint will not work until it is successfully "
                "registered."
            )

        # if the service has an unexpected internal error and is unable to send
        # back a FuncxResponseError
        except GlobusAPIError as e:
            if e.http_status < 500:
                if (
                    e.http_status == EndpointLockedError.http_status_code
                    or e.http_status == EndpointInUseError.http_status_code
                ):
                    log.warning(f"Endpoint registration blocked.  [{e.message}]")
                    exit(-1)
                raise

            log.exception("Caught exception while attempting endpoint registration")
            log.critical(
                "Endpoint registration will be retried in the new endpoint daemon "
                "process. The endpoint will not work until it is successfully "
                "registered."
            )
        # if the service is unreachable due to a timeout or connection error
        except NetworkError as e:
            # the output of a NetworkError exception is huge and unhelpful, so
            # it seems better to just stringify it here and get a concise error
            log.exception(
                f"Caught exception while attempting endpoint registration: {e}"
            )
            log.critical(
                "funcx-endpoint is unable to reach the funcX service due to a "
                "NetworkError \n"
                "Please make sure that the funcX service address you provided is "
                "reachable \n"
                "and then attempt restarting the endpoint"
            )
            exit(-1)

        if reg_info:
            log.info("Launching endpoint daemon process")
        else:
            log.critical("Launching endpoint daemon process with errors noted above")

        # NOTE
        # It's important that this log is emitted before we enter the daemon context
        # because daemonization closes down everything, a log message inside the
        # context won't write the currently configured loggers
        logfile = os.path.join(endpoint_dir, "endpoint.log")
        log.info(
            "Logging will be reconfigured for the daemon. logfile=%s , debug=%s",
            logfile,
            self.debug,
        )

        with context:
            setup_logging(
                logfile=logfile,
                debug=self.debug,
                console_enabled=log_to_console,
                no_color=no_color,
            )

            Endpoint.daemon_launch(
                self.name,
                endpoint_uuid,
                endpoint_dir,
                endpoint_config,
                reg_info,
                fx_client,
                result_store,
            )

    @staticmethod
    def daemon_launch(
        ep_name,
        endpoint_uuid,
        endpoint_dir,
        endpoint_config: Config,
        reg_info,
        funcx_client: FuncXClient,
        result_store: ResultStore,
    ):
        interchange = EndpointInterchange(
            config=endpoint_config,
            reg_info=reg_info,
            endpoint_id=endpoint_uuid,
            endpoint_dir=endpoint_dir,
            endpoint_name=ep_name,
            funcx_client=funcx_client,
            result_store=result_store,
            logdir=endpoint_dir,
        )

        interchange.start()

        log.critical("Interchange terminated.")

    @staticmethod
    def get_endpoint_id(endpoint_dir: pathlib.Path) -> str | None:
        info_file_path = endpoint_dir / "endpoint.json"
        if info_file_path.is_file():
            return json.loads(info_file_path.read_bytes())["endpoint_id"]
        return None

    @staticmethod
    def stop_endpoint(
        endpoint_dir: pathlib.Path,
        endpoint_config: Config | None,
        remote: bool = False,
    ):
        pid_path = endpoint_dir / "daemon.pid"
        ep_name = endpoint_dir.name

        if remote is True:
            endpoint_id = Endpoint.get_endpoint_id(endpoint_dir)
            if not endpoint_id:
                raise ValueError(f"Endpoint <{ep_name}> could not be located")

            fx_client = Endpoint.get_funcx_client(endpoint_config)
            fx_client.stop_endpoint(endpoint_id)

        ep_status = Endpoint.check_pidfile(pid_path)
        if ep_status["exists"] and not ep_status["active"]:
            Endpoint.pidfile_cleanup(pid_path)
            return
        elif not ep_status["exists"]:
            log.info(f"Endpoint <{ep_name}> is not active.")
            return

        log.debug(f"{ep_name} has a daemon.pid file")
        pid = int(pid_path.read_text().strip())
        try:
            log.debug(f"Signaling process: {pid}")
            # For all the processes, including the deamon and its descendants,
            # send SIGTERM, wait for 10s, and then SIGKILL any still alive.
            grace_period_s = 10
            parent = psutil.Process(pid)
            processes = parent.children(recursive=True)
            processes.append(parent)
            for p in processes:
                p.send_signal(signal.SIGTERM)
            terminated, alive = psutil.wait_procs(processes, timeout=grace_period_s)
            for p in alive:
                try:
                    p.send_signal(signal.SIGKILL)
                except psutil.NoSuchProcess:
                    pass

            if pid_path.exists():
                log.warning(f"Endpoint <{ep_name}> did not gracefully shutdown")
                # Do cleanup anyway, TODO figure out where it should have done that
                pid_path.unlink(missing_ok=True)
            else:
                log.info(f"Endpoint <{ep_name}> is now stopped")
        except OSError:
            log.warning(f"Endpoint <{ep_name}> could not be terminated")
            log.warning(f"Attempting Endpoint <{ep_name}> cleanup")
            pid_path.unlink(missing_ok=True)
            exit(-1)

    @staticmethod
    def delete_endpoint(endpoint_dir: pathlib.Path):
        ep_name = endpoint_dir.name

        if not endpoint_dir.exists():
            log.warning(f"Endpoint <{ep_name}> does not exist")
            exit(-1)

        # stopping the endpoint should handle all of the process cleanup before
        # deletion of the directory
        Endpoint.stop_endpoint(endpoint_dir, None)

        shutil.rmtree(endpoint_dir)
        log.info(f"Endpoint <{ep_name}> has been deleted.")

    @staticmethod
    def check_pidfile(pid_path: pathlib.Path | str):
        """Helper function to identify possible dead endpoints

        Returns a record with 'exists' and 'active' fields indicating
        whether the pidfile exists, and whether the process is active if it does exist
        (The endpoint can only start correctly if there is no pidfile)

        Parameters
        ----------
        pid_path : str
            Path to the pid file
        """
        status = {"exists": False, "active": False}
        pid_path = pathlib.Path(pid_path)
        if not pid_path.exists():
            return status

        status["exists"] = True

        try:
            pid = int(pid_path.read_text().strip())
            psutil.Process(pid)
            status["active"] = True
        except ValueError:
            # Invalid literal for int() parsing
            pass
        except psutil.NoSuchProcess:
            pass

        return status

    @staticmethod
    def pidfile_cleanup(pid_path: pathlib.Path | str):
        pid_path = pathlib.Path(pid_path)
        pid_path.unlink(missing_ok=True)
        ep_name = pid_path.parent.name
        log.info(f"Endpoint <{ep_name}> has been cleaned up.")

    @staticmethod
    def get_endpoints(funcx_conf_dir: pathlib.Path | str) -> dict[str, dict]:
        """
        Gets a dictionary that contains information about all locally
        known endpoints.

        "status" can be one of:
            ["Initialized", "Running", "Disconnected", "Stopped"]

        Example output:
        {
            "default": {
                 "status": "Running",
                 "id": "123abcde-a393-4456-8de5-123456789abc"
            },
            "my_test_ep": {
                 "status": "Disconnected",
                 "id": "xxxxxxxx-xxxx-1234-abcd-xxxxxxxxxxxx"
            }
        }
        """
        ep_statuses = {}

        funcx_conf_dir = pathlib.Path(funcx_conf_dir)
        ep_dir_paths = (p.parent for p in funcx_conf_dir.glob("*/config.py"))
        for ep_path in ep_dir_paths:
            ep_status = {
                "status": "Initialized",
                "id": Endpoint.get_endpoint_id(ep_path),
            }
            ep_statuses[ep_path.name] = ep_status
            if not ep_status["id"]:
                continue

            pid_check = Endpoint.check_pidfile(ep_path / "daemon.pid")
            if pid_check["active"]:
                ep_status["status"] = "Running"
            elif pid_check["exists"]:
                ep_status["status"] = "Disconnected"
            else:
                ep_status["status"] = "Stopped"

        return ep_statuses

    @staticmethod
    def get_running_endpoints(conf_dir: pathlib.Path):
        return {
            ep_name: ep_status
            for ep_name, ep_status in Endpoint.get_endpoints(conf_dir).items()
            if ep_status["status"].lower() == "running"
        }

    @staticmethod
    def print_endpoint_table(conf_dir: pathlib.Path, ofile=None):
        """
        Converts locally configured endpoint list to a text based table
        and prints the output.
          For example format, see the texttable module
        """
        if not ofile:
            ofile = sys.stdout

        endpoints = Endpoint.get_endpoints(conf_dir)
        if not endpoints:
            print(
                "No endpoints configured!\n\n  (Hint: funcx-endpoint configure)",
                file=ofile,
            )
            return

        table = texttable.Texttable()
        headings = ["Endpoint ID", "Status", "Endpoint Name"]
        table.header(headings)

        idx_id = 0
        idx_st = 1
        idx_name = 2
        width_st = len(headings[idx_st])
        width_id = len(headings[idx_id])
        for ep_name, ep_info in endpoints.items():
            table.add_row([ep_info["id"], ep_info["status"], ep_name])
            width_id = max(width_id, len(str(ep_info["id"])))
            width_st = max(width_st, len(str(ep_info["status"])))

        tsize = shutil.get_terminal_size()
        max_width = max(68, tsize.columns)  # require at least a reasonable size ...
        table.set_max_width(max_width)  # ... but allow to expand to terminal width

        # trickery here, but don't want to subclass texttable -- a very stable
        # library -- at this time; ensure that the only "fungible" column is the
        # name.  Can redress ~when~ *if* this becomes a problem.  "Simple"
        table._compute_cols_width()
        if table._width[idx_id] < width_id:
            table._width[idx_name] -= width_id - table._width[idx_id]
            table._width[idx_id] = width_id
        if table._width[idx_st] < width_st:
            table._width[idx_name] -= width_st - table._width[idx_st]
            table._width[idx_st] = width_st

        # ensure no mistakes -- width of name (well, _any_) column can't be < 0
        table._width[idx_name] = max(10, table._width[idx_name])

        print(table.draw(), file=ofile)
