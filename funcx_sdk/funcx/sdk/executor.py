from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import json
import logging
import queue
import threading
import time
import typing as t
from asyncio import QueueEmpty

from funcx.errors import FuncxTaskExecutionFailed
from funcx.sdk.asynchronous.funcx_future import FuncXFuture
from funcx.sdk.asynchronous.ws_polling_task import WebSocketPollingTask
from funcx.sdk.client import FuncXClient

log = logging.getLogger(__name__)


class TaskSubmissionInfo:
    def __init__(
        self,
        *,
        future_id: int,
        function_id: str,
        endpoint_id: str,
        args: t.Tuple[t.Any],
        kwargs: t.Dict[str, t.Any],
    ):
        self.future_id = future_id
        self.function_id = function_id
        self.endpoint_id = endpoint_id
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return (
            "TaskSubmissionInfo("
            f"future_id={self.future_id}, "
            f"function_id='{self.function_id}', "
            f"endpoint_id='{self.endpoint_id}', "
            "args=..., kwargs=...)"
        )


class AtomicController:
    """This is used to synchronize between the FuncXExecutor which starts
    WebSocketPollingTasks and the WebSocketPollingTask which closes itself when there
    are 0 tasks.
    """

    def __init__(self, start_callback, stop_callback):
        self._value = 0
        self._lock = threading.Lock()
        self.start_callback = start_callback
        self.stop_callback = stop_callback

    def reset(self):
        """Reset the counter to 0; this method does not call callbacks"""
        with self._lock:
            self._value = 0

    def increment(self, val: int = 1):
        with self._lock:
            if self._value == 0:
                self.start_callback()
            self._value += val

    def decrement(self):
        with self._lock:
            self._value -= 1
            if self._value == 0:
                self.stop_callback()
            return self._value

    def value(self):
        with self._lock:
            return self._value

    def __repr__(self):
        return f"AtomicController value:{self._value}"


class FuncXExecutor2(concurrent.futures.Executor):
    """Extends the concurrent.futures.Executor class to layer this interface
    over funcX. The executor returns future objects that are asynchronously
    updated with results by the WebSocketPollingTask using a websockets connection
    to the hosted funcx-websocket-service.
    """

    def __init__(
        self,
        funcx_client: FuncXClient | None = None,
        label: str = "FuncXExecutor",
        batch_enabled: bool = True,
        batch_interval: float = 1.0,
        batch_size: int = 100,
    ):
        """
        Parameters
        ==========

        funcx_client : client object
            Instance of FuncXClient to be used by the executor

        label : str
            Optional string label to name the executor.
            Default: 'FuncXExecutor'
        """

        if funcx_client:
            self.funcx_client = funcx_client
        else:
            self.funcx_client = FuncXClient()

        self.label = label
        self.batch_enabled = batch_enabled
        self.batch_interval = batch_interval
        self.batch_size = batch_size
        self.task_outgoing: queue.Queue[TaskSubmissionInfo | None] = queue.Queue()

        self.task_count_submitted = 0
        self._future_counter: int = 0
        self._counter_future_map: t.Dict[int, FuncXFuture] = {}
        self._function_registry: t.Dict[t.Any, str] = {}
        self._kill_event: t.Optional[threading.Event] = None
        self._task_submit_thread: t.Optional[threading.Thread] = None

        self.poller_thread = ExecutorPollerThread(self.funcx_client.init_kwargs)
        self.poller_thread.start()

        if self.batch_enabled:
            log.debug("Batch submission enabled.")
            self.start_batching_thread()

        atexit.register(self.shutdown)

    def _reset_poller(self):
        if self.poller_thread.is_alive():
            self.poller_thread.shutdown()
        self.poller_thread = ExecutorPollerThread(self.funcx_client.init_kwargs)
        self.poller_thread.start()

        self._future_counter = 0
        while self._counter_future_map:
            _, fut = self._counter_future_map.popitem()
            if not fut.done():
                fut.cancel()

    @property
    def results_ws_uri(self) -> str:
        return self.funcx_client.results_ws_uri

    @property
    def task_group_id(self) -> str:
        return self.funcx_client.session_task_group_id

    def start_batching_thread(self):
        self._kill_event = threading.Event()
        # Start the task submission thread
        self._task_submit_thread = threading.Thread(
            target=self._submit_task_kernel,
            args=(self._kill_event,),
            name="FuncX-Submit-Thread",
        )
        self._task_submit_thread.daemon = True
        self._task_submit_thread.start()
        log.debug("Started task submit thread")

    def register_function(self, func: t.Callable, container_uuid=None):
        # Please note that this is a partial implementation, not all function
        # registration options are fleshed out here.
        log.debug(f"Function:{func} is not registered. Registering")
        try:
            function_id = self.funcx_client.register_function(
                func,
                function_name=func.__name__,
                container_uuid=container_uuid,
            )
        except Exception:
            log.error(f"Error in registering {func.__name__}")
            raise
        else:
            self._function_registry[func] = function_id
            log.debug(f"Function registered with id:{function_id}")

    def submit(self, function, *args, endpoint_id=None, container_uuid=None, **kwargs):
        """Initiate an invocation

        Parameters
        ----------
        function : Function/Callable
            Function / Callable to execute

        *args : Any
            Args as specified by the function signature

        endpoint_id : uuid str
            Endpoint UUID string. Required

        **kwargs : Any
            Arbitrary kwargs

        Returns
        -------
        Future : funcx.sdk.asynchronous.funcx_future.FuncXFuture
            A future object
        """

        if function not in self._function_registry:
            self.register_function(function)
        future_id = self._future_counter
        self._future_counter += 1

        assert endpoint_id is not None, "endpoint_id key-word argument must be set"

        task = TaskSubmissionInfo(
            future_id=future_id,  # an integer; because we don't yet know the task id
            function_id=self._function_registry[function],
            endpoint_id=endpoint_id,
            args=args,
            kwargs=kwargs,
        )

        fut = FuncXFuture()
        self._counter_future_map[future_id] = fut

        if self.batch_enabled:
            self.task_outgoing.put(task)
        else:
            self._submit_tasks([task])

        return fut

    def _submit_task_kernel(self, kill_event: threading.Event):
        """
        Fetch enqueued tasks task_outgoing queue and submit them to funcX in batches
        of up to self.batch_size.

        Parameters
        ==========
        kill_event : threading.Event
            Sentinel event; used to stop the thread and exit.
        """
        to_send = self.task_outgoing  # cache lookup
        interval = self.batch_interval
        while not kill_event.is_set():
            tasks: t.List[TaskSubmissionInfo] = []
            try:
                task = to_send.get()  # Block while waiting for first result ...
                beg = time.time()
                while task is not None:
                    tasks.append(task)
                    if (
                        not (len(tasks) < self.batch_size)
                        or time.time() - beg > interval
                    ):
                        break
                    task = to_send.get(block=False)  # ... but don't block thereafter
            except queue.Empty:
                pass
            if tasks:
                log.info(f"Submitting tasks to funcX: {len(tasks)}")
                self._submit_tasks(tasks)

        log.debug("Exiting")

    def _submit_tasks(self, tasks: t.List[TaskSubmissionInfo]):
        """Submit a batch of tasks"""
        batch = self.funcx_client.create_batch(task_group_id=self.task_group_id)
        for task in tasks:
            batch.add(
                *task.args,
                **task.kwargs,
                endpoint_id=task.endpoint_id,
                function_id=task.function_id,
            )
            log.debug(f"Adding task {task} to funcX batch")
        try:
            batch_tasks = self.funcx_client.batch_run(batch)
            self.task_count_submitted += len(batch_tasks)
            log.debug("Batch submitted to task_group: %s", self.task_group_id)
        except Exception:
            log.error(f"Error submitting {len(tasks)} tasks to funcX")
            raise
        else:
            for i, task in enumerate(tasks):
                task_uuid: str = batch_tasks[i]
                fut = self._counter_future_map.pop(task.future_id)
                fut.task_id = task_uuid
                self.poller_thread.watch_for_task(fut)

    def reload_tasks(self) -> t.Iterable[FuncXFuture]:
        """
        Load the set of tasks associated with this Executor's Task Group (FuncXClient)
        from the server and return a set of futures, one for each task.  This is
        nominally intended to "reattach" to a previously initiated session, based on
        the Task Group ID.  An example use might be::

            import sys
            import typing as T
            from funcx import FuncXClient, FuncXExecutor
            from funcx.sdk.executor import FuncXFuture

            fxc_kwargs = {}
            if len(sys.argv) > 1:
                fxc_kwargs["task_group_id"] = sys.argv[1]

            def example_funcx_kernel(num):
                result = f"your funcx logic result, from task: {num}"
                return result

            fxclient = FuncXClient(**fxc_kwargs)
            fxexec = FuncXExecutor(fxclient)

            # Save the task_group_id somewhere.  Perhaps in a file, or less
            # robustly "as mere text" on your console:
            print("If this script dies, rehydrate futures with this "
                 f"Task Group ID: {fxexec.task_group_id}")

            futures: T.Iterable[FuncXFuture] = []
            results, exceptions = [], []
            if "task_group_id" in fxc_kwargs:
                print(f"Reloading tasks from Task Group ID: {fxexec.task_group_id}")
                futures = fxexec.reload_tasks()

                # Ask server once up-front if there are any known results before
                # waiting for each result in turn (below):
                task_ids = [f.task_id for f in futures]
                finished_tasks = set()
                for task_id, state in fxclient.get_batch_result(task_ids).items():
                    if not state["pending"]:
                        finished_tasks.add(task_id)
                        if state["status"] == "success":
                            results.append(state["result"])
                        else:
                            exceptions.append(state["exception"])
                futures = [f for f in futures if f.task_id not in finished_tasks]

            else:
                print("New session; creating FuncX tasks ...")
                ep_id = "<YOUR_ENDPOINT_UUID>"
                for i in range(1, 5):
                    futures.append(
                        fxexec.submit(example_funcx_kernel, endpoint_id=ep_id)
                    )

                # ... Right here, your script dies for [SILLY REASON;
                #           DID YOU LOSE POWER?] ...

            # Get results:
            for f in futures:
                try:
                    results.append(f.result(timeout=10))
                except Exception as exc:
                    exceptions.append(exc)

        Returns
        -------
        An iterable of futures.

        Known throws
        ------
        - The usual (unhandled) request errors (e.g., no connection; invalid
          authorization)
        - ValueError if the server response is incorrect
        - KeyError if the server did not return an expected response

        Notes
        -----
        Any previous futures received from this executor will be cancelled.
        """

        # step 1: cleanup!
        self._reset_poller()

        # step 2: from server, acquire list of related task ids and make futures
        r = self.funcx_client.web_client.get_taskgroup_tasks(self.task_group_id)
        if r["taskgroup_id"] != self.task_group_id:
            msg = (
                "Server did not respond with requested TaskGroup Tasks.  "
                f"(Requested tasks for {self.task_group_id} but received "
                f"tasks for {r['taskgroup_id']}"
            )
            raise ValueError(msg)

        # step 3: create the associated set of futures
        futures: t.List[FuncXFuture] = []
        for task in r.get("tasks", []):
            task_uuid: str = task["id"]
            fut = FuncXFuture(task_uuid)
            self.poller_thread.watch_for_task(fut)
            futures.append(fut)

        if not futures:
            log.warning(f"Received no tasks for Task Group ID: {self.task_group_id}")

        # step 4: the goods for the consumer
        return futures

    def shutdown(self):
        if self.batch_enabled and self._kill_event:
            self._kill_event.set()  # Reminder: stops the batch submission thread
            self.task_outgoing.put(None)

        self.poller_thread.shutdown()

        log.debug(f"Executor:{self.label} shutting down")


class ExecutorPollerThread(threading.Thread):
    """This encapsulates the creation of the thread on which event loop lives,
    the instantiation of the WebSocketPollingTask onto the event loop and the
    synchronization primitives used (AtomicController)
    """

    def __init__(self, funcx_client_kwargs: dict[str, t.Any]):
        """
        Parameters
        ==========

        funcx_client : client object
            Instance of FuncXClient to be used by the executor

        function_future_map
            A mapping of task_uuid to associated FuncXFutures; used for updating
            when the upstream websocket service sends updates
        """

        super().__init__()
        self.funcx_client_kwargs = funcx_client_kwargs  # Thread safety; recreate
        self._thread_id = threading.get_ident()
        self._time_to_stop = False

        self._ws_task: WebSocketPollingTask | None = None
        self._eventloop: asyncio.AbstractEventLoop | None = None

        # Results that the user expects to receive from the server
        self._pending_results: dict[str, FuncXFuture] = {}

        # Results received from the server.  The keys correlate with the
        # _pending_results dict's keys
        self._received_results: dict[str, dict] = {}

        # A flag to indicate that useful work is ready; the processor clears
        # this event, and new results set it (e.g., `.watch_for_task()`)
        self._data_arrived: asyncio.Event | None = None

    def start(self) -> None:
        if self.is_alive():
            return
        super().start()

    async def _has_new_items(self, timeout=1) -> bool:
        """
        Internal convenience method: wait up to timeout seconds to determine if
        there are new items to process.
        """
        try:
            return await asyncio.wait_for(self._data_arrived.wait(), timeout=timeout)
        except asyncio.exceptions.TimeoutError:
            pass
        return False

    def run(self) -> None:
        self._time_to_stop = False
        self._thread_id = threading.get_ident()
        self._eventloop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._eventloop)
        self._data_arrived = asyncio.Event()
        funcx_client = FuncXClient(**self.funcx_client_kwargs)

        async def _kernel():
            self._ws_task = WebSocketPollingTask(
                funcx_client,
                asyncio.get_running_loop(),
                init_task_group_id=funcx_client.session_task_group_id,
                results_ws_uri=funcx_client.results_ws_uri,
                auto_start=False,
            )

            result_queue = asyncio.Queue()
            deserializer = funcx_client.fx_serializer.deserialize
            res_handler = asyncio.create_task(self._result_handler(result_queue))
            res_finisher = asyncio.create_task(self._result_finisher(deserializer))

            while not self._time_to_stop:
                if not await self._has_new_items():
                    continue

                await self._web_socket_poller(result_queue)

            self._time_to_stop = True
            self._ws_task.closed_by_main_thread = True

            await self._ws_task.close()
            res_handler.cancel()
            res_finisher.cancel()

        self._eventloop.run_until_complete(_kernel())

    def watch_for_task(self, task_fut: FuncXFuture):
        if self._time_to_stop:
            raise RuntimeError("Request to watch task but poller thread is stopped.")

        self._pending_results[task_fut.task_id] = task_fut
        if self.is_alive():
            self._eventloop.call_soon(self._data_arrived.set)
        else:
            log.warning(
                "Added result future, but Poller thread is not active (%s)",
                task_fut.task_id,
            )

    async def _web_socket_poller(self, result_queue: asyncio.Queue):
        """Start ws and listen for tasks.
        If a remote disconnect breaks the ws, close the ws and reconnect"""
        assert self._ws_task is not None  # created by the thread

        # Step 1: Loop until it's time to stop
        while not self._time_to_stop:
            log.debug("Connecting to websocket.")
            # Step 2: Tell the websocket server what queue we want to watch
            await self._ws_task.init_ws(start_message_handlers=False)

            # Step 3: Put incoming results into result_queue
            recv = asyncio.create_task(self._ws_task.recv_incoming(result_queue))
            self._time_to_stop = await recv  # remote-side disconnect?  Then loop again

            log.debug("Attempting to close websocket.")
            await self._ws_task.close()

    async def _result_handler(self, result_queue: asyncio.Queue):
        pending_futures = self._pending_results
        while not self._time_to_stop:
            try:
                res = await asyncio.wait_for(result_queue.get(), timeout=1)
            except asyncio.TimeoutError:
                continue

            try:
                data = json.loads(res)
            except json.JSONDecodeError as exc:
                log.error(f"Unable to parse result message: {exc}")
                continue

            task_id = data.get("task_id")
            if task_id:
                self._received_results[task_id] = data
                self._data_arrived.set()
            else:
                # This is not an expected case.  If upstream does not return a
                # task_id, then we have a larger error in play.  Time to shut down
                # (annoy the user!) and field the requisite bug reports.
                upstream_error = data.get("exception", "(no reason given!)")
                errmsg = f"Upstream error: {upstream_error}\nShutting down connection."
                log.error(errmsg)
                self._time_to_stop = True
                for fut in pending_futures.values():
                    if not fut.done():
                        fut.cancel()
                return

    async def _result_finisher(self, deserializer: t.Callable):
        pending_results = self._pending_results
        received_results = self._received_results
        while not self._time_to_stop:
            if not await self._has_new_items():
                continue
            self._data_arrived.clear()

            completed_task_ids = pending_results.keys() & received_results.keys()
            for task_id in completed_task_ids:
                task_fut = pending_results.pop(task_id)
                data = received_results.pop(task_id)

                try:
                    status = str(data.get("status")).lower()
                    if status == "success" and "result" in data:
                        task_fut.set_result(deserializer(data["result"]))
                    elif "exception" in data:
                        task_fut.set_exception(
                            FuncxTaskExecutionFailed(
                                data["exception"], data["completion_t"]
                            )
                        )
                    else:
                        msg = f"Data contained neither result nor exception: {data}"
                        task_fut.set_exception(Exception(msg))
                except Exception as exc:
                    task_exc = Exception(
                        f"Malformed or unexpected data structure.  Task data: {data}",
                    )
                    task_exc.__cause__ = exc
                    task_fut.set_exception(task_exc)

                continue

    def shutdown(self):
        """
        Shut down the thread and cancel any outstanding result futures.

        N.B. joins the thread, and so _must_ be called by the parent process
        """
        self._time_to_stop = True
        if self.is_alive():
            if self._ws_task:
                self._ws_task.closed_by_main_thread = True

        while self._pending_results:
            _, fut = self._pending_results.popitem()
            if not fut.done():
                fut.cancel()

        self.join(timeout=5)


class FuncXExecutor(concurrent.futures.Executor):
    def __init__(
        self,
        endpoint_id: str,
        container_id: str | None = None,
        batch_size: int = 512,
        label: str = "FuncXExecutor",
        function_registry: dict | None = None,
        funcx_client_kwargs: dict | None = None,
    ):
        self.endpoint_id = endpoint_id
        self.batch_size = batch_size
        self.label = label
        self.container_id = container_id
        self._function_registry = function_registry or {}
        self._funcx_client_kwargs = funcx_client_kwargs or {}
        self._funcx_client: FuncXClient | None = None

        # Results that the user expects to receive from the server
        self._pending_results: dict[str, FuncXFuture] = {}

        # Results received from the server.  The keys correlate with the
        # _pending_results dict's keys
        self._received_results: dict[str, dict] = {}

        # A flag to indicate that useful work is ready; the processor clears
        # this event, and new results set it (e.g., `.watch_for_task()`)
        self._check_results: asyncio.Event | None = None

        self.task_count_submitted = 0
        self._to_submit: asyncio.Queue | None = None
        self._tasks_to_send: asyncio.Queue[
            tuple[FuncXFuture, TaskSubmissionInfo]
        ] | None = None

        self._task_loop: asyncio.AbstractEventLoop | None = None
        self._network_loop: asyncio.AbstractEventLoop | None = None

        threading.Thread(target=self._network_thread_impl, daemon=True).start()
        threading.Thread(target=self._async_thread_impl, daemon=True).start()

        while not (self._network_loop and self._network_loop.is_running()):
            log.debug("Waiting for network loop to initialize ...")
            time.sleep(0.5)
        while not (self._task_loop and self._task_loop.is_running()):
            log.debug("Waiting for task loop to initialize ...")
            time.sleep(0.5)

    @property
    def task_group_id(self) -> str:
        assert self._funcx_client is not None
        return self._funcx_client.session_task_group_id

    @task_group_id.setter
    def task_group_id(self, val) -> None:
        assert self._funcx_client is not None
        self._funcx_client.session_task_group_id = val

    def submit(self, fn: t.Callable, *args, **kwargs) -> FuncXFuture:
        f = FuncXFuture()

        def enqueue_it():
            self._to_submit.put_nowait((f, fn, args, kwargs))
        self._task_loop.call_soon_threadsafe(enqueue_it)
        return f

    def map(self, fn, *iterables, timeout=None, chunksize=1) -> t.Iterator:
        pass

    def shutdown(self, wait=True, *, cancel_futures=False):
        self._task_loop.stop()
        self._network_loop.stop()
        self._task_loop = None
        self._network_loop = None

    def _network_thread_impl(self) -> None:
        self._network_thread_id = threading.get_ident()
        self._network_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._network_loop)

        log.debug("Creating FuncXClient")
        self._funcx_client = FuncXClient(**self._funcx_client_kwargs)
        debug_args = self.label, self._network_thread_id
        log.debug(
            "FuncXClient instantiated for executor (%s, thread: %s) - %s",
            *debug_args,
            self._funcx_client_kwargs
        )

        self._network_loop.run_forever()
        log.debug("Network loop ending (%s, thread: %s)", *debug_args)

    def _network_thread_check(self) -> None:
        if self._network_thread_id != threading.get_ident():
            raise RuntimeError(
                "Called from wrong thread.  The network thread is "
                f"{self._network_thread_id} while this thread is "
                f"{threading.get_ident()}")

    def _network_register_function(self, fut: asyncio.Future[str], func: t.Callable):
        self._network_thread_check()

        try:
            function_id = self._funcx_client.register_function(
                func,
                function_name=func.__name__,
                container_uuid=self.container_id,
            )
            log.debug(
                "Network thread - function registered: %s -> %s",
                func.__name__,
                function_id
            )

            def _set():
                fut.set_result(function_id)
        except Exception as exc:
            def _set():
                fut.set_exception(exc)
        fut.get_loop().call_soon_threadsafe(_set)

    def _network_submit_tasks(
        self,
        fut: asyncio.Future[list[str]],
        tasks: t.Iterable[TaskSubmissionInfo]
    ):
        self._network_thread_check()

        batch = self._funcx_client.create_batch(task_group_id=self.task_group_id)
        for task in tasks:
            batch.add(
                *task.args,
                **task.kwargs,
                endpoint_id=task.endpoint_id,
                function_id=task.function_id,
            )
            log.debug(f"Task added to funcX batch: %s", task)
        try:
            task_ids = self._funcx_client.batch_run(batch)
            log.debug("Batch submitted to task_group: %s", self.task_group_id)
            self.task_count_submitted += len(task_ids)

            def _set():
                fut.set_result(task_ids)
        except Exception as exc:
            def _set():
                fut.set_exception(exc)
        fut.get_loop().call_soon_threadsafe(_set)

    def _async_thread_impl(self) -> None:
        self._task_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._task_loop)

        self._to_submit = asyncio.Queue()
        self._tasks_to_send = asyncio.Queue()
        self._check_results = asyncio.Event()

        # Cheeky and "not safe" generally, but okay for the time being because
        # the websocket polling task (currently) uses only thread-safe items.
        # We'll need to address this if that changes.
        while not self._funcx_client:
            time.sleep(0.01)

        self._ws_task = WebSocketPollingTask(
            self._funcx_client,
            self._task_loop,
            init_task_group_id=self.task_group_id,
            results_ws_uri=self._funcx_client.results_ws_uri,
            auto_start=False,
        )

        result_queue = asyncio.Queue()
        deserializer = self._funcx_client.fx_serializer.deserialize
        self._task_loop.create_task(self._prepare_tasks())
        self._task_loop.create_task(self._send_tasks_upstream())
        self._task_loop.create_task(self._web_socket_poller(result_queue))
        self._task_loop.create_task(self._result_handler(result_queue))
        self._task_loop.create_task(self._result_finisher(deserializer))

        self._task_loop.run_forever()
        log.debug("Executor thread ends (%s - %s)", self.label, threading.get_ident())

    async def _prepare_tasks(self):
        log.debug(
            "FuncXExecutor _prepare_tasks begins (%s, thread: %s)",
            self.label,
            threading.get_ident()
        )
        task_counter = 0
        while True:
            fut, fn, args, kwargs = await self._to_submit.get()
            if fn not in self._function_registry:
                log.debug("Registering new function: %s", fn.__name__)
                await self._register_function(fn)

            task_counter += 1
            task = TaskSubmissionInfo(
                future_id=task_counter,  # Unused anymore; useful for debug?
                function_id=self._function_registry[fn],
                endpoint_id=self.endpoint_id,
                args=args,
                kwargs=kwargs,
            )
            await self._tasks_to_send.put((fut, task))

            self._to_submit.task_done()

    async def _register_function(self, func: t.Callable):
        # Note that this is a partial implementation; not all function
        # registration options are fleshed out here.
        f: asyncio.Future[str] = self._task_loop.create_future()
        self._network_loop.call_soon_threadsafe(
            self._network_register_function, f, func
        )
        await f

        try:
            function_id = f.result()
            self._function_registry[func] = function_id
            log.debug("Registered function; ID: %s", function_id)
        except Exception as exc:
            log.error(f"Failed to register {func.__name__}.  (Exception: {exc})")
            raise

    async def _send_tasks_upstream(self) -> None:
        log.debug(
            "FuncXExecutor _send_tasks_upstream begins (%s, thread: %s)",
            self.label,
            threading.get_ident()
        )
        while True:
            fut, task = await self._tasks_to_send.get()
            futs: list[FuncXFuture] = [fut]
            tasks: list[TaskSubmissionInfo] = [task]
            while len(tasks) < self.batch_size:
                try:
                    fut, task = self._tasks_to_send.get_nowait()
                    futs.append(fut)
                    tasks.append(task)
                except QueueEmpty:
                    break

            f: asyncio.Future[list[str]] = self._task_loop.create_future()
            self._network_loop.call_soon_threadsafe(
                self._network_submit_tasks, f, tasks
            )
            await f
            try:
                task_ids = f.result()
            except Exception as exc:
                log.error(f"Failed to submit tasks to funcX.  Exception: {exc}")
                raise

            for fut, task, task_uuid in zip(futs, tasks, task_ids):
                fut.task_id = task_uuid
                self._pending_results[task_uuid] = fut

            self._check_results.set()
            del futs, tasks

    async def _web_socket_poller(self, result_queue: asyncio.Queue):
        """Start ws and listen for tasks.
        If a remote disconnect breaks the ws, close the ws and reconnect"""
        assert self._ws_task is not None  # created by the thread

        log.debug(
            "FuncXExecutor _web_socket_poller begins (%s, thread: %s)",
            self.label,
            threading.get_ident()
        )
        # Step 1: Loop until it's time to stop
        while True:
            await self._check_results.wait()

            log.debug("Connecting to websocket.")
            # Step 2: Tell the websocket server what queue we want to watch
            await self._ws_task.init_ws(start_message_handlers=False)

            # Step 3: Put incoming results into result_queue
            await asyncio.create_task(self._ws_task.recv_incoming(result_queue))

            log.debug("Attempting to close websocket.")
            await self._ws_task.close()

    async def _result_handler(self, result_queue: asyncio.Queue):
        log.debug(
            "FuncXExecutor _result_handler begins (%s, thread: %s)",
            self.label,
            threading.get_ident()
        )
        pending_futures = self._pending_results
        while True:
            res = await result_queue.get()

            try:
                data = json.loads(res)
            except json.JSONDecodeError as exc:
                log.error(f"Unable to parse result message: {exc}")
                continue

            task_id = data.get("task_id")
            if task_id:
                self._received_results[task_id] = data
                self._check_results.set()
            else:
                # This is not an expected case.  If upstream does not return a
                # task_id, then we have a larger error in play.  Time to shut down
                # (annoy the user!) and field the requisite bug reports.
                upstream_error = data.get("exception", "(no reason given!)")
                errmsg = f"Upstream error: {upstream_error}\nShutting down connection."
                log.error(errmsg)
                for fut in pending_futures.values():
                    if not fut.done():
                        fut.cancel()
                self.shutdown()
                return

    async def _result_finisher(self, deserializer: t.Callable):
        log.debug(
            "FuncXExecutor _result_finisher begins (%s, thread: %s)",
            self.label,
            threading.get_ident()
        )
        pending_results = self._pending_results
        received_results = self._received_results
        while True:
            await self._check_results.wait()
            self._check_results.clear()

            completed_task_ids = pending_results.keys() & received_results.keys()
            for task_id in completed_task_ids:
                task_fut = pending_results.pop(task_id)
                data = received_results.pop(task_id)

                try:
                    status = str(data.get("status")).lower()
                    if status == "success" and "result" in data:
                        task_fut.set_result(deserializer(data["result"]))
                    elif "exception" in data:
                        task_fut.set_exception(
                            FuncxTaskExecutionFailed(
                                data["exception"], data["completion_t"]
                            )
                        )
                    else:
                        msg = f"Data contained neither result nor exception: {data}"
                        task_fut.set_exception(Exception(msg))
                except Exception as exc:
                    task_exc = Exception(
                        f"Malformed or unexpected data structure.  Task data: {data}",
                    )
                    task_exc.__cause__ = exc
                    task_fut.set_exception(task_exc)

                continue
