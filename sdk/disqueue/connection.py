from typing import Iterable
import threading
import requests
from time import sleep


class Connection:
    SERVER_LIST_FETCH_FREQUENCY = .1

    def __init__(self, uris: Iterable[str]):
        uris_n = []
        self._primary_server, self._servers = None, None
        for uri in uris:
            uris_n.append(uri[:-1] if uri[-1] == '/' else uri)

        self._servers_lock = threading.Lock()
        self._update_servers(uris_n)
        self._round_robin_index = 0

        self._stop_worker = False

        self._worker_thread = threading.Thread(target=self._worker_routine)
        self._worker_thread.start()

    @property
    def _secondary_servers(self):
        self._servers_lock.acquire()
        ans = list(filter(lambda x: x != self._primary_server, self._servers))
        self._servers_lock.release()
        return ans

    def _update_servers(self, uris: Iterable[str]):
        uris = list(uris)
        invalid_uris = []
        for uri in uris:
            try:
                res = requests.get(uri + '/managers')
            except requests.exceptions.ConnectionError:
                invalid_uris.append(uri)
                continue
            if res.ok:
                self._servers_lock.acquire()
                managers = res.json()['managers']
                self._servers = []
                for server in managers:
                    if server[1]:
                        self._primary_server = server[0]
                    else:
                        self._servers.append(server[0])
                self._servers_lock.release()
            else:
                invalid_uris.append(uri)
        for uri in invalid_uris:
            uris.remove(uri)

        if self._primary_server is None:
            raise Exception('No primary server found')

    def _send_to_primary(self, action, path: str, data: dict = None, params: dict = None):
        try:
            res = action(self._primary_server + path, data=data, params=params)
            return res
        except requests.exceptions.ConnectionError:
            self._servers_lock.acquire()
            self._servers.remove(self._primary_server)
            self._servers_lock.release()
            self._update_servers(self._servers)
            return self._send_to_primary(action, path, data=data, params=params)

    def _send_to_secondary(self, action, path: str, data: dict = None, params: dict = None):

        if len(self._secondary_servers) == 0:
            try:
                res = action(self._primary_server + path, data=data, params=params)
                return res
            except requests.exceptions.ConnectionError:
                raise Exception('No server found')
        secondary_server = self._secondary_servers[self._round_robin_index]
        try:
            res = action(secondary_server + path, data=data, params=params)
            self._round_robin_index = (self._round_robin_index + 1) % len(self._secondary_servers)
            return res
        except requests.exceptions.ConnectionError:
            self._servers_lock.acquire()
            self._servers.remove(secondary_server)
            self._round_robin_index = self._round_robin_index % len(self._secondary_servers)
            self._servers_lock.release()
            return self._send_to_secondary(action, path, data=data, params=params)

    def get(self, path: str, data: dict = None, params: dict = None):
        return self._send_to_secondary(requests.get, path, data=data, params=params)

    def post_readonly(self, path: str, data: dict = None, params: dict = None):
        return self._send_to_secondary(requests.post, path, data=data, params=params)

    def post(self, path: str, data: dict = None, params: dict = None):
        return self._send_to_primary(requests.post, path, data=data, params=params)

    def _worker_routine(self):
        while not self._stop_worker:
            try:
                self._update_servers(self._servers)
            finally:
                sleep(1 / self.SERVER_LIST_FETCH_FREQUENCY)

    def __del__(self):
        self._stop_worker = True
        if hasattr(self, '_worker_thread'):
            self._worker_thread.join()
