import json
from collections.abc import Generator
from typing import Literal, Optional

import httpx

from forloop_modules.globals.active_entity_tracker import aet


class HttpClient(httpx.Client):
    """
    HTTPX client providing all the typical features like TCP connection pooling, while
    also setting all necessary headers dynamically for each request. It accepts all
    HTTPX arguments.
    """

    def __init__(self, *args, **httpx_kwargs):
        super().__init__(*args, **httpx_kwargs)
        self._sse_parser = SSEParser(httpx_client=self)
        self.sse_stream = self._sse_parser.stream

    def _request(self, url: str, method: str, *, headers: Optional[dict[str, str]] = None, **kwargs):
        headers = headers or {}
        global_headers = {
            # "Authorization": f"Bearer {aet.get_access_token()}",
            "User-Email": aet.user_email or "",
        }
        global_headers.update(headers)
        response = super().request(method, url, headers=global_headers, **kwargs)
        response.ok = response.is_success # Cross compatibility with requests
        # response.raise_for_status()
        return response

    def get(self, url: str, **kwargs):
        return self._request(url, "GET", **kwargs)

    def post(self, url: str, **kwargs):
        return self._request(url, "POST", **kwargs)

    def put(self, url: str, **kwargs):
        return self._request(url, "PUT", **kwargs)

    def delete(self, url: str, **kwargs):
        return self._request(url, "DELETE", **kwargs)


class SSEParser:
    """
    Collect and parse messages from a Server-Sent Event stream. This class augments the HTTPX with
    parsing logic for SSE streams - it collects yielded lines from a SSE and parses it as a
    dictionary.
    """

    def __init__(self, httpx_client: httpx.Client):
        self.client = httpx_client

    def stream(
        self,
        method: Literal["GET", "POST"],
        url: str,
        as_dict: bool = True,
        httpx_kwargs: Optional[dict] = None,
    ) -> Generator[dict, None, None]:
        httpx_kwargs = httpx_kwargs or {}

        with self.client.stream(method, url, **httpx_kwargs) as response:
            response.raise_for_status()
            yield from self._get_messages(response.iter_lines(), as_dict=as_dict)

    def _get_messages(
        self, lines_iter: Generator[str, None, None], as_dict: bool
    ) -> Generator[dict, None, None]:
        message = {}
        for line in lines_iter:
            if message and line == "":  # Yield when reached the end of the message
                yield message
                message = {}
                continue
            elif not message and line == "":  # Ignore consecutive empty lines
                continue

            parsed_line = self._parse_line(line, as_dict)
            message.update(parsed_line)

    def _parse_line(self, line: str, as_dict: bool) -> dict:
        line_type, message_string = line.split(":", 1)

        if line_type not in ["id", "data", "event", "retry"]:
            raise AttributeError(f"SSE line type '{line_type}' not recognized", line)

        if line_type == "data" and as_dict:
            try:
                message = json.loads(message_string)
            except json.JSONDecodeError:
                message = message_string
            return {line_type: message}
        else:
            return {line_type: message_string}
