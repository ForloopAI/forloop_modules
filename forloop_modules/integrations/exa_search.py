import uuid
from dataclasses import dataclass
from typing import Any, Optional

import httpx

import forloop_modules.flog as flog
import forloop_modules.utils.synchronization_flags as sf

EXA_SEARCH_URL = "https://api.exa.ai/search"


class ExaSearchError(Exception):
    """Exception raised when an Exa search request fails."""

    def __init__(self, detail: str, status_code: int = 502):
        self.detail = detail
        self.status_code = status_code
        super().__init__(detail)


@dataclass(frozen=True)
class ExaSearchConfig:
    api_key: Optional[str] = None
    search_type: str = "auto"
    timeout_seconds: int = 20
    max_results: int = 20
    max_query_length: int = 500

    @classmethod
    def from_sync(cls) -> "ExaSearchConfig":
        return cls(
            api_key=sf.EXA_API_KEY,
            search_type=sf.EXA_SEARCH_TYPE,
            timeout_seconds=sf.EXA_SEARCH_TIMEOUT_SECONDS,
            max_results=sf.EXA_SEARCH_MAX_RESULTS,
            max_query_length=sf.EXA_SEARCH_MAX_QUERY_LENGTH,
        )


def _validate_search_request(
    query: str, num_results: int, config: ExaSearchConfig
) -> tuple[str, int]:
    normalized_query = query.strip()

    if not normalized_query:
        raise ExaSearchError("Search query is required", status_code=400)

    if len(normalized_query) > config.max_query_length:
        raise ExaSearchError(
            f"Search query must be {config.max_query_length} characters or less",
            status_code=400,
        )

    safe_num_results = min(max(num_results, 1), config.max_results)
    return normalized_query, safe_num_results


def _extract_result_highlights(item: dict[str, Any]) -> list[str]:
    highlights = item.get("highlights")
    if isinstance(highlights, list):
        highlight_parts = [str(part).strip() for part in highlights if part]
        if highlight_parts:
            return highlight_parts

    summary = item.get("summary")
    if summary:
        summary_text = str(summary).strip()
        if summary_text:
            return [summary_text]

    text = item.get("text")
    if text:
        text_content = str(text).strip()
        if text_content:
            return [text_content]

    return []


def _normalize_exa_results(raw_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_results: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for index, item in enumerate(raw_results):
        url = (item.get("url") or "").strip()
        if not url or url in seen_urls:
            continue

        seen_urls.add(url)
        normalized_results.append(
            {
                "id": str(item.get("id") or url or f"result-{index}"),
                "title": (item.get("title") or url).strip(),
                "url": url,
                "highlights": _extract_result_highlights(item),
            }
        )

    return normalized_results


def _build_exa_payload(
    query: str, num_results: int, config: ExaSearchConfig
) -> dict[str, Any]:
    return {
        "query": query,
        "type": config.search_type,
        "numResults": num_results,
        "contents": {
            "highlights": True,
        },
    }


def _map_exa_error(
    status_code: int, response_body: Any, correlation_id: str
) -> ExaSearchError:
    detail = "Exa search request failed"

    if isinstance(response_body, dict):
        detail = response_body.get("error") or detail

    if status_code == 401:
        mapped_status = 502
        detail = "Exa API authentication failed"
    elif status_code == 429:
        mapped_status = 429
        detail = "Exa search rate limit exceeded"
    elif status_code in {400, 422}:
        mapped_status = 400
    elif status_code >= 500:
        mapped_status = 502
    else:
        mapped_status = 502

    flog.warning(
        f"[exa-search:{correlation_id}] Exa returned HTTP {status_code}: {detail}"
    )
    return ExaSearchError(detail, status_code=mapped_status)


def search_exa(
    query: str,
    num_results: int,
    *,
    config: Optional[ExaSearchConfig] = None,
    user_email: Optional[str] = None,
    project_uid: Optional[str] = None,
) -> dict[str, Any]:
    exa_config = config or ExaSearchConfig.from_sync()
    correlation_id = uuid.uuid4().hex

    if not exa_config.api_key:
        flog.error(f"[exa-search:{correlation_id}] EXA_API_KEY is not configured")
        raise ExaSearchError("Exa search is not configured", status_code=503)

    normalized_query, safe_num_results = _validate_search_request(
        query, num_results, exa_config
    )

    flog.info(
        f"[exa-search:{correlation_id}] Starting search "
        f"user_email={user_email} project_uid={project_uid} "
        f"type={exa_config.search_type} "
        f"num_results={safe_num_results} query_length={len(normalized_query)}"
    )

    payload = _build_exa_payload(normalized_query, safe_num_results, exa_config)

    try:
        with httpx.Client(timeout=exa_config.timeout_seconds) as client:
            response = client.post(
                EXA_SEARCH_URL,
                headers={
                    "accept": "application/json",
                    "content-type": "application/json",
                    "x-api-key": exa_config.api_key,
                },
                json=payload,
            )
    except httpx.TimeoutException as error:
        flog.warning(f"[exa-search:{correlation_id}] Exa request timed out")
        raise ExaSearchError("Exa search timed out", status_code=504) from error
    except httpx.HTTPError as error:
        flog.error(f"[exa-search:{correlation_id}] Exa request failed: {error}")
        raise ExaSearchError("Exa search request failed", status_code=502) from error

    try:
        response_body = response.json()
    except ValueError as error:
        flog.error(f"[exa-search:{correlation_id}] Exa returned invalid JSON")
        raise ExaSearchError(
            "Exa search returned an invalid response",
            status_code=502,
        ) from error

    if response.status_code >= 400:
        raise _map_exa_error(response.status_code, response_body, correlation_id)

    exa_request_id = response_body.get("requestId")
    search_type = response_body.get("searchType")
    raw_results = response_body.get("results") or []

    if not isinstance(raw_results, list):
        flog.error(f"[exa-search:{correlation_id}] Exa results payload was malformed")
        raise ExaSearchError(
            "Exa search returned an invalid response",
            status_code=502,
        )

    normalized_results = _normalize_exa_results(raw_results)

    flog.info(
        f"[exa-search:{correlation_id}] Completed search "
        f"exa_request_id={exa_request_id} search_type={search_type} "
        f"normalized_results={len(normalized_results)}"
    )

    return {
        "query": normalized_query,
        "results": normalized_results,
    }
