import time
import random
import requests
from requests.auth import HTTPBasicAuth


class WistiaClient:
    def __init__(
        self,
        api_token: str,
        base_url: str = "https://api.wistia.com/v1",
        timeout: int = 30,
        max_retries: int = 6,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.auth = HTTPBasicAuth("api", api_token)

    def _get(self, path: str, params=None):
        url = f"{self.base_url}{path}"
        params = params or {}

        for attempt in range(self.max_retries):
            response = self.session.get(
                url,
                params=params,
                auth=self.auth,
                timeout=self.timeout
            )

            if response.status_code == 200:
                return response.json()

            if response.status_code == 429:
                time.sleep(2 ** attempt)
                continue

            if response.status_code >= 500:
                time.sleep(2 ** attempt + random.random())
                continue

            raise Exception(f"API error {response.status_code}: {response.text}")

        raise Exception(f"Failed after retries: {url}")

    # ---------- Media ----------
    def get_media_stats(self, media_id):
        return self._get(f"/stats/medias/{media_id}.json")

    def get_media_engagement(self, media_id):
        return self._get(f"/stats/medias/{media_id}/engagement.json")

    def get_media_by_date(self, media_id, start_date, end_date):
        return self._get(
            f"/stats/medias/{media_id}/by_date.json",
            params={"start_date": start_date, "end_date": end_date}
        )

    # ---------- Pagination ----------
    def paginate(self, path, per_page=100):
        page = 1
        while True:
            data = self._get(path, params={"page": page, "per_page": per_page})
            if not data:
                break
            yield page, data
            page += 1
