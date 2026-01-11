import os
import json
import uuid
from datetime import date
import boto3

from ingest.wistia_client import WistiaClient

# ---------------- CONFIG ----------------
S3_BUCKET = "dewistia-analytics"
RAW_PREFIX = "raw/wistia"
MEDIA_IDS = ["gskhw4w4lm", "v08dlrgr7v"]

RUN_DATE = str(date.today())
TOKEN_ENV = "WISTIA_API_TOKEN"
# ----------------------------------------


def write_json(s3, key, payload):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json"
    )


def main():
    token = os.getenv(TOKEN_ENV)
    if not token:
        raise RuntimeError("WISTIA_API_TOKEN not set")

    s3 = boto3.client("s3")
    client = WistiaClient(token)

    # -------- MEDIA ENDPOINTS --------
    for media_id in MEDIA_IDS:
        # media_by_date
        data = client.get_media_by_date(media_id, RUN_DATE, RUN_DATE)
        key = (
            f"{RAW_PREFIX}/endpoint=media_by_date/"
            f"run_date={RUN_DATE}/media_id={media_id}/"
            f"part-{uuid.uuid4().hex}.json"
        )
        write_json(s3, key, data)

        # engagement
        data = client.get_media_engagement(media_id)
        key = (
            f"{RAW_PREFIX}/endpoint=media_engagement/"
            f"run_date={RUN_DATE}/media_id={media_id}/"
            f"part-{uuid.uuid4().hex}.json"
        )
        write_json(s3, key, data)

        # media stats
        data = client.get_media_stats(media_id)
        key = (
            f"{RAW_PREFIX}/endpoint=media_stats/"
            f"run_date={RUN_DATE}/media_id={media_id}/"
            f"part-{uuid.uuid4().hex}.json"
        )
        write_json(s3, key, data)

    # -------- EVENTS (paginated) --------
    for page, data in client.paginate("/stats/events.json"):
        key = (
            f"{RAW_PREFIX}/endpoint=events/"
            f"run_date={RUN_DATE}/page={page:04d}/"
            f"part-{uuid.uuid4().hex}.json"
        )
        write_json(s3, key, data)

    # -------- VISITORS (paginated) --------
    for page, data in client.paginate("/stats/visitors.json"):
        key = (
            f"{RAW_PREFIX}/endpoint=visitors/"
            f"run_date={RUN_DATE}/page={page:04d}/"
            f"part-{uuid.uuid4().hex}.json"
        )
        write_json(s3, key, data)

    print("✅ Wistia raw ingestion completed")


if __name__ == "__main__":
    main()
