from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
TARGET_LABEL_NAME = "YNAB Amazon"


def get_gmail_service():
    creds = None
    token_path = Path("token.json")
    creds_path = Path("credentials.json")

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not creds_path.exists():
                raise FileNotFoundError(
                    "Missing credentials.json in this folder."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("gmail", "v1", credentials=creds)


def main():
    service = get_gmail_service()
    labels = service.users().labels().list(userId="me").execute().get("labels", [])
    labels = labels or []

    exact = [l for l in labels if l.get("name") == TARGET_LABEL_NAME]
    partial = [l for l in labels if TARGET_LABEL_NAME.lower() in l.get("name", "").lower()]

    print(f"Total labels: {len(labels)}")
    if exact:
        label = exact[0]
        print(f'Exact match: "{label["name"]}" id={label["id"]}')
        print(f'Search query: label:{label["id"]}')
        return

    if partial:
        print("No exact match found. Partial matches:")
        for label in partial:
            print(f'- "{label.get("name", "(no name)")}" id={label.get("id", "(no id)")}')
        return

    print(f'No label found matching "{TARGET_LABEL_NAME}".')


if __name__ == "__main__":
    main()
