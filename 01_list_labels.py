from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


# Read-only Gmail access is sufficient for listing labels.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


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
                    "Missing credentials.json. Download OAuth client JSON from Google Cloud "
                    "and save it as credentials.json in this folder."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
            creds = flow.run_local_server(port=0)

        token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("gmail", "v1", credentials=creds)


def main():
    service = get_gmail_service()
    response = service.users().labels().list(userId="me").execute()
    labels = response.get("labels", []) or []

    if not labels:
        print("No labels found.")
        return

    print(f"Found {len(labels)} labels:")
    for label in labels:
        name = label.get("name", "(no name)")
        label_id = label.get("id", "(no id)")
        print(f"- {name} [{label_id}]")


if __name__ == "__main__":
    main()
