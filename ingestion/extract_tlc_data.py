from pathlib import Path
import os
import requests
from dotenv import load_dotenv


def download_file() -> Path:
    load_dotenv()
    url = os.getenv("TLC_DATA_URL")
    if not url:
        raise ValueError("TLC_DATA_URL is not set")

    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)
    filename = url.split("/")[-1]
    output_path = output_dir / filename

    response = requests.get(url, timeout=120)
    response.raise_for_status()
    output_path.write_bytes(response.content)
    return output_path


if __name__ == "__main__":
    file_path = download_file()
    print(f"Downloaded to {file_path}")