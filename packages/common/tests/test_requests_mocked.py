from bag3d.common.utils.requests import get_metadata, download_as_str, download_file


def test_get_metadata_parses_timeliness(mock_requests):
    url = "https://api.pdok.nl/brt/top10nl/download/v1_0/dataset"
    mock_requests.get(
        url,
        json={
            "timeliness": [
                {
                    "featuretype": "gebouw",
                    "datetimeTo": "2022-10-08T00:00:00Z",
                },
                {
                    "featuretype": "waterdeel",
                    "datetimeTo": "2022-10-08T00:00:00Z",
                },
            ]
        },
    )

    assert get_metadata(url) == {"timeliness": {"2022-10-08": ["gebouw", "waterdeel"]}}


def test_download_as_str_returns_text(mock_requests):
    url = "https://example.com/AHN4.md5"
    body = "56c731a1814dd73c79a0a5347f8a04c7  C_01CZ1.LAZ\n"
    mock_requests.get(url, text=body)

    assert download_as_str(url=url) == body


def test_download_file_writes_payload(mock_requests, tmp_path):
    url = "https://example.com/AHN4.md5"
    body = "56c731a1814dd73c79a0a5347f8a04c7  C_01CZ1.LAZ\n"
    mock_requests.head(url, headers={"content-length": str(len(body))})
    mock_requests.get(url, body=body.encode())

    path = download_file(url=url, target_path=tmp_path / "checksums.md5")

    assert path is not None
    assert path == tmp_path / "checksums.md5"
    assert path.read_text() == body
