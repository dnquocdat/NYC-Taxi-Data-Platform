from scripts.superset_clickhouse_uri import build_superset_clickhouse_uri


def test_superset_clickhouse_uri_redacts_password_by_default() -> None:
    uri = build_superset_clickhouse_uri(
        user="default",
        password="secret",
        host="clickhouse",
        port=8123,
        database="nyc_taxi",
        show_password=False,
    )

    assert uri == "clickhousedb://default:********@clickhouse:8123/nyc_taxi"


def test_superset_clickhouse_uri_can_render_password_for_copy_paste() -> None:
    uri = build_superset_clickhouse_uri(
        user="nyc user",
        password="p@ss word",
        host="clickhouse",
        port=8123,
        database="nyc_taxi",
        show_password=True,
    )

    assert uri == "clickhousedb://nyc%20user:p%40ss%20word@clickhouse:8123/nyc_taxi"
