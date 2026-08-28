import pytest

from ex_agent.application.services import _bounded_report_markdown


def test_report_markdown_is_preserved_when_within_limit() -> None:
    assert _bounded_report_markdown("# 결과\n\n성공") == "# 결과\n\n성공"


def test_report_markdown_is_bounded() -> None:
    report = _bounded_report_markdown("가" * 100, max_chars=80)

    assert len(report) == 80
    assert report.endswith("축약되었습니다.")


def test_empty_report_is_rejected() -> None:
    with pytest.raises(ValueError, match="empty Markdown"):
        _bounded_report_markdown("  ")
