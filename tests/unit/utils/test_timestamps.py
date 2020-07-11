from datetime import timezone

from pgevents.utils import timestamps


def test_epoch():
    assert timestamps.EPOCH.year == 1970
    assert timestamps.EPOCH.month == 1
    assert timestamps.EPOCH.day == 1
    assert timestamps.EPOCH.hour == 0
    assert timestamps.EPOCH.minute == 0
    assert timestamps.EPOCH.second == 0
    assert timestamps.EPOCH.tzinfo == timezone.utc


def test_now():
    assert timestamps.now().tzinfo == timezone.utc
