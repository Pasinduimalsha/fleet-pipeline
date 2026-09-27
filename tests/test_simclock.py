from datetime import timedelta

from common.simclock import SIM_EPOCH, sim_day, sim_hour, seconds_until_next_day


def test_day_zero_at_epoch():
    assert sim_day(SIM_EPOCH) == 0
    assert sim_hour(SIM_EPOCH) == 0


def test_day_increments_after_sim_day_seconds():
    from common.config import settings

    ts = SIM_EPOCH + timedelta(seconds=settings.sim_day_seconds + 5)
    assert sim_day(ts) == 1


def test_hour_wraps_within_a_day():
    from common.config import settings

    midday = SIM_EPOCH + timedelta(seconds=settings.sim_day_seconds / 2)
    assert sim_hour(midday) == 12


def test_seconds_until_next_day_counts_down():
    from common.config import settings

    just_after_boundary = SIM_EPOCH + timedelta(seconds=1)
    remaining = seconds_until_next_day(just_after_boundary)
    assert remaining == settings.sim_day_seconds - 1
