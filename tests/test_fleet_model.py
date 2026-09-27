from common.fleet_model import ZONES, vehicle_roster


def test_roster_size_matches_request():
    roster = vehicle_roster(10)
    assert len(roster) == 10


def test_vehicle_ids_are_unique():
    roster = vehicle_roster(25)
    ids = [v.vehicle_id for v in roster]
    assert len(ids) == len(set(ids))


def test_every_vehicle_gets_a_known_zone():
    roster = vehicle_roster(15)
    for v in roster:
        assert v.home_zone in ZONES
