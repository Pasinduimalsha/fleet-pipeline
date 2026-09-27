import json

from sim.telemetry_producer import VehicleAgent, corrupt


def test_vehicle_agent_starts_idle():
    agent = VehicleAgent("V0001", "D0001", "downtown")
    event = agent.tick()
    assert event["status"] == "idle"
    assert event["fare"] == 0.0
    assert event["trip_completed"] is False


def test_vehicle_lifecycle_eventually_completes_a_trip():
    agent = VehicleAgent("V0001", "D0001", "downtown")
    # Force the state machine through several transitions regardless of the
    # random dwell timers by fast-forwarding state_until.
    saw_trip_completed = False
    for _ in range(6):
        agent.state_until = 0  # force the next tick() to transition state
        event = agent.tick()
        if event["trip_completed"]:
            saw_trip_completed = True
            assert event["fare"] > 0
            break
    assert saw_trip_completed


def test_corrupt_always_fails_schema_validation():
    event = {
        "trip_id": "t1", "driver_id": "d1", "vehicle_id": "V0001", "zone": "downtown",
        "lat": 6.93, "lon": 79.85, "speed": 10.0, "status": "idle", "fare": 0.0,
        "trip_completed": False, "timestamp": "2024-01-01T00:00:00+00:00",
    }
    for _ in range(20):  # corrupt() picks one of three modes at random
        broken = corrupt(dict(event))
        try:
            parsed = json.loads(broken)
        except json.JSONDecodeError:
            continue  # truncated payload: unparsable, definitely invalid
        # Parseable but must still violate the required schema: either the
        # vehicle_id key is gone, or speed is no longer numeric.
        assert "vehicle_id" not in parsed or not isinstance(parsed.get("speed"), (int, float))
