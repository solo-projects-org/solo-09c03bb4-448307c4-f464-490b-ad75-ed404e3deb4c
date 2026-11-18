"""End-to-end pipeline behaviour: invalidation, publication, correction, recovery."""

from app import db, service
from app.journeys import Journey

from helpers import day, make_csv, pairs, state, tap, upload


def test_upload_publishes_and_counts(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
        tap("e3", "c2", "2024-05-01T09:00:00Z", "A", "in"),
        tap("e4", "c2", "2024-05-01T09:30:00Z", "C", "out"),
    ])
    detail = day(db_path, "2024-05-01")
    assert detail["status"] == "current"
    assert pairs(detail) == {("A", "B"): 1, ("A", "C"): 1}
    assert detail["trip_total"] == 2


def test_acceptance_marks_days_stale_before_recompute(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    assert day(db_path, "2024-05-01")["status"] == "current"

    # Accept *without* running the worker: the old result must already be stale.
    result = service.accept_upload(db_path, "correction.csv", make_csv([
        tap("e2", "c1", "2024-05-01T08:20:00Z", "C", "out", revision=1),
    ]))
    assert "2024-05-01" in result.affected_days
    stale = day(db_path, "2024-05-01")
    assert stale["status"] == "stale"
    assert stale["station_pairs"] == []          # nothing is served as current
    assert stale["last_error"]


def test_correction_changes_pairing_after_recompute(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    assert pairs(day(db_path, "2024-05-01")) == {("A", "B"): 1}

    upload(db_path, [tap("e2", "c1", "2024-05-01T08:20:00Z", "C", "out", revision=1)])
    detail = day(db_path, "2024-05-01")
    assert detail["status"] == "current"
    assert pairs(detail) == {("A", "C"): 1}


def test_lower_revision_is_ignored(db_path):
    upload(db_path, [tap("e1", "c1", "2024-05-01T08:20:00Z", "B", "out", revision=2)])
    result = upload(db_path, [tap("e1", "c1", "2024-05-01T08:20:00Z", "Z", "out", revision=1)])
    assert result.inserted == 0
    assert result.ignored == 1
    conn = db.connect(db_path)
    try:
        row = conn.execute("SELECT station, revision FROM events WHERE event_no='e1'").fetchone()
        assert (row["station"], row["revision"]) == ("B", 2)
    finally:
        conn.close()


def test_cross_midnight_correction_updates_both_days_together(db_path):
    # Day 1: entry 23:50 A; day 2: exit 00:20 B -> one A->B trip on 2024-05-01.
    upload(db_path, [
        tap("in1", "c1", "2024-05-01T23:50:00Z", "A", "in"),
        tap("out1", "c1", "2024-05-02T00:20:00Z", "B", "out"),
    ])
    assert pairs(day(db_path, "2024-05-01")) == {("A", "B"): 1}
    assert day(db_path, "2024-05-02")["status"] == "current"
    assert day(db_path, "2024-05-02")["trip_total"] == 0

    # Correct the exit station: only a day-2 event changes, day-1 totals move.
    accepted = service.accept_upload(db_path, "fix.csv", make_csv([
        tap("out1", "c1", "2024-05-02T00:20:00Z", "C", "out", revision=1),
    ]))
    assert set(accepted.affected_days) == {"2024-05-01", "2024-05-02"}
    # Both days are stale in the same instant, before recompute runs.
    assert state(db_path, "2024-05-01")["status"] == "stale"
    assert state(db_path, "2024-05-02")["status"] == "stale"

    service.run_pending_jobs(db_path)
    assert pairs(day(db_path, "2024-05-01")) == {("A", "C"): 1}
    # The two days were published by one transaction: same computed_at and batch.
    s1, s2 = state(db_path, "2024-05-01"), state(db_path, "2024-05-02")
    assert s1["status"] == s2["status"] == "current"
    assert s1["computed_at"] == s2["computed_at"]


def test_failed_recompute_keeps_day_stale(db_path, monkeypatch):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    original = service._publish

    def boom(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(service, "_publish", boom)
    accepted = service.accept_upload(db_path, "c.csv", make_csv([
        tap("e2", "c1", "2024-05-01T08:20:00Z", "C", "out", revision=1),
    ]))
    service.run_pending_jobs(db_path)

    detail = day(db_path, "2024-05-01")
    assert detail["status"] == "stale"
    assert "disk full" in detail["last_error"]
    # Job is failed (non-retryable error) and the repair path can re-queue it.
    conn = db.connect(db_path)
    try:
        job = conn.execute("SELECT status FROM recompute_jobs WHERE id = ?", (accepted.job_id,)).fetchone()
        assert job["status"] == "failed"
    finally:
        conn.close()

    monkeypatch.setattr(service, "_publish", original)
    service.request_recompute(db_path, ["2024-05-01"])
    service.run_pending_jobs(db_path)
    detail = day(db_path, "2024-05-01")
    assert detail["status"] == "current"
    assert pairs(detail) == {("A", "C"): 1}


def test_restart_recovery_requeues_and_never_publishes_old_results(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    # Simulate a crash mid-recompute after accepting new data: day is stale,
    # job left "running", and a newer revision sits in events.
    accepted = service.accept_upload(db_path, "c.csv", make_csv([
        tap("e2", "c1", "2024-05-01T08:20:00Z", "D", "out", revision=1),
    ]))
    conn = db.connect(db_path)
    try:
        conn.execute("UPDATE recompute_jobs SET status='running' WHERE id=?", (accepted.job_id,))
        # Also fake a corrupt "current" claim with an outdated epoch.
        conn.execute(
            "UPDATE day_state SET status='current', data_epoch=0 WHERE service_date='2024-05-01'"
        )
    finally:
        conn.close()

    service.recover(db_path)
    # recovery demoted the bogus current claim and re-queued the interrupted job
    assert state(db_path, "2024-05-01")["status"] == "stale"
    service.run_pending_jobs(db_path)
    detail = day(db_path, "2024-05-01")
    assert detail["status"] == "current"
    assert pairs(detail) == {("A", "D"): 1}


def test_recover_requeues_stale_day_without_job(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    conn = db.connect(db_path)
    try:
        # Lose the job and orphan the day -> recovery must repair it.
        conn.execute("DELETE FROM recompute_jobs")
        conn.execute("UPDATE day_state SET status='stale' WHERE service_date='2024-05-01'")
    finally:
        conn.close()
    service.recover(db_path)
    service.run_pending_jobs(db_path)
    assert day(db_path, "2024-05-01")["status"] == "current"


def test_slow_recompute_cannot_overwrite_newer_revision(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    first = service.accept_upload(db_path, "r2.csv", make_csv([
        tap("e2", "c1", "2024-05-01T08:20:00Z", "C", "out", revision=1),
    ]))

    # While the first job's snapshot is open, a competing upload lands.
    def hook(stage, conn, info):
        if stage == "after_compute" and info["job_id"] == first.job_id:
            service.accept_upload(db_path, "r3.csv", make_csv([
                tap("e2", "c1", "2024-05-02T08:20:00Z", "E", "out", revision=2),
            ]))

    service.run_pending_jobs(db_path, hook=hook)
    # The stale job did not publish; it was re-queued and the newest job wins.
    service.run_pending_jobs(db_path)
    detail = day(db_path, "2024-05-01")
    assert detail["status"] == "current"
    assert pairs(detail) == {("A", "E"): 1}
    assert detail["pending_total"] == 0


def test_targeted_recompute_of_one_side_publishes_both_days(db_path):
    upload(db_path, [
        tap("in1", "c1", "2024-05-01T23:50:00Z", "A", "in"),
        tap("out1", "c1", "2024-05-02T00:20:00Z", "B", "out"),
    ])
    # Invalidating only the entry day must not publish it alone: the pair is
    # linked across midnight, so both days turn current in one transaction.
    service.request_recompute(db_path, ["2024-05-01"])
    service.run_pending_jobs(db_path)
    d1, d2 = day(db_path, "2024-05-01"), day(db_path, "2024-05-02")
    assert d1["status"] == d2["status"] == "current"
    assert pairs(d1) == {("A", "B"): 1}
    assert d2["trip_total"] == 0
    assert state(db_path, "2024-05-01")["computed_at"] == state(db_path, "2024-05-02")["computed_at"]


def test_new_card_on_second_day_does_not_invalidate_first(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    first_computed = state(db_path, "2024-05-01")["computed_at"]

    accepted = service.accept_upload(db_path, "day2.csv", make_csv([
        tap("e3", "c2", "2024-05-02T09:00:00Z", "C", "in"),
        tap("e4", "c2", "2024-05-02T09:30:00Z", "D", "out"),
    ]))
    service.run_pending_jobs(db_path)

    assert accepted.affected_days == ["2024-05-02"]
    assert state(db_path, "2024-05-01")["computed_at"] == first_computed
    assert pairs(day(db_path, "2024-05-02")) == {("C", "D"): 1}


def test_pending_taps_listed_and_not_counted(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:10:00Z", "B", "in"),
    ])
    detail = day(db_path, "2024-05-01")
    assert detail["trip_total"] == 0
    assert detail["pending_total"] == 2
    assert {p["event_no"] for p in detail["pending"]} == {"e1", "e2"}


def test_two_day_upload_and_span_validation(db_path):
    upload(db_path, [
        tap("a1", "c1", "2024-05-01T23:00:00Z", "A", "in"),
        tap("a2", "c1", "2024-05-02T00:10:00Z", "B", "out"),
        tap("b1", "c2", "2024-05-02T10:00:00Z", "C", "in"),
        tap("b2", "c2", "2024-05-02T10:30:00Z", "D", "out"),
    ])
    assert pairs(day(db_path, "2024-05-01")) == {("A", "B"): 1}
    assert pairs(day(db_path, "2024-05-02")) == {("C", "D"): 1}


# ---------------------------------------------------------------------------
# cross-midnight chains: day1 -> day2 -> day3 must publish as one unit
# ---------------------------------------------------------------------------

def _seed_chain(db_path):
    """Two cards chaining across midnight: cA day1→day2, cB day2→day3.

    cB's event numbers sort before cA's, so cB's journey is derived first
    (events come back in event_no index order) — the adverse order under
    which a single-pass linkage would drop day 3 from a day-1 recompute.
    """
    upload(db_path, [
        tap("e1", "cB", "2024-05-02T23:50:00Z", "C", "in"),
        tap("e2", "cB", "2024-05-03T00:20:00Z", "D", "out"),
    ])
    upload(db_path, [
        tap("e3", "cA", "2024-05-01T23:50:00Z", "A", "in"),
        tap("e4", "cA", "2024-05-02T00:20:00Z", "B", "out"),
    ])
    # An unrelated date with no cross-day link: must keep serving throughout.
    upload(db_path, [
        tap("x1", "cX", "2024-05-10T08:00:00Z", "G", "in"),
        tap("x2", "cX", "2024-05-10T08:30:00Z", "H", "out"),
    ])


def _correct_day3_exit_during_first_compute(stage, conn, info, fired, db_path):
    if stage == "after_compute" and not fired:
        fired.append(info["job_id"])
        # While the old job computes, the day-3 exit receives a higher revision.
        service.accept_upload(db_path, "fix.csv", make_csv([
            tap("e2", "cB", "2024-05-03T00:20:00Z", "E", "out", revision=1),
        ]))


def test_linked_days_closure_is_transitive():
    journeys = [
        Journey("cB", "e1", "e2", "2024-05-02", "2024-05-03", "C", "D"),
        Journey("cA", "e3", "e4", "2024-05-01", "2024-05-02", "A", "B"),
    ]
    whole = ["2024-05-01", "2024-05-02", "2024-05-03"]
    # Reachable from either end and from the middle, regardless of order.
    assert service._linked_days(["2024-05-01"], journeys) == whole
    assert service._linked_days(["2024-05-03"], journeys) == whole
    assert service._linked_days(["2024-05-02"], journeys) == whole
    assert service._linked_days(["2024-05-01"], list(reversed(journeys))) == whole
    # A journey fully inside an unrelated day is not pulled in.
    journeys.append(Journey("cZ", "z1", "z2", "2024-05-10", "2024-05-10", "G", "H"))
    assert service._linked_days(["2024-05-01"], journeys) == whole
    assert service._linked_days(["2024-05-10"], journeys) == ["2024-05-10"]


def test_correction_during_targeted_recompute_publishes_whole_chain(db_path):
    _seed_chain(db_path)
    untouched = state(db_path, "2024-05-10")["computed_at"]

    # Auditor asks for a manual recompute of day 1 only.
    service.request_recompute(db_path, ["2024-05-01"])

    fired = []
    published = []

    def hook(stage, conn, info):
        _correct_day3_exit_during_first_compute(stage, conn, info, fired, db_path)
        if stage == "after_publish":
            published.append(set(info["days"]))

    service.run_pending_jobs(db_path, hook=hook)
    assert len(fired) == 1
    service.run_pending_jobs(db_path)

    # No transaction may publish only a part of the chain: every publish
    # touching day 1..3 must cover all three dates at once.
    chain = {"2024-05-01", "2024-05-02", "2024-05-03"}
    for days_published in published:
        if days_published & chain:
            assert chain <= days_published
    d1, d2, d3 = (day(db_path, d) for d in ("2024-05-01", "2024-05-02", "2024-05-03"))
    assert d1["status"] == d2["status"] == d3["status"] == "current"
    assert pairs(d1) == {("A", "B"): 1}
    assert pairs(d2) == {("C", "E"): 1}
    assert d3["trip_total"] == 0
    # The unrelated date was never invalidated or republished.
    assert state(db_path, "2024-05-10")["status"] == "current"
    assert state(db_path, "2024-05-10")["computed_at"] == untouched


def test_chain_not_half_published_when_followup_job_fails(db_path, monkeypatch):
    _seed_chain(db_path)
    service.request_recompute(db_path, ["2024-05-01"])

    fired = []
    original = service._publish
    publishes = {"n": 0}

    def flaky(*args, **kwargs):
        publishes["n"] += 1
        if publishes["n"] > 1:
            raise RuntimeError("disk full")  # the correction's own job fails
        return original(*args, **kwargs)

    monkeypatch.setattr(service, "_publish", flaky)
    service.run_pending_jobs(db_path, hook=lambda stage, conn, info: _correct_day3_exit_during_first_compute(
        stage, conn, info, fired, db_path))

    # The retried old job had to publish the entire chain from the snapshot
    # that includes the correction — it must not publish only a part and
    # leave day 3 behind while the follow-up job is failed.
    d1, d2, d3 = (day(db_path, d) for d in ("2024-05-01", "2024-05-02", "2024-05-03"))
    assert d1["status"] == d2["status"] == d3["status"] == "current"
    assert pairs(d1) == {("A", "B"): 1}
    assert pairs(d2) == {("C", "E"): 1}
    assert d3["trip_total"] == 0


def test_restart_recovery_keeps_chain_stale_then_publishes_together(db_path):
    _seed_chain(db_path)
    untouched = state(db_path, "2024-05-10")["computed_at"]

    job_id = service.request_recompute(db_path, ["2024-05-01"])
    # The manual job is claimed by a worker...
    conn = db.connect(db_path)
    try:
        conn.execute(
            "UPDATE recompute_jobs SET status='running', started_at=?, attempts=1 WHERE id=?",
            (service.utcnow(), job_id),
        )
    finally:
        conn.close()
    # ...when the day-3 correction lands (it cannot merge into a running
    # job, so it gets its own job), and the service crashes before publish.
    accepted = service.accept_upload(db_path, "fix.csv", make_csv([
        tap("e2", "cB", "2024-05-03T00:20:00Z", "E", "out", revision=1),
    ]))
    assert set(accepted.affected_days) == {"2024-05-02", "2024-05-03"}

    service.recover(db_path)

    # The stale state survives the restart: nothing linked is served as current.
    for d in ("2024-05-01", "2024-05-02", "2024-05-03"):
        assert state(db_path, d)["status"] == "stale"
    assert state(db_path, "2024-05-10")["status"] == "current"

    published = []

    def hook(stage, conn, info):
        if stage == "after_publish":
            published.append(set(info["days"]))

    service.run_pending_jobs(db_path, hook=hook)
    # Recovery published the chain as one unit, never piecemeal.
    chain = {"2024-05-01", "2024-05-02", "2024-05-03"}
    for days_published in published:
        if days_published & chain:
            assert chain <= days_published
    d1, d2, d3 = (day(db_path, d) for d in ("2024-05-01", "2024-05-02", "2024-05-03"))
    assert d1["status"] == d2["status"] == d3["status"] == "current"
    assert pairs(d1) == {("A", "B"): 1}
    assert pairs(d2) == {("C", "E"): 1}
    assert d3["trip_total"] == 0
    assert state(db_path, "2024-05-10")["computed_at"] == untouched
