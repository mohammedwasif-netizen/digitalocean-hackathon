from app.domain.models import ClaimState
from app import main
from app.cross_device import CrossDeviceCoordinator, FakePushAdapter

def create_session(client, key="same-key"):
    response = client.post("/v1/sessions", json={"idempotency_key": key})
    assert response.status_code == 201
    return response.json()

def test_health_and_public_not_found(client):
    assert client.get("/healthz").json() == {"status": "ok"}
    response = client.get("/v1/claims/missing")
    assert response.status_code == 404

def test_session_creation_is_idempotent(client):
    first = create_session(client)
    second = create_session(client)
    assert first["id"] == second["id"]
    assert first["credential"] == second["credential"]

def test_session_validation(client):
    response = client.post("/v1/sessions", json={"idempotency_key": ""})
    assert response.status_code == 422

def test_fixture_pipeline_and_public_read(client):
    session = create_session(client, "pipeline")
    completed = client.post(f'/v1/sessions/{session["id"]}/claims')
    assert completed.status_code == 200
    body = completed.json()
    assert body["state"] == "COMPLETE"
    assert len(body["evidence"]) == 3
    assert len(body["verdict"]["citation_ids"]) == 3
    assert {item["publisher"] for item in body["evidence"]} == {"US EPA", "US Department of Energy", "ICCT"}
    assert client.get(f'/v1/claims/{body["public_id"]}').json() == body

def test_websocket_emits_all_pipeline_states_and_replay_does_not_duplicate(client):
    session = create_session(client, "socket")
    with client.websocket_connect(f'/v1/sessions/{session["id"]}/stream') as ws:
        message = {"type":"start_fixture", "session_id":session["id"], "sequence":1, "payload":{}}
        ws.send_json(message)
        assert ws.receive_json()["type"] == "ack"
        states = [ws.receive_json()["payload"]["state"] for _ in range(7)]
        assert states == [state.value for state in list(ClaimState)[:7]]
        ws.send_json(message)
        assert ws.receive_json()["type"] == "error"

def test_extension_fixture_delivers_exactly_one_paired_notification(client):
    push = FakePushAdapter()
    main.cross_device = CrossDeviceCoordinator(secret="fixture-push-test", push=push)
    session = create_session(client, "socket-push")
    pairing = client.post("/v1/pairings", json={"session_id": session["id"]}).json()
    device = client.post("/v1/pairings/redeem", json={
        "code": pairing["code"], "device_label": "Demo iPhone",
    }).json()
    subscription = client.post("/v1/push-subscriptions", json={
        "device_id": device["device_id"],
        "device_token": device["device_token"],
        "endpoint": "https://push.example/fixture",
        "p256dh": "p" * 32,
        "auth": "a" * 16,
    })
    assert subscription.status_code == 201

    with client.websocket_connect(f'/v1/sessions/{session["id"]}/stream') as ws:
        ws.send_json({"type":"start_fixture", "session_id":session["id"], "sequence":1, "payload":{}})
        assert ws.receive_json()["type"] == "ack"
        for _ in range(7):
            ws.receive_json()

    assert len(push.deliveries) == 1
    payload = push.deliveries[0][1]
    assert payload["notification_id"] == f'claim:{payload["public_id"]}'
    assert payload["title"] == "Verity found context"

def test_live_websocket_records_finals_and_creates_target_claim_once(client):
    session = create_session(client, "live")
    url = f'/v1/sessions/{session["id"]}/stream?credential={session["credential"]}'
    with client.websocket_connect(url) as ws:
        ws.send_json({"type":"start_live","schema_version":"2","session_id":session["id"],"sequence":0,"payload":{"stream_id":"stream"}})
        assert {ws.receive_json()["type"], ws.receive_json()["type"]} == {"capture_state", "ack"}

        events = []
        for message_sequence, chunk_sequence in ((1, 0), (2, 1)):
            body = b"opus"
            ws.send_json({
                "type":"audio_chunk","schema_version":"2","session_id":session["id"],"sequence":message_sequence,
                "payload":{"stream_id":"stream","chunk_sequence":chunk_sequence,"captured_at_ms":chunk_sequence*1000,"duration_ms":1000,"mime_type":"audio/webm;codecs=opus","sample_rate":48000,"channels":1,"byte_length":len(body)},
            })
            ws.send_bytes(body)
            while True:
                event = ws.receive_json()
                events.append(event)
                if event["type"] == "audio_ack":
                    break

        while not any(event["type"] == "claim_state" for event in events):
            events.append(ws.receive_json())
        claim_event = next(event for event in events if event["type"] == "claim_state")
        claim = claim_event["payload"]["claim"]
        assert claim["exact_text"] == "Electric vehicles produce no carbon emissions."
        assert claim["speaker_label"] == "Speaker B"
        assert claim["state"] == "CHECKING"
        assert len(claim["public_id"].removeprefix("claim-")) == 32

        body = b"opus"
        ws.send_json({
            "type":"audio_chunk","schema_version":"2","session_id":session["id"],"sequence":3,
            "payload":{"stream_id":"stream","chunk_sequence":1,"captured_at_ms":1000,"duration_ms":1000,"mime_type":"audio/webm;codecs=opus","sample_rate":48000,"channels":1,"byte_length":len(body)},
        })
        ws.send_bytes(body)
        replay_events = []
        while True:
            event = ws.receive_json()
            replay_events.append(event)
            if event["type"] == "audio_ack":
                break
        assert not {"transcript_final", "claim_state"} & {event["type"] for event in replay_events}
        persisted = client.get(f'/v1/claims/{claim["public_id"]}').json()
        assert persisted["state"] in {"CHECKING", "EVIDENCE_READY", "SYNTHESIZING", "COMPLETE"}
        assert persisted["public_id"] == claim["public_id"]

def test_verdict_endpoint_rejects_claim_not_awaiting_client_synthesis(client):
    session = create_session(client, "verdict")
    claim = client.post(f'/v1/sessions/{session["id"]}/claims').json()
    verdict = {**claim["verdict"], "claim_public_id": claim["public_id"], "prompt_version": "phase3-v1"}
    response = client.post(f'/v1/claims/{claim["public_id"]}/verdict', json=verdict)
    assert response.status_code == 409
    persisted = client.get(f'/v1/claims/{claim["public_id"]}').json()
    assert persisted["verdict"]["citation_ids"] == claim["verdict"]["citation_ids"]
