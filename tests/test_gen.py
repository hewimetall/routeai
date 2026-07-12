from datetime import datetime, timezone

import jwt

import gen


def test_generate_mcp_token_contains_expected_claims(monkeypatch):
    monkeypatch.setattr(gen, "JWT_SECRET", "test-secret")

    token = gen.generate_mcp_token(
        user_id="user-1",
        namespace="ns",
        services=["github", "slack"],
        expires_hours=2,
    )

    payload = jwt.decode(token, "test-secret", algorithms=[gen.JWT_ALGORITHM])
    assert payload["user_id"] == "user-1"
    assert payload["namespace"] == "ns"
    assert payload["services"] == ["github", "slack"]
    assert payload["iss"] == "mcp-token-generator"
    assert payload["exp"] > int(datetime.now(timezone.utc).timestamp())
    assert payload["iat"] <= int(datetime.now(timezone.utc).timestamp())
