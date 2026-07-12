import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import jwt
import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

import main


class FakeRedis:
    def __init__(self, cached=None, get_error=None):
        self.cached = cached
        self.get_error = get_error
        self.get_keys = []
        self.set_calls = []

    async def get(self, key):
        self.get_keys.append(key)
        if self.get_error:
            raise self.get_error
        return self.cached

    async def setex(self, key, ttl, value):
        self.set_calls.append((key, ttl, value))

    async def ping(self):
        return True


class FakeCore:
    def __init__(self, pods=None, logs="", error=None):
        self.pods = pods or []
        self.logs = logs
        self.error = error

    def list_namespaced_pod(self, namespace, label_selector):
        if self.error:
            raise self.error
        self.last_list = (namespace, label_selector)
        return SimpleNamespace(items=self.pods)

    def read_namespaced_pod_log(self, name, namespace):
        self.last_log = (name, namespace)
        return self.logs

    def get_api_resources(self):
        return {"resources": []}


class FakeBatch:
    def __init__(self, jobs=None, create_error=None, read_error=None):
        self.jobs = list(jobs or [])
        self.create_error = create_error
        self.read_error = read_error
        self.created = []

    def create_namespaced_job(self, namespace, body):
        if self.create_error:
            raise self.create_error
        self.created.append((namespace, body))

    def read_namespaced_job(self, name, namespace):
        if self.read_error:
            raise self.read_error
        self.last_read = (name, namespace)
        return self.jobs.pop(0)


@pytest.fixture(autouse=True)
def isolated_runtime(monkeypatch):
    async def immediate_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(main.asyncio, "to_thread", immediate_to_thread)
    monkeypatch.setattr(main.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(main, "REQUEST_TIMEOUT", 300)
    monkeypatch.setattr(main, "JOB_POLL_INTERVAL", 0)
    monkeypatch.setattr(main, "k8s_client", object())
    monkeypatch.setattr(main, "k8s_batch", object())
    yield


def credentials_for(payload):
    token = jwt.encode(payload, main.JWT_SECRET, algorithm=main.JWT_ALGORITHM)
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


def user_data(services=None):
    return main.UserTokenData(
        user_id="user-1",
        namespace="user-ns",
        services=services or ["github"],
    )


@pytest.mark.asyncio
async def test_verify_token_returns_user_data():
    credentials = credentials_for(
        {"user_id": "user-1", "namespace": "ns", "services": ["github"]}
    )

    result = await main.verify_token(credentials)

    assert result == main.UserTokenData(
        user_id="user-1",
        namespace="ns",
        services=["github"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "detail"),
    [
        ({"namespace": "ns", "services": []}, "Invalid token data"),
        (
            {
                "user_id": "user-1",
                "namespace": "ns",
                "exp": datetime.now(timezone.utc) - timedelta(seconds=1),
            },
            "Token expired",
        ),
    ],
)
async def test_verify_token_rejects_bad_payloads(payload, detail):
    credentials = credentials_for(payload)

    with pytest.raises(HTTPException) as exc_info:
        await main.verify_token(credentials)

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == detail


@pytest.mark.asyncio
async def test_verify_token_rejects_invalid_jwt():
    credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials="bad")

    with pytest.raises(HTTPException) as exc_info:
        await main.verify_token(credentials)

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Invalid token"


@pytest.mark.asyncio
async def test_verify_service_access_allows_configured_service():
    assert await main.verify_service_access(user_data(), "github") is None


@pytest.mark.asyncio
async def test_verify_service_access_denies_missing_service():
    with pytest.raises(HTTPException) as exc_info:
        await main.verify_service_access(user_data(), "slack")

    assert exc_info.value.status_code == 403
    assert "slack" in exc_info.value.detail


def test_generate_request_hash_is_stable_and_order_independent():
    first = main.generate_request_hash({"b": 2, "a": 1})
    second = main.generate_request_hash({"a": 1, "b": 2})

    assert first == second
    assert len(first) == 12


def test_create_jrpc_error_builds_response():
    response = main.create_jrpc_error(-32000, "boom", "req-1")

    assert response.error == {"code": -32000, "message": "boom"}
    assert response.id == "req-1"


def test_initialize_kubernetes_clients_loads_config_once(monkeypatch):
    calls = []
    core = FakeCore()
    batch = FakeBatch()

    monkeypatch.setattr(main, "k8s_client", None)
    monkeypatch.setattr(main, "k8s_batch", None)
    monkeypatch.setattr(
        main.config,
        "load_kube_config",
        lambda config_file: calls.append(config_file),
    )
    monkeypatch.setattr(main.k8s, "CoreV1Api", lambda: core)
    monkeypatch.setattr(main.k8s, "BatchV1Api", lambda: batch)

    main.initialize_kubernetes_clients()
    main.initialize_kubernetes_clients()

    assert calls == [main.KUBECONFIG_PATH]
    assert main.get_k8s_client() is core
    assert main.get_k8s_batch() is batch


def test_get_k8s_client_raises_when_initialization_fails(monkeypatch):
    monkeypatch.setattr(main, "k8s_client", None)
    monkeypatch.setattr(main, "k8s_batch", None)
    monkeypatch.setattr(main, "initialize_kubernetes_clients", lambda: None)

    with pytest.raises(RuntimeError, match="CoreV1Api"):
        main.get_k8s_client()

    with pytest.raises(RuntimeError, match="BatchV1Api"):
        main.get_k8s_batch()


def test_create_job_manifest_includes_request_and_secret_env():
    request = main.JRPCRequest(method="tools/list", params={"limit": 1}, id="req-1")

    manifest = main.create_job_manifest("job-1", user_data(), "github", request)

    container = manifest["spec"]["template"]["spec"]["containers"][0]
    env_by_name = {item["name"]: item for item in container["env"]}
    assert manifest["metadata"]["namespace"] == "user-ns"
    assert container["image"] == "github-mcp-server:latest"
    assert env_by_name["GITHUB_TOKEN"]["valueFrom"]["secretKeyRef"] == {
        "name": "github-token",
        "key": "token",
    }
    assert env_by_name["JRPC_METHOD"]["value"] == "tools/list"
    assert env_by_name["JRPC_PARAMS"]["value"] == '{"limit": 1}'
    assert env_by_name["JRPC_ID"]["value"] == "req-1"


@pytest.mark.asyncio
async def test_execute_mcp_service_returns_cached_result(monkeypatch):
    cached = json.dumps({"jsonrpc": "2.0", "result": {"cached": True}, "id": "req-1"})
    monkeypatch.setattr(main, "redis_client", FakeRedis(cached=cached))
    monkeypatch.setattr(main, "get_k8s_batch", pytest.fail)

    response = await main.execute_mcp_service(
        "github",
        main.JRPCRequest(method="tools/list", id="req-1"),
        user_data(),
    )

    assert response.result == {"cached": True}
    assert response.id == "req-1"


@pytest.mark.asyncio
async def test_execute_mcp_service_reports_unknown_service():
    response = await main.execute_mcp_service(
        "missing",
        main.JRPCRequest(method="tools/list", id="req-1"),
        user_data(["missing"]),
    )

    assert response.error == {
        "code": -32001,
        "message": "Service missing not found",
    }


@pytest.mark.asyncio
async def test_execute_mcp_service_reports_job_creation_error(monkeypatch):
    redis_client = FakeRedis()
    batch = FakeBatch(create_error=main.k8s.ApiException(status=500, reason="boom"))
    monkeypatch.setattr(main, "redis_client", redis_client)
    monkeypatch.setattr(main, "k8s_batch", batch)
    monkeypatch.setattr(main, "k8s_client", FakeCore())

    response = await main.execute_mcp_service(
        "github",
        main.JRPCRequest(method="tools/list", id="req-1"),
        user_data(),
    )

    assert response.error["code"] == -32000
    assert "Failed to create job" in response.error["message"]


@pytest.mark.asyncio
async def test_execute_mcp_service_waits_and_caches_success(monkeypatch):
    redis_client = FakeRedis()
    batch = FakeBatch()

    async def wait_for_job_completion(job_id, namespace, request_id):
        assert job_id.startswith("mcp-user-1-github-")
        assert namespace == "user-ns"
        return main.JRPCResponse(result={"ok": True}, id=request_id)

    monkeypatch.setattr(main, "redis_client", redis_client)
    monkeypatch.setattr(main, "k8s_batch", batch)
    monkeypatch.setattr(main, "k8s_client", FakeCore())
    monkeypatch.setattr(main, "wait_for_job_completion", wait_for_job_completion)

    response = await main.execute_mcp_service(
        "github",
        main.JRPCRequest(method="tools/list", id="req-1"),
        user_data(),
    )

    assert response.result == {"ok": True}
    assert batch.created[0][0] == "user-ns"
    key, ttl, value = redis_client.set_calls[0]
    assert key.startswith("result:mcp-user-1-github-")
    assert ttl == 3600
    assert json.loads(value)["result"] == {"ok": True}


@pytest.mark.asyncio
async def test_execute_mcp_service_reports_wait_error(monkeypatch):
    async def wait_for_job_completion(_job_id, _namespace, _request_id):
        raise RuntimeError("worker died")

    monkeypatch.setattr(main, "redis_client", FakeRedis())
    monkeypatch.setattr(main, "k8s_batch", FakeBatch())
    monkeypatch.setattr(main, "k8s_client", FakeCore())
    monkeypatch.setattr(main, "wait_for_job_completion", wait_for_job_completion)

    response = await main.execute_mcp_service(
        "github",
        main.JRPCRequest(method="tools/list", id="req-1"),
        user_data(),
    )

    assert response.error["code"] == -32000
    assert "worker died" in response.error["message"]


@pytest.mark.asyncio
async def test_execute_mcp_service_continues_after_redis_read_error(monkeypatch):
    async def wait_for_job_completion(_job_id, _namespace, request_id):
        return main.JRPCResponse(result="done", id=request_id)

    monkeypatch.setattr(main, "redis_client", FakeRedis(get_error=RuntimeError("down")))
    monkeypatch.setattr(main, "k8s_batch", FakeBatch())
    monkeypatch.setattr(main, "k8s_client", FakeCore())
    monkeypatch.setattr(main, "wait_for_job_completion", wait_for_job_completion)

    response = await main.execute_mcp_service(
        "github",
        main.JRPCRequest(method="tools/list", id="req-1"),
        user_data(),
    )

    assert response.result == "done"


@pytest.mark.asyncio
async def test_wait_for_job_completion_returns_result_on_success(monkeypatch):
    job = SimpleNamespace(status=SimpleNamespace(succeeded=1, failed=None))

    async def get_job_result(job_id, namespace, request_id):
        assert (job_id, namespace, request_id) == ("job-1", "ns", "req-1")
        return main.JRPCResponse(result="ok", id=request_id)

    monkeypatch.setattr(main, "k8s_batch", FakeBatch(jobs=[job]))
    monkeypatch.setattr(main, "get_job_result", get_job_result)

    response = await main.wait_for_job_completion("job-1", "ns", "req-1")

    assert response.result == "ok"


@pytest.mark.asyncio
async def test_wait_for_job_completion_reports_failure(monkeypatch):
    job = SimpleNamespace(status=SimpleNamespace(succeeded=None, failed=1))
    monkeypatch.setattr(main, "k8s_batch", FakeBatch(jobs=[job]))

    response = await main.wait_for_job_completion("job-1", "ns", "req-1")

    assert response.error == {"code": -32000, "message": "Job execution failed"}


@pytest.mark.asyncio
async def test_wait_for_job_completion_times_out(monkeypatch):
    monkeypatch.setattr(main, "REQUEST_TIMEOUT", 0)

    response = await main.wait_for_job_completion("job-1", "ns", "req-1")

    assert response.error == {"code": -32000, "message": "Job execution timeout"}


@pytest.mark.asyncio
async def test_wait_for_job_completion_ignores_404_then_times_out(monkeypatch):
    monkeypatch.setattr(main, "REQUEST_TIMEOUT", 0.01)
    monkeypatch.setattr(
        main,
        "k8s_batch",
        FakeBatch(read_error=main.k8s.ApiException(status=404, reason="missing")),
    )

    response = await main.wait_for_job_completion("job-1", "ns", "req-1")

    assert response.error["message"] == "Job execution timeout"


@pytest.mark.asyncio
async def test_get_job_result_returns_jsonrpc_response(monkeypatch):
    pod = SimpleNamespace(metadata=SimpleNamespace(name="pod-1"))
    logs = 'noise\n{"jsonrpc":"2.0","result":{"answer":42},"id":"worker"}'
    monkeypatch.setattr(main, "k8s_client", FakeCore(pods=[pod], logs=logs))

    response = await main.get_job_result("job-1", "ns", "req-1")

    assert response.result == {"answer": 42}
    assert response.id == "req-1"


@pytest.mark.asyncio
async def test_get_job_result_returns_last_plain_log(monkeypatch):
    pod = SimpleNamespace(metadata=SimpleNamespace(name="pod-1"))
    monkeypatch.setattr(main, "k8s_client", FakeCore(pods=[pod], logs="first\nlast"))

    response = await main.get_job_result("job-1", "ns", "req-1")

    assert response.result == "last"


@pytest.mark.asyncio
async def test_get_job_result_reports_missing_pod(monkeypatch):
    monkeypatch.setattr(main, "k8s_client", FakeCore())

    response = await main.get_job_result("job-1", "ns", "req-1")

    assert response.error == {"code": -32000, "message": "Pod not found"}


@pytest.mark.asyncio
async def test_get_job_result_reports_empty_logs(monkeypatch):
    pod = SimpleNamespace(metadata=SimpleNamespace(name="pod-1"))
    monkeypatch.setattr(main, "k8s_client", FakeCore(pods=[pod], logs=""))

    response = await main.get_job_result("job-1", "ns", "req-1")

    assert response.error == {"code": -32000, "message": "Empty response from job"}


@pytest.mark.asyncio
async def test_get_job_result_reports_kubernetes_error(monkeypatch):
    error = main.k8s.ApiException(status=500, reason="boom")
    monkeypatch.setattr(main, "k8s_client", FakeCore(error=error))

    response = await main.get_job_result("job-1", "ns", "req-1")

    assert response.error["code"] == -32000
    assert "Failed to get result" in response.error["message"]


@pytest.mark.asyncio
async def test_list_services_returns_user_services():
    response = await main.list_services(user_data(["github", "slack"]))

    assert response == {
        "jsonrpc": "2.0",
        "result": {
            "user_id": "user-1",
            "namespace": "user-ns",
            "services": ["github", "slack"],
        },
        "id": "services_list",
    }


@pytest.mark.asyncio
async def test_startup_event_checks_redis_and_kubernetes(monkeypatch):
    monkeypatch.setattr(main, "redis_client", FakeRedis())
    monkeypatch.setattr(main, "k8s_client", FakeCore())

    await main.startup_event()


@pytest.mark.asyncio
async def test_startup_event_logs_connection_errors(monkeypatch):
    class BrokenRedis:
        async def ping(self):
            raise RuntimeError("redis down")

    monkeypatch.setattr(main, "redis_client", BrokenRedis())
    monkeypatch.setattr(
        main,
        "get_k8s_client",
        lambda: (_ for _ in ()).throw(RuntimeError("kube down")),
    )

    await main.startup_event()
