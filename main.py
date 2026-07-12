import asyncio
import hashlib
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import jwt
import kubernetes.client as k8s
import redis.asyncio as redis
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from kubernetes import config
from pydantic import BaseModel

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="MCP Gateway")
security = HTTPBearer()

# Конфигурация
JWT_SECRET = os.getenv("JWT_SECRET", "your-jwt-secret-key")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
REDIS_HOST = os.getenv("REDIS_HOST", "192.168.0.13")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
KUBECONFIG_PATH = os.getenv("KUBECONFIG", "./kubeconfig.yml")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "300"))  # 5 минут
JOB_POLL_INTERVAL = float(os.getenv("JOB_POLL_INTERVAL", "2"))

# Инициализация клиентов
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
k8s_client: Optional[k8s.CoreV1Api] = None
k8s_batch: Optional[k8s.BatchV1Api] = None


def initialize_kubernetes_clients() -> None:
    """Lazy Kubernetes initialization keeps imports testable and side-effect free."""
    global k8s_client, k8s_batch

    if k8s_client is not None and k8s_batch is not None:
        return

    config.load_kube_config(config_file=KUBECONFIG_PATH)
    k8s_client = k8s.CoreV1Api()
    k8s_batch = k8s.BatchV1Api()


def get_k8s_client() -> k8s.CoreV1Api:
    initialize_kubernetes_clients()
    if k8s_client is None:
        raise RuntimeError("Kubernetes CoreV1Api client is not initialized")
    return k8s_client


def get_k8s_batch() -> k8s.BatchV1Api:
    initialize_kubernetes_clients()
    if k8s_batch is None:
        raise RuntimeError("Kubernetes BatchV1Api client is not initialized")
    return k8s_batch

# Модели данных
class UserTokenData(BaseModel):
    user_id: str
    namespace: str
    services: List[str]

class JRPCRequest(BaseModel):
    jsonrpc: str = "2.0"
    method: str
    params: Optional[Dict[str, Any]] = None
    id: Optional[str] = None

class JRPCResponse(BaseModel):
    jsonrpc: str = "2.0"
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None
    id: Optional[str] = None

# Конфигурация MCP сервисов
MCP_SERVICES = {
    "notion": {
        "image": "notion-mcp-server:latest",
        "env_secrets": {
            "NOTION_TOKEN": "notion-token"
        }
    },
    "github": {
        "image": "github-mcp-server:latest", 
        "env_secrets": {
            "GITHUB_TOKEN": "github-token"
        }
    },
    "slack": {
        "image": "slack-mcp-server:latest",
        "env_secrets": {
            "SLACK_TOKEN": "slack-token"
        }
    },
    # ... добавьте остальные сервисы
}

async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> UserTokenData:
    """Верификация JWT токена и извлечение данных пользователя"""
    try:
        token = credentials.credentials
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])

        user_id = payload.get("user_id")
        namespace = payload.get("namespace")
        if not user_id or not namespace:
            raise HTTPException(status_code=401, detail="Invalid token data")

        user_data = UserTokenData(
            user_id=user_id,
            namespace=namespace,
            services=payload.get("services", []),
        )

        return user_data

    except HTTPException:
        raise
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

async def verify_service_access(user_data: UserTokenData, service: str):
    """Проверка доступа пользователя к сервису"""
    if service not in user_data.services:
        raise HTTPException(
            status_code=403,
            detail=f"Access denied to service: {service}"
        )

def generate_request_hash(request_data: Dict) -> str:
    """Генерация хеша запроса для кеширования"""
    request_str = json.dumps(request_data, sort_keys=True)
    return hashlib.sha256(request_str.encode()).hexdigest()[:12]

def create_jrpc_error(code: int, message: str, id: Optional[str] = None) -> JRPCResponse:
    """Создание JRPC ошибки"""
    return JRPCResponse(
        error={"code": code, "message": message},
        id=id
    )

@app.post("/{service}/")
async def execute_mcp_service(
    service: str,
    request: JRPCRequest,
    user_data: UserTokenData = Depends(verify_token)
) -> JRPCResponse:
    """Основной эндпоинт для выполнения MCP сервисов через JRPC"""
    
    # Проверяем доступ к сервису
    await verify_service_access(user_data, service)
    
    # Проверяем существование сервиса
    if service not in MCP_SERVICES:
        return create_jrpc_error(-32001, f"Service {service} not found", request.id)
    
    # Генерируем хеш запроса для кеширования
    request_hash = generate_request_hash(request.model_dump())
    job_id = f"mcp-{user_data.user_id}-{service}-{request_hash}"
    cache_key = f"result:{job_id}"
    
    # Проверяем кеш
    try:
        cached_result = await redis_client.get(cache_key)
        if cached_result:
            logger.info(f"Cache hit for {job_id}")
            result_data = json.loads(cached_result)
            return JRPCResponse(**result_data)
    except Exception as e:
        logger.warning(f"Redis cache error: {e}")
    
    # Создаем Kubernetes Job
    try:
        job_manifest = create_job_manifest(job_id, user_data, service, request)
        batch_client = get_k8s_batch()
        await asyncio.to_thread(
            batch_client.create_namespaced_job,
            namespace=user_data.namespace,
            body=job_manifest,
        )
        
        logger.info(f"Created job {job_id} in namespace {user_data.namespace}")
        
    except k8s.ApiException as e:
        logger.error(f"Failed to create job {job_id}: {e}")
        return create_jrpc_error(-32000, f"Failed to create job: {e}", request.id)
    
    # Ожидаем завершения Job
    try:
        result = await wait_for_job_completion(job_id, user_data.namespace, request.id)
        
        # Кешируем успешные результаты
        if result.result is not None:
            await redis_client.setex(
                cache_key,
                3600,  # TTL 1 час
                json.dumps(result.model_dump()),
            )
        
        return result
        
    except Exception as e:
        logger.error(f"Job execution failed {job_id}: {e}")
        return create_jrpc_error(-32000, f"Job execution failed: {e}", request.id)

async def wait_for_job_completion(job_id: str, namespace: str, request_id: Optional[str]) -> JRPCResponse:
    """Ожидание завершения Job и получение результата"""
    
    start_time = datetime.now()
    
    while (datetime.now() - start_time).total_seconds() < REQUEST_TIMEOUT:
        try:
            # Проверяем статус Job
            batch_client = get_k8s_batch()
            job = await asyncio.to_thread(
                batch_client.read_namespaced_job,
                name=job_id,
                namespace=namespace,
            )
            
            if job.status.succeeded:
                # Job завершена успешно
                logger.info(f"Job {job_id} succeeded")
                return await get_job_result(job_id, namespace, request_id)
                
            elif job.status.failed:
                # Job завершена с ошибкой
                logger.error(f"Job {job_id} failed")
                return create_jrpc_error(-32000, "Job execution failed", request_id)
                
        except k8s.ApiException as e:
            if e.status != 404:
                logger.warning(f"Error checking job {job_id}: {e}")
        
        await asyncio.sleep(JOB_POLL_INTERVAL)
    
    # Таймаут
    logger.warning(f"Job {job_id} timeout")
    return create_jrpc_error(-32000, "Job execution timeout", request_id)

async def get_job_result(job_id: str, namespace: str, request_id: Optional[str]) -> JRPCResponse:
    """Получение результата выполнения Job"""
    
    try:
        # Получаем Pod для Job
        core_client = get_k8s_client()
        pods = await asyncio.to_thread(
            core_client.list_namespaced_pod,
            namespace=namespace,
            label_selector=f"job-name={job_id}",
        )
        
        if not pods.items:
            return create_jrpc_error(-32000, "Pod not found", request_id)
        
        pod = pods.items[0]
        
        # Получаем логи Pod
        logs = await asyncio.to_thread(
            core_client.read_namespaced_pod_log,
            name=pod.metadata.name,
            namespace=namespace,
        )
        
        # Парсим JRPC ответ (ищем последнюю валидную JSON строку)
        lines = [line.strip() for line in logs.split("\n") if line.strip()]
        
        for line in reversed(lines):
            try:
                jrpc_data = json.loads(line)
                # Проверяем, что это валидный JRPC response
                if isinstance(jrpc_data, dict) and jrpc_data.get("jsonrpc") == "2.0":
                    return JRPCResponse(
                        result=jrpc_data.get("result"),
                        error=jrpc_data.get("error"),
                        id=request_id  # Сохраняем оригинальный ID запроса
                    )
            except json.JSONDecodeError:
                continue
        
        # Если не нашли валидный JRPC, возвращаем последнюю строку как результат
        if lines:
            return JRPCResponse(result=lines[-1], id=request_id)

        return create_jrpc_error(-32000, "Empty response from job", request_id)
        
    except k8s.ApiException as e:
        logger.error(f"Failed to get result for job {job_id}: {e}")
        return create_jrpc_error(-32000, f"Failed to get result: {e}", request_id)

def create_job_manifest(job_id: str, user_data: UserTokenData, service: str, request: JRPCRequest) -> Dict:
    """Создание манифеста Kubernetes Job"""
    
    service_config = MCP_SERVICES[service]
    
    # Подготавливаем environment variables из secrets
    env_vars = []
    for env_name, secret_name in service_config["env_secrets"].items():
        env_vars.append({
            "name": env_name,
            "valueFrom": {
                "secretKeyRef": {
                    "name": secret_name,
                    "key": "token"
                }
            }
        })
    
    # Добавляем JRPC запрос как environment variables
    env_vars.extend([
        {
            "name": "JRPC_METHOD",
            "value": request.method
        },
        {
            "name": "JRPC_PARAMS", 
            "value": json.dumps(request.params) if request.params else "{}"
        },
        {
            "name": "JRPC_ID",
            "value": request.id or job_id
        },
        {
            "name": "JOB_ID",
            "value": job_id
        },
        {
            "name": "USER_ID",
            "value": user_data.user_id
        },
        {
            "name": "SERVICE_NAME",
            "value": service
        }
    ])
    
    job_manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_id,
            "namespace": user_data.namespace,
            "labels": {
                "app": "mcp-worker",
                "user": user_data.user_id,
                "service": service
            }
        },
        "spec": {
            "ttlSecondsAfterFinished": 300,  # Автоудаление через 5 минут
            "template": {
                "metadata": {
                    "labels": {
                        "app": "mcp-worker",
                        "user": user_data.user_id,
                        "service": service
                    }
                },
                "spec": {
                    "containers": [{
                        "name": "mcp-worker",
                        "image": service_config["image"],
                        "env": env_vars,
                        "resources": {
                            "requests": {
                                "memory": "128Mi",
                                "cpu": "100m"
                            },
                            "limits": {
                                "memory": "512Mi", 
                                "cpu": "500m"
                            }
                        }
                    }],
                    "restartPolicy": "Never"
                }
            }
        }
    }
    
    return job_manifest

@app.get("/services/")
async def list_services(user_data: UserTokenData = Depends(verify_token)):
    """Эндпоинт для получения списка доступных сервисов"""
    return {
        "jsonrpc": "2.0",
        "result": {
            "user_id": user_data.user_id,
            "namespace": user_data.namespace,
            "services": user_data.services
        },
        "id": "services_list"
    }



@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске"""
    try:
        await redis_client.ping()
        logger.info("✅ Connected to Redis")
    except Exception as e:
        logger.error(f"❌ Redis connection error: {e}")
    
    try:
        core_client = get_k8s_client()
        await asyncio.to_thread(core_client.get_api_resources)
        logger.info("✅ Connected to Kubernetes")
    except Exception as e:
        logger.error(f"❌ Kubernetes connection error: {e}")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)