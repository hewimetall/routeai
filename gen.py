import jwt
import os
from datetime import datetime, timedelta, timezone
from typing import List

# Секретный ключ (должен совпадать с JWT_SECRET в Gateway)
JWT_SECRET = os.getenv("JWT_SECRET", "your-jwt-secret-key")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

def generate_mcp_token(
    user_id: str,
    namespace: str, 
    services: List[str],
    expires_hours: int = 24
) -> str:
    """
    Генерация JWT токена для MCP Gateway
    
    Args:
        user_id: идентификатор пользователя (обязательно)
        namespace: Kubernetes namespace (обязательно) 
        services: список доступных MCP сервисов
        expires_hours: срок жизни токена в часах
    
    Returns:
        JWT токен строкой
    """
    
    # Полезная нагрузка - должна точно соответствовать тому, что ожидает verify_token
    payload = {
        # Обязательные поля для UserTokenData
        "user_id": user_id,
        "namespace": namespace,
        "services": services,
        
        # Стандартные JWT claims
        "exp": datetime.now(timezone.utc) + timedelta(hours=expires_hours),
        "iat": datetime.now(timezone.utc),
        "iss": "mcp-token-generator"
    }
    
    # Генерация токена
    token = jwt.encode(
        payload,
        JWT_SECRET,
        algorithm=JWT_ALGORITHM
    )
    
    return token

# Примеры использования для разных пользователей
if __name__ == "__main__":
    
    # Пример 1: Пользователь с доступом к популярным сервисам
    token1 = generate_mcp_token(
        user_id="user-001",
        namespace="user001",
        services=["notion", "github", "slack", "yandex-disk", "vk"]
    )
    
    print("=== Токен для пользователя 1 ===")
    print("User ID: user-001")
    print("Namespace: user-001-namespace") 
    print("Services: notion, github, slack, yandex-disk, vk")
    print(f"Token: {token1}")
    print()
    
   