import time, logging, uuid
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
logger = logging.getLogger(__name__)
class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        rid = str(uuid.uuid4())[:8]
        t = time.time()
        logger.info("[%s] -> %s %s", rid, request.method, request.url.path)
        response = await call_next(request)
        logger.info("[%s] <- %d | %.1fms", rid, response.status_code, (time.time()-t)*1000)
        response.headers["X-Request-ID"] = rid
        return response
