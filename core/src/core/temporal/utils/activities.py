def should_retry_exception(exception: Exception) -> bool:
    retryable_exceptions = {
        "ConnectionError",
        "TimeoutError",
        "TemporaryFailure",
        "DatabaseConnectionError",
        "NetworkError",
        "ServiceUnavailableError",
        "RateLimitError",
    }

    non_retryable_exceptions = {
        "ConfigurationError",
        "AuthenticationError",
        "PermissionError",
        "ValidationError",
        "SchemaError",
        "SyntaxError",
        "ValueError",
    }

    exception_type = type(exception).__name__

    if exception_type in non_retryable_exceptions:
        return False

    if exception_type in retryable_exceptions:
        return True

    error_message = str(exception).lower()

    retryable_patterns = [
        "connection",
        "timeout",
        "temporary",
        "network",
        "unavailable",
        "rate limit",
        "too many requests",
        "service unavailable",
        "internal server error",
    ]

    non_retryable_patterns = [
        "not found",
        "unauthorized",
        "forbidden",
        "bad request",
        "invalid",
        "malformed",
    ]

    for pattern in non_retryable_patterns:
        if pattern in error_message:
            return False

    for pattern in retryable_patterns:
        if pattern in error_message:
            return True

    return False
