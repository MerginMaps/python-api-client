from ..common import ClientError, ErrorCode

def test_client_error_is_blocked_sync():
    """Test the is_blocked_sync method of ClientError."""
    error = ClientError(detail="", server_code=None)
    assert error.is_blocking_sync() is False
    error.server_code = ErrorCode.ProjectsLimitHit.value
    assert error.is_blocking_sync() is False

    error.server_code = ErrorCode.AnotherUploadRunning.value
    assert error.is_blocking_sync() is True
    error.server_code = ErrorCode.ProjectVersionExists.value
    assert error.is_blocking_sync() is True

def test_client_error_is_rate_limit():
    """Test the is_rate_limit method of ClientError."""
    error = ClientError(detail="", http_error=None)
    assert error.is_rate_limit() is False
    error.http_error = 500
    assert error.is_rate_limit() is False
    error.http_error = 429
    assert error.is_rate_limit() is True

def test_client_error_is_retryable_sync():
    """Test the is_retryable_sync method of ClientError."""
    error = ClientError(detail="", server_code=None, http_error=None)
    assert error.is_retryable_sync() is False

    error.server_code = ErrorCode.ProjectsLimitHit.value
    assert error.is_retryable_sync() is False
    error.server_code = ErrorCode.AnotherUploadRunning.value
    assert error.is_retryable_sync() is True

    error.server_code = None
    error.http_error = 400
    error.detail = "Some other error"
    assert error.is_retryable_sync() is False
    error.detail = "Another process"
    assert error.is_retryable_sync() is True
    error.detail = "Version mismatch"
    assert error.is_retryable_sync() is True

    error.http_error = 500
    assert error.is_retryable_sync() is False
    error.http_error = 429
    assert error.is_retryable_sync() is True