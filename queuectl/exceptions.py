"""Custom exceptions for QueueCTL"""


class QueueCTLException(Exception):
    """Base exception for all QueueCTL errors"""
    pass


class JobNotFoundException(QueueCTLException):
    """Raised when a job is not found"""
    pass


class InvalidJobStateException(QueueCTLException):
    """Raised when attempting invalid state transition"""
    pass


class JobExecutionException(QueueCTLException):
    """Raised when job execution fails"""
    pass


class StorageException(QueueCTLException):
    """Raised when storage operation fails"""
    pass


class ConfigurationException(QueueCTLException):
    """Raised when configuration is invalid"""
    pass
