class PipelineError(Exception):
    """
    Base exception for the Spark pipeline.
    """
    pass


class DataQualityError(PipelineError):
    """
    Raised when data quality checks fail.
    """
    pass


class InputDataError(PipelineError):
    """
    Raised when input data is missing or invalid.
    """
    pass


class PipelineWriteError(PipelineError):
    """
    Raised when output writing fails.
    """
    pass