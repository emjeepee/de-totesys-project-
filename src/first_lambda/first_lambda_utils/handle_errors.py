


def handle_errors(e: Exception, logger, context):
    """
    This function:
        1) logs errors with full 
            traceback 
        2) raises a RuntimeError with 
            context.

    Arguments:
        e: an instance of the Exception
            class
        logger: an object of standard 
            library logging            
        context: an error message

    Returns: None            

    """

    logger.error("%s: %s", context, e, exc_info=True)
    raise RuntimeError(context) from e

