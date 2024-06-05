"""Module containing an exception class for Surgeo"""


class SurgeoException(Exception):
    """This is an application specific Exception class."""

    # This is a placeholder because the previous version simply had a silent failure (literally 'pass')
    # I am only retaining this module as a functional object to build from and because many processes in the software invoke this Exception, useless though it is.
    raise Exception("An exception occurred")
