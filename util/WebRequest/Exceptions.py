


class WebGetException(Exception):
	pass

class RedirectedError(WebGetException):
	pass

class ContentTypeError(WebGetException):
	pass

class ArgumentError(WebGetException):
	pass

class FetchFailureError(WebGetException):
	pass

