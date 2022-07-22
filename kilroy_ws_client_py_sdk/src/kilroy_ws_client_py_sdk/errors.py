class ProtocolError(Exception):
    pass


INVALID_MESSAGE_ERROR = ProtocolError("Invalid message received.")
CONVERSATION_ERROR = ProtocolError("Received incompatible conversation id.")


class AppError(Exception):
    def __init__(self, code, reason):
        self.code = code
        self.reason = reason
        super().__init__(self.reason)

    def __str__(self):
        return f"{self.code}: {self.reason}"
