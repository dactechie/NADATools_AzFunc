
class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class InputFileError(Error):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """
    def __init__(self, expression, message):
      self.expression = expression
      self.message = message
    
    #def __repr__(self):
    def get_msg(self):
      msg = f'{self.message} >: {self.expression}'
      return msg
    # def __init__(self, clazz, expression, message):
    #   self.clazz = clazz
    #   self.expression = expression
    #   self.message = message
    
    # #def __repr__(self):
    # def get_msg(self):
    #   msg = f'{self.clazz}: {self.message} >: {self.expression}'
    #   return msg

class NoDataError(InputFileError):
  def __init__(self, expression, message):
    super().__init__(expression, message)


class SchemaValidationError(InputFileError):
  def __init__(self, expression, message):
    super().__init__(expression, message)



class MissingHeadersError(InputFileError):
  def __init__(self, expression, message):
    super().__init__(expression, message)


class InputFilenameFormatError(InputFileError):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, expression, message):
        #self.expression = expression
        #self.message = message
        super().__init__(expression, message)
        #super().__init__(__name__, expression, message)


class TooSmallFileError(InputFileError):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, expression, message):      
        #self.expression = expression
        #self.message = message
        super().__init__(expression, message)


