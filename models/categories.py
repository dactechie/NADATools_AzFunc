from enum import Enum, auto

class Purpose(Enum):
  NADA = auto()
  MATCHING = auto()


class ResultType(Enum):
  OK = auto()
  NOT_OK = auto()