from enum import Enum, auto


class DataType(Enum):
  ASSESSMENTS = auto()
  EPISODES = auto()
  PROCESSED_ASSESSMENTS = auto()
  PROCESSED_EPISODES = auto()
  # OTHER = auto()

class Purpose(Enum):
  NADA = auto()
  MATCHING = auto()


class ResultType(Enum):
  OK = auto()
  NOT_OK = auto()