from datetime import datetime
from abc import ABC
from dataclasses import dataclass
from enum import Enum, auto
from typing import Protocol, Optional


class IssueLevel(Enum):
  WARNING = auto()
  ERROR = auto()

class IssueType(Enum):
  DATE_MISMATCH = auto()
  ONLY_IN_ASSESSMENT = auto()
  CLIENT_ONLYIN_ASMT = auto()
  ONLY_IN_EPISODE = auto()
  CLIENT_ONLYIN_EPISODE = auto()
  ASMT_MATCHED_MULTI = auto()

class MatchingDatasetType(Enum):
  ATOM = auto()
  EPISODE = auto()
  NOT_APPLICABLE = auto()

@dataclass()#kw_only=True)
class ValidationMeta(ABC):
  common_client_id:str # SLK  
 

@dataclass(kw_only=True)
class EpisodeValidationMeta(ValidationMeta):
  program:Optional[str] ="" 
  episode_id:Optional[str] =""
  episode_start:Optional[datetime]= datetime.now()
  episode_end:Optional[datetime]= datetime.now()

@dataclass(kw_only=True)
class AssessmentValidationMeta(ValidationMeta):
  row_key:Optional[str] =""
  assessment_date:Optional[datetime]= datetime.now()


class ValidationNotifier(Protocol):
  def get_meta(self) -> ValidationMeta:
    ...

  def notify(self) -> str:
    ...

  # @property
  # def notification_type(self) -> ValidationMeta:
  #   ...

@dataclass(kw_only=True)
class MatchingIssue:
  # metas: list[ValidationMeta]
  client_id:str
  id:str  # ATOM RowKey/ EpisodeID
  id_type:MatchingDatasetType

  program:str
  staff:str

  # meta: ValidationMeta
  issue_level:IssueLevel #= NotificationType.WARNING
  issue_type:IssueType
  msg:Optional[str]=""

  # def get_meta(self) -> ValidationMeta:
  #   return self.meta

  def notify(self) -> str:
    return f"{self.issue_level.name}: "