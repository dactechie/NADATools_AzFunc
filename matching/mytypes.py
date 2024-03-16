from enum import Enum, auto
from dataclasses import dataclass
from typing import Optional #, KW_ONLY
from collections import namedtuple


class DataKeys(Enum):
  client_id =  'SLK'
  episode_id = 'PMSEpisodeID'
  assessment_id = 'RowKey'
  assessment_date = 'AssessmentDate'
  episode_start_date = 'CommencementDate'
  episode_end_date = 'EndDate'

class DatasetType(Enum):
    ASSESSMENT = auto()
    EPISODE = auto()


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


@dataclass()
class ValidationIssue(Exception):  
  msg:str
  issue_type:IssueType
  issue_level:IssueLevel
  key:Optional[str] = None

  def make_copy(self):
    return ValidationIssue(self.msg, self.issue_type,self.issue_level, self.key)
  
  def to_dict(self):
      return {
          "msg": self.msg,
          "issue_type": self.issue_type.name,
          "issue_level": self.issue_level.name,
          "key": self.key
      }

@dataclass(kw_only=True)
class ValidationError(ValidationIssue):
  issue_level:IssueLevel= IssueLevel.ERROR


@dataclass(kw_only=True)
class ValidationWarning(ValidationIssue):
  issue_level:IssueLevel= IssueLevel.WARNING

ValidationIssueTuple = namedtuple('ValidationIssue', ['mask', 'validation_issue'])