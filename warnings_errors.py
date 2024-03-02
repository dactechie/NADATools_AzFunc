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
  FOUND_ONLY_IN_ATOM = auto()
  FOUND_ONLY_IN_EPISODE = auto()

class MatchingDatasetType(Enum):
  ATOM = auto()
  EPISODE = auto()
  NOT_APPLICABLE = auto()

@dataclass()#kw_only=True)
class ValidationMeta(ABC):
  client_id:str # SLK
 

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
  meta: ValidationMeta
  issue_level:IssueLevel #= NotificationType.WARNING
  issue_type:IssueType
  msg:Optional[str]=""

  def get_meta(self) -> ValidationMeta:
    return self.meta

  def notify(self) -> str:
    return f"{self.issue_level.name}: "
  


# from typing import Type
import pandas as pd


def add_to_issue_report(data:pd.DataFrame 
                , issue_type:IssueType
                , issue_level:IssueLevel
                , msg:Optional[str]=""      
                 ) -> list:
  
  if 'CommencementDate' in data.columns:
    metas = [EpisodeValidationMeta(client_id=item.SLK
                                    , program=item.Program
                                , episode_start=item.CommencementDate
                                , episode_end=item.EndDate)
              for idx, item in data.iterrows()]
  else:
    metas = [AssessmentValidationMeta(client_id=item.SLK
                                    , row_key=item.RowKey
                           , assessment_date=item.AssessmentDate)
             for idx, item in data.iterrows()]
  
  issues = [MatchingIssue(meta=m, issue_type=issue_type
                       ,issue_level=issue_level, msg=msg)
            for m in metas]

  return issues



def gap_ew(df:pd.DataFrame, i_type:IssueType,\
            i_level:IssueLevel, msg:str="")-> list[MatchingIssue]:
  metas = [AssessmentValidationMeta(client_id=item.SLK
                                  , row_key=item.RowKey
                                , assessment_date=item.AssessmentDate)
              for idx, item in df.iterrows()]

  issues = [MatchingIssue(meta=m, issue_type=i_type
                       ,issue_level=i_level, msg=msg)
            for m in metas]
  return issues

def gap_asesmtdate_epsd_boundaries(merged_df1:pd.DataFrame):
  merged_df = merged_df1.copy()
    # Calculating the difference in days
  merged_df.loc[:,'days_from_start'] = \
    (merged_df['AssessmentDate'] - merged_df['CommencementDate']).dt.days
  merged_df.loc[:,'days_from_end'] = \
    (merged_df['AssessmentDate'] - merged_df['EndDate']).dt.days
  return merged_df


def get_outofbounds_issues(unmatched_df:pd.DataFrame, limit_days:int = 1):
  gaps_df = gap_asesmtdate_epsd_boundaries(unmatched_df)
  # Warning if assessment is within 3 days outside the episode boundaries
  warning_mask = ((gaps_df['days_from_start'] < 0) & \
                      (gaps_df['days_from_start'] >= -limit_days)) | \
                  ((gaps_df['days_from_end'] > 0) & \
                      (gaps_df['days_from_end'] <= limit_days))
  # gaps_df.loc[warning_mask, 'status'] = 'Warning'
  # Error if assessment date is more than 3 days out of the episode boundaries
  error_mask = (gaps_df['days_from_start'] < -limit_days) | \
                  (gaps_df['days_from_end'] > limit_days)
  # gaps_df.loc[error_mask, 'status'] = 'Error'

  message = f"Assessment is out of Episode boundaries by more than {limit_days}."

  warnings = gaps_df[warning_mask]  
  i_warns = gap_ew(warnings
                   , i_type=IssueType.DATE_MISMATCH
                   , i_level=IssueLevel.WARNING, msg=message)

  errors = gaps_df[error_mask]  
  i_errs = gap_ew(errors
                  , i_type=IssueType.DATE_MISMATCH
                  , i_level=IssueLevel.ERROR, msg=message)
  i_warns.extend(i_errs)
  return i_warns 
