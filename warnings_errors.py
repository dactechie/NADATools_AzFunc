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
  ONLY_IN_EPISODE = auto()
  ASMT_MATCHED_MULTI = auto()

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



def create_issues(df:pd.DataFrame, i_type:IssueType,\
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
  mask_isuetype_map = [ 
      {
        'mask': (gaps_df['days_from_start'] < 0) & \
                          (gaps_df['days_from_start'] >= -limit_days),
       'message': f"Assessment date is before episode start date by fewer than {limit_days}.",
       'issue_type':IssueLevel.WARNING
      },
      {
        'mask':  (gaps_df['days_from_end'] > 0) & \
                      (gaps_df['days_from_end'] <= limit_days),
        'message': f"Assessment date is after episode end date by fewer than {limit_days}.",
        'issue_type':IssueLevel.WARNING
      },
      {
        'mask': (gaps_df['days_from_start'] < -limit_days) ,
        'message': f"Assessment date is before episode start date by more than {limit_days}.",
        'issue_type':IssueLevel.ERROR
      },
      {
        'mask':  (gaps_df['days_from_end'] > limit_days) ,
        'message': f"Assessment date is after episode end date by more than {limit_days}.",
        'issue_type':IssueLevel.ERROR
      }
  ]

  results:list[MatchingIssue] = []
  for we in mask_isuetype_map:
    warns_errs = gaps_df[we['mask']]
    if len(warns_errs) > 0 :
      matching_issues = create_issues(warns_errs
                   , i_type=IssueType.DATE_MISMATCH
                   , i_level=we['issue_type'], msg=we['message'])
      results.extend(matching_issues)
  return results


def get_duplicate_issues(dfs:list[pd.DataFrame]):
  i_t:IssueType = IssueType.ASMT_MATCHED_MULTI
  i_l:IssueLevel = IssueLevel.ERROR
  message = f"Assessment is matched to more than one episode."
  results:list[MatchingIssue] = []
  for df in dfs:
      matching_issues = create_issues(df
                   , i_type=i_t
                   , i_level=i_l, msg=message)
      results.extend(matching_issues)

  return results
