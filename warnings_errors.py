from datetime import datetime
from dataclasses import dataclass
from enum import Enum, auto
from typing import Protocol, Optional


class NotificationType(Enum):
  WARNING = auto()
  ERROR = auto()

class MatchingDatasetType(Enum):
  ATOM = auto()
  EPISODE = auto()
  NOT_APPLICABLE = auto()

@dataclass(kw_only=True)
class ValidationMeta:
  client_id:str # SLK
  episode_id:Optional[str] =""
  episode_start:Optional[datetime]= datetime.now()
  episode_end:Optional[datetime]= datetime.now()
  assessment_date:Optional[datetime]= datetime.now()

@dataclass(kw_only=True)
class MatchingValidationMeta(ValidationMeta):
  found_only_in:Optional[MatchingDatasetType] = MatchingDatasetType.NOT_APPLICABLE


class ValidationNotifier(Protocol):
  def get_meta(self) -> ValidationMeta:
    ...

  def notify(self) -> str:
    ...

  # @property
  # def notification_type(self) -> ValidationMeta:
  #   ...

@dataclass(kw_only=True)
class MatchingWarning:
  meta: MatchingValidationMeta
  notification_type:NotificationType = NotificationType.WARNING

  def get_meta(self) -> ValidationMeta:
    return self.meta

  def notify(self) -> str:
    return f"{self.notification_type.name}: "
  

@dataclass(kw_only=True)
class MatchingError:
  meta: MatchingValidationMeta
  notification_type:NotificationType = NotificationType.ERROR

  def get_meta(self) -> ValidationMeta:
    return self.meta
  
  def notify(self) -> str:
    return f"{self.notification_type.name}: "
  


import pandas as pd

# def build_errors_warnings(data:pd.DataFrame
#                  , dataset_type:MatchingDatasetType
#                  , Notifier:ValidationNotifier) -> list:
#   ew:list= []
#   for idx, item in data.iterrows():
#      m = MatchingValidationMeta(client_id=item.SLK
#                                 , assessment_date=item.AssessmentDate
#                                 , found_only_in=dataset_type)
#      ew.append(Notifier(meta=m))  
     
#   return ew

def build_errors(data:pd.DataFrame
                 , dataset_type:MatchingDatasetType
                 ) -> list:
  errors:list= []
  if dataset_type == MatchingDatasetType.ATOM:
    for idx, item in data.iterrows():
      m = MatchingValidationMeta(client_id=item.SLK
                                 #, # TODO add Row_Key
                                  , assessment_date=item.AssessmentDate
                                  , found_only_in=dataset_type)
      #  errors.append(Notifier(meta=m))  
      errors.append(MatchingError(meta=m))
  elif dataset_type == MatchingDatasetType.EPISODE:
    for idx, item in data.iterrows():
      m = MatchingValidationMeta(client_id=item.SLK
                                  , episode_start=item.CommencementDate
                                  , episode_end=item.EndDate
                                  # , # TODO : add episode ID
                                  , found_only_in=dataset_type)
      errors.append(MatchingError(meta=m)) 
  else:
    raise NotImplementedError
  return errors