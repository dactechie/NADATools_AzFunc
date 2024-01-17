
from datetime import datetime
import urllib.parse
from .InputFileErrors \
      import (InputFilenameFormatError, TooSmallFileError)
from .MDSValidator.AOD_MDS.constants import program_domain_map

programs = program_domain_map.keys()

FILENAME_MIN_LENGTH = 14

def get_data(req):
  request_body = req.get_body()
  req_body_len = len(request_body)

  if req_body_len < 500:
    msg = 'Insufficient data. quitting...'
    raise TooSmallFileError( req_body_len, msg )

  data = request_body.decode('utf-8',errors='ignore').splitlines()
  if len(data) < 2:
    msg = 'Insufficient data. quitting...'
    raise TooSmallFileError( req_body_len, msg )
    
  return data
    

def _get_program_startdate_period(modded_fname, filename):
  program, mmyyyy, period = modded_fname[:-4].split('_')
  try:
    if program not in programs:
      raise InputFilenameFormatError(filename,
            f"Not a valid program name.. Valid program names are {programs}.")
    
    if len(mmyyyy) != 6:
      raise InputFilenameFormatError(filename,'Invalid start date part in the filename')
    
    year = int(mmyyyy[2:])
    month = int(mmyyyy[0:2])
    current_year = datetime.now().year
    if not 1 <= month <= 12 or not current_year-2 <=  year <= current_year:
      raise InputFilenameFormatError(filename, 
            f'Month: {month} /Year: {year} is not in acceptable range.')

    period_start_date = datetime(year,month,1)
    p = int(period)
    if p not in [1, 3, 6, 12]:
      raise InputFilenameFormatError(filename,
            f'Period: {p} is not valid, should be one of 1,3,6,12.')
      
  except ValueError:
    curr_year = datetime.now().year
    raise InputFilenameFormatError(filename, 
          'Start date and/or Period is not a valid integer.'\
           f'Valid examples of start date (MMYYYY): 02{curr_year-2} or 10{curr_year}')
  
  return program, period_start_date, p


def get_filename(filename):
  if not filename:
    raise InputFilenameFormatError(filename,"No file name")

  filename = urllib.parse.unquote(urllib.parse.unquote(filename))
  filename = filename.split('/')[-1]
  
  if not filename:
    raise InputFilenameFormatError(filename,"No file name")
  elif len(filename) <= FILENAME_MIN_LENGTH:
    raise InputFilenameFormatError(filename,"Too short filename")

  _count = filename.count('_')
  modded_fname = filename

  if _count > 2: # 'AMDS_Indivuduals_TSS_072019_12.csv' ; modded = 'TSS_...csv'
    before_progname = filename[0:len(filename)-FILENAME_MIN_LENGTH]
    # 13 is just to get it past the the last 2 undersccores : _072019_12.csv'
    modded_fname = filename[before_progname.rfind('_')+1:]

  print(">>>>>>>>>>>>>>>>>", modded_fname)
  if not modded_fname or modded_fname.count('_') != 2 \
                      or modded_fname[-4:] != '.csv':
    raise InputFilenameFormatError(filename,"Not a valid file name/type")

  return modded_fname


def get_details_from(filename):
  modded_fname = get_filename(filename)

  return _get_program_startdate_period(modded_fname, filename)
