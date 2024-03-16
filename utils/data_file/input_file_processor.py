
import io
import re

import pandas as pd
from .InputFileErrors \
      import TooSmallFileError
from utils.dtypes import blank_to_today_str
# from .MDSValidator.AOD_MDS.constants import program_domain_map
# programs = program_domain_map.keys()
# FILENAME_MIN_LENGTH = 14

def get_csv_content(req_body):
   # Find the boundary string
    boundary_pattern = r'--+([^\r\n]+)'
    boundary_match = re.search(boundary_pattern, req_body)
    if boundary_match:
        boundary = '--' + boundary_match.group(1)
    else:
      return None
     # Split the request body by the boundary string
    parts = req_body.split(boundary)

    # Find the part that contains the CSV content
    for part in parts:
        if 'filename=' in part:
            csv_content = part.split('\r\n\r\n')[1]
            break
    return csv_content


def get_df_from_reqbody(request_body) -> pd.DataFrame:

  req_body_len = len(request_body)

  if req_body_len < 500:
    msg = 'Insufficient data. quitting...'
    raise TooSmallFileError( req_body_len, msg )

  data_str = request_body.decode('utf-8',errors='ignore')
  csv_content = get_csv_content(data_str)
  df = pd.read_csv(io.StringIO(csv_content),
                    dtype=str,
                    header=0                   
                   )
  df.dropna(subset=['START DATE'], inplace=True)
  df['END DATE'] = df['END DATE'].apply(lambda x: blank_to_today_str(x))
  
  if len(df) < 2:
    msg = 'Insufficient data. quitting...'
    raise TooSmallFileError( req_body_len, msg )
    
  return df

def get_data(req):
  import csv
  request_body = req.get_body()
  req_body_len = len(request_body)

  if req_body_len < 500:
    msg = 'Insufficient data. quitting...'
    raise TooSmallFileError( req_body_len, msg )

  data_str = request_body.decode('utf-8',errors='ignore')
  data = []
  reader = csv.reader(io.StringIO(data_str))

  for row in reader:
    data.append(row)

  if len(data) < 2:
    msg = 'Insufficient data. quitting...'
    raise TooSmallFileError( req_body_len, msg )
    
  return data


    

# def _get_program_startdate_period(modded_fname, filename):
#   program, mmyyyy, period = modded_fname[:-4].split('_')
#   try:
#     if program not in programs:
#       raise InputFilenameFormatError(filename,
#             f"Not a valid program name.. Valid program names are {programs}.")
    
#     if len(mmyyyy) != 6:
#       raise InputFilenameFormatError(filename,'Invalid start date part in the filename')
    
#     year = int(mmyyyy[2:])
#     month = int(mmyyyy[0:2])
#     current_year = datetime.now().year
#     if not 1 <= month <= 12 or not current_year-2 <=  year <= current_year:
#       raise InputFilenameFormatError(filename, 
#             f'Month: {month} /Year: {year} is not in acceptable range.')

#     period_start_date = datetime(year,month,1)
#     p = int(period)
#     if p not in [1, 3, 6, 12]:
#       raise InputFilenameFormatError(filename,
#             f'Period: {p} is not valid, should be one of 1,3,6,12.')
      
#   except ValueError:
#     curr_year = datetime.now().year
#     raise InputFilenameFormatError(filename, 
#           'Start date and/or Period is not a valid integer.'\
#            f'Valid examples of start date (MMYYYY): 02{curr_year-2} or 10{curr_year}')
  
#   return program, period_start_date, p


# def get_filename(filename):
#   if not filename:
#     raise InputFilenameFormatError(filename,"No file name")

#   filename = urllib.parse.unquote(urllib.parse.unquote(filename))
#   filename = filename.split('/')[-1]
  
#   if not filename:
#     raise InputFilenameFormatError(filename,"No file name")
#   elif len(filename) <= FILENAME_MIN_LENGTH:
#     raise InputFilenameFormatError(filename,"Too short filename")

#   _count = filename.count('_')
#   modded_fname = filename

#   if _count > 2: # 'AMDS_Indivuduals_TSS_072019_12.csv' ; modded = 'TSS_...csv'
#     before_progname = filename[0:len(filename)-FILENAME_MIN_LENGTH]
#     # 13 is just to get it past the the last 2 undersccores : _072019_12.csv'
#     modded_fname = filename[before_progname.rfind('_')+1:]

#   print(">>>>>>>>>>>>>>>>>", modded_fname)
#   if not modded_fname or modded_fname.count('_') != 2 \
#                       or modded_fname[-4:] != '.csv':
#     raise InputFilenameFormatError(filename,"Not a valid file name/type")

#   return modded_fname


# def get_details_from(filename):
#   modded_fname = get_filename(filename)

#   return _get_program_startdate_period(modded_fname, filename)
