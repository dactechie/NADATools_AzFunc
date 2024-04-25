import pandas as pd
def final_cols(matched_df, final_fields):
  df_final = pd.DataFrame(columns=final_fields)

  for column in final_fields:
      if column in matched_df.columns:
          df_final[column] = matched_df[column]  # Or use another default value
      else:
          df_final[column] =""
  return df_final
  