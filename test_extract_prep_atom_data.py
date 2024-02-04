from datetime import datetime
from utils.df_xtrct_prep import extract_prep_atom_data
from utils.environment import MyEnvironmentConfig

MyEnvironmentConfig().setup('prod')

def test_extract_prep_atom_data():
    # Arrange
    period_start_dt = 20230101 # datetime(2023, 1, 1)
    period_end_dt = 20230131 # datetime(2023, 1, 31)
    test_purpose = "NADA"

    # Mock data
    # mock_atom_df = pd.DataFrame({
        
    # })
    atom_df = extract_prep_atom_data(period_start_dt, period_end_dt
                                    , purpose='NADA')
    print (atom_df.head())
    return atom_df


if __name__ == "__main__":
  result = test_extract_prep_atom_data()

    